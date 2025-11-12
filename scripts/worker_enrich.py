#!/usr/bin/env python3
import time
import logging
import asyncio
import json
import datetime as dt
from typing import Any, Optional

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.enrichers import enrich_user, enrich_page
from app.services.ai_ig import process_run as ig_ai_process_run, analyze_conversation
from app.services.monitoring import record_heartbeat, increment_counter, ai_run_log
import os
import socket
from app.db import get_session
from app.models import IGAiDebugRun


log = logging.getLogger("worker.enrich")
logging.basicConfig(level=logging.INFO)


def main() -> None:
    log.info("worker_enrich starting pid=%s host=%s", os.getpid(), socket.gethostname())
    # Log Redis connectivity and queue depths at startup for diagnostics
    try:
        from app.services.queue import _get_redis

        r = _get_redis()
        pong = r.ping()
        llen_ai = int(r.llen("jobs:ig_ai_process_run"))
        llen_eu = int(r.llen("jobs:enrich_user"))
        llen_ep = int(r.llen("jobs:enrich_page"))
        log.info(
            "redis ok=%s url=%s qdepth ig_ai=%s enrich_user=%s enrich_page=%s",
            bool(pong), os.getenv("REDIS_URL"), llen_ai, llen_eu, llen_ep,
        )
    except Exception as e:
        log.warning("redis diag failed: %s", e)
    while True:
        # heartbeat when idle too
        try:
            record_heartbeat("enrich", os.getpid(), socket.gethostname())
        except Exception:
            pass
        log.debug("waiting for jobs: enrich_user|enrich_page|ig_ai_process_run")
        job = dequeue("enrich_user", timeout=1) or dequeue("enrich_page", timeout=1) or dequeue("ig_ai_process_run", timeout=1)
        if not job:
            time.sleep(0.25)
            continue
        jid = int(job["id"])  # type: ignore
        kind = job.get("kind")
        payload = job.get("payload") or {}
        try:
            log.info("dequeued jid=%s kind=%s key=%s payload=%s", jid, kind, job.get("key"), json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass
        try:
            if kind == "enrich_user":
                uid = str(payload.get("ig_user_id") or job.get("key"))
                log.info("enrich_user start uid=%s", uid)
                asyncio.run(enrich_user(uid))
                try:
                    increment_counter("enrich_user", 1)
                    increment_counter("enrich_success", 1)
                except Exception:
                    pass
                log.info("enrich_user done uid=%s", uid)
            elif kind == "enrich_page":
                gid = str(payload.get("igba_id") or job.get("key"))
                log.info("enrich_page start igba_id=%s", gid)
                asyncio.run(enrich_page(gid))
                try:
                    increment_counter("enrich_page", 1)
                    increment_counter("enrich_success", 1)
                except Exception:
                    pass
                log.info("enrich_page done igba_id=%s", gid)
            elif kind == "ig_ai_process_run":
                payload = payload or {}
                rid = int(payload.get("run_id") or 0) or int(job.get("key") or 0)
                df = payload.get("date_from")
                dtv = payload.get("date_to")
                age_raw = payload.get("min_age_minutes")
                age = int(age_raw) if age_raw is not None else 60
                limit_raw = payload.get("limit")
                lim = int(limit_raw) if limit_raw is not None else 200
                rep = bool(payload.get("reprocess") not in (False, 0, "0", "false", "False", None))
                convo_id = payload.get("conversation_id")
                debug_run_id = payload.get("debug_run_id")

                logs_meta: list[dict[str, Any]] = []

                def _append_log(level: str, message: str, extra: Optional[dict[str, Any]] = None) -> None:
                    entry = {
                        "ts": dt.datetime.utcnow().isoformat(),
                        "level": level,
                        "message": message,
                    }
                    if extra is not None:
                        entry["extra"] = extra
                    logs_meta.append(entry)

                try:
                    log.info(
                        "ig_ai start rid=%s convo=%s date_from=%s date_to=%s min_age=%s limit=%s reprocess=%s",
                        rid,
                        convo_id,
                        df,
                        dtv,
                        age,
                        lim,
                        rep,
                    )
                except Exception:
                    pass

                ai_run_log(
                    rid,
                    "info",
                    "worker_start",
                    {
                        "conversation_id": convo_id,
                        "date_from": df,
                        "date_to": dtv,
                        "min_age_minutes": age,
                        "limit": lim,
                        "reprocess": rep,
                        "debug_run_id": debug_run_id,
                    },
                )
                _append_log("info", "worker_start", {
                    "conversation_id": convo_id,
                    "run_id": rid,
                    "debug_run_id": debug_run_id,
                })

                dfp = None
                dtp = None
                try:
                    if df:
                        import datetime as _dt

                        dfp = _dt.date.fromisoformat(str(df))
                except Exception:
                    dfp = None
                try:
                    if dtv:
                        import datetime as _dt

                        dtp = _dt.date.fromisoformat(str(dtv))
                except Exception:
                    dtp = None

                if debug_run_id:
                    with get_session() as session:
                        run_row = session.get(IGAiDebugRun, int(debug_run_id))
                        if run_row:
                            run_row.status = "running"
                            run_row.job_id = jid
                            run_row.started_at = dt.datetime.utcnow()
                            run_row.ai_run_id = rid
                            session.add(run_row)
                            session.commit()

                try:
                    res = ig_ai_process_run(
                        run_id=rid,
                        date_from=dfp,
                        date_to=dtp,
                        min_age_minutes=age,
                        limit=lim,
                        reprocess=rep,
                        conversation_id=convo_id,
                        debug_run_id=int(debug_run_id) if debug_run_id else None,
                    )
                except Exception as process_err:
                    _append_log("error", "process_failed", {"error": str(process_err)})
                    if debug_run_id:
                        with get_session() as session:
                            run_row = session.get(IGAiDebugRun, int(debug_run_id))
                            if run_row:
                                run_row.status = "failed"
                                run_row.error_message = str(process_err)
                                run_row.completed_at = dt.datetime.utcnow()
                                run_row.logs_json = json.dumps(logs_meta, ensure_ascii=False)
                                session.add(run_row)
                                session.commit()
                    raise

                try:
                    log.info(
                        "ig_ai done rid=%s convo=%s considered=%s processed=%s linked=%s purchases=%s unlinked=%s errors=%s",
                        rid,
                        convo_id,
                        int(res.get("considered", 0)),
                        int(res.get("processed", 0)),
                        int(res.get("linked", 0)),
                        int(res.get("purchases", 0)),
                        int(res.get("purchases_unlinked", 0)),
                        len(res.get("errors", []) if isinstance(res.get("errors"), list) else []),
                    )
                except Exception:
                    pass

                ai_run_log(
                    rid,
                    "info",
                    "worker_done",
                    {
                        "conversation_id": convo_id,
                        "considered": int(res.get("considered", 0)),
                        "processed": int(res.get("processed", 0)),
                        "linked": int(res.get("linked", 0)),
                        "purchases": int(res.get("purchases", 0)),
                        "unlinked": int(res.get("purchases_unlinked", 0)),
                        "errors": len(res.get("errors", []) if isinstance(res.get("errors"), list) else []),
                    },
                )

                _append_log("info", "worker_done", {
                    "summary": {
                        "considered": res.get("considered"),
                        "processed": res.get("processed"),
                        "linked": res.get("linked"),
                        "purchases": res.get("purchases"),
                        "unlinked": res.get("purchases_unlinked"),
                        "errors": res.get("errors"),
                    }
                })

                try:
                    increment_counter("ig_ai_process_run", 1)
                except Exception:
                    pass

                if debug_run_id:
                    debug_entries = res.get("debug_entries") if isinstance(res, dict) else None
                    debug_entry = debug_entries[0] if debug_entries else None
                    if debug_entry:
                        _append_log(
                            "info",
                            "debug_entry",
                            {
                                "conversation_id": debug_entry.get("conversation_id"),
                                "status": debug_entry.get("status"),
                                "linked_order_id": debug_entry.get("linked_order_id"),
                                "has_meta": bool(debug_entry.get("meta")),
                            },
                        )
                    with get_session() as session:
                        run_row = session.get(IGAiDebugRun, int(debug_run_id))
                        if run_row:
                            meta = {}
                            if debug_entry and isinstance(debug_entry.get("meta"), dict):
                                meta = debug_entry.get("meta") or {}
                            run_row.ai_model = meta.get("ai_model") if meta else run_row.ai_model
                            run_row.system_prompt = meta.get("system_prompt") if meta else run_row.system_prompt
                            run_row.user_prompt = meta.get("user_prompt") if meta else run_row.user_prompt
                            run_row.raw_response = meta.get("raw_response") if meta else run_row.raw_response
                            if debug_entry and debug_entry.get("result") is not None:
                                run_row.extracted_json = json.dumps(debug_entry.get("result"), ensure_ascii=False)

                            status_state = "completed"
                            error_msg = None
                            if debug_entry and debug_entry.get("status") == "error":
                                status_state = "failed"
                                error_msg = debug_entry.get("error") or json.dumps(res.get("errors", []), ensure_ascii=False)
                            elif not debug_entry and res.get("errors"):
                                status_state = "failed"
                                error_msg = json.dumps(res.get("errors", []), ensure_ascii=False)

                            run_row.status = status_state
                            run_row.error_message = error_msg
                            run_row.completed_at = dt.datetime.utcnow()
                            run_row.logs_json = json.dumps(logs_meta, ensure_ascii=False)
                            session.add(run_row)
                            session.commit()
            else:
                delete_job(jid)
                continue
            log.info("enrich ok jid=%s kind=%s", jid, kind)
            delete_job(jid)
        except Exception as e:
            log.warning("enrich fail jid=%s kind=%s err=%s", jid, kind, e)
            increment_attempts(jid)
            time.sleep(1)


if __name__ == "__main__":
    main()

