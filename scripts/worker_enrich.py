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
            log.info("dequeued jid=%s kind=%s key=%s", jid, kind, job.get("key"))
        except Exception:
            pass
        try:
            if kind == "enrich_user":
                uid = str(payload.get("ig_user_id") or job.get("key"))
                asyncio.run(enrich_user(uid))
                try:
                    increment_counter("enrich_user", 1)
                    increment_counter("enrich_success", 1)
                except Exception:
                    pass
            elif kind == "enrich_page":
                gid = str(payload.get("igba_id") or job.get("key"))
                asyncio.run(enrich_page(gid))
                try:
                    increment_counter("enrich_page", 1)
                    increment_counter("enrich_success", 1)
                except Exception:
                    pass
            elif kind == "ig_ai_process_run":
                payload = payload or {}
                rid = int(payload.get("run_id") or 0) or int(job.get("key") or 0)
                df = payload.get("date_from")
                dtv = payload.get("date_to")
                age = int(payload.get("min_age_minutes") or 60)
                lim = int(payload.get("limit") or 200)
                rep = bool(payload.get("reprocess") not in (False, 0, "0", "false", "False", None))
                try:
                    log.info("ig_ai start rid=%s date_from=%s date_to=%s min_age=%s limit=%s reprocess=%s", rid, df, dtv, age, lim, rep)
                except Exception:
                    pass
                ai_run_log(
                    rid,
                    "info",
                    "worker_start",
                    {"date_from": df, "date_to": dtv, "min_age_minutes": age, "limit": lim, "reprocess": rep},
                )
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
                res = ig_ai_process_run(run_id=rid, date_from=dfp, date_to=dtp, min_age_minutes=age, limit=lim, reprocess=rep)
                try:
                    log.info(
                        "ig_ai done rid=%s considered=%s processed=%s linked=%s purchases=%s unlinked=%s errors=%s",
                        rid,
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
                        "considered": int(res.get("considered", 0)),
                        "processed": int(res.get("processed", 0)),
                        "linked": int(res.get("linked", 0)),
                        "purchases": int(res.get("purchases", 0)),
                        "unlinked": int(res.get("purchases_unlinked", 0)),
                        "errors": len(res.get("errors", []) if isinstance(res.get("errors"), list) else []),
                    },
                )
                try:
                    increment_counter("ig_ai_process_run", 1)
                except Exception:
                    pass
            elif kind == "ig_ai_debug_convo":
                debug_run_id = int(payload.get("debug_run_id") or job.get("key") or 0)
                logs: list[dict[str, Any]] = []

                def _append_log(level: str, message: str, extra: Optional[dict[str, Any]] = None) -> None:
                    entry = {
                        "ts": dt.datetime.utcnow().isoformat(),
                        "level": level,
                        "message": message,
                    }
                    if extra:
                        entry["extra"] = extra
                    logs.append(entry)

                if not debug_run_id:
                    _append_log("error", "missing_debug_run_id", {"job_id": jid})
                    delete_job(jid)
                    continue

                with get_session() as session:
                    run_row = session.get(IGAiDebugRun, debug_run_id)
                    if not run_row:
                        _append_log("error", "debug_run_not_found", {"debug_run_id": debug_run_id})
                        delete_job(jid)
                        continue
                    run_row.status = "running"
                    run_row.job_id = jid
                    run_row.started_at = dt.datetime.utcnow()
                    session.add(run_row)
                    session.commit()
                    conversation_id = run_row.conversation_id
                _append_log(
                    "info",
                    "debug_run_started",
                    {"debug_run_id": debug_run_id, "conversation_id": conversation_id},
                )

                try:
                    result = analyze_conversation(conversation_id, include_meta=True)
                    meta = result.get("meta") or {}
                    _append_log("info", "analysis_completed", {"debug_run_id": debug_run_id})
                    with get_session() as session:
                        run_row = session.get(IGAiDebugRun, debug_run_id)
                        if run_row:
                            run_row.status = "completed"
                            run_row.completed_at = dt.datetime.utcnow()
                            run_row.ai_model = meta.get("ai_model")
                            run_row.system_prompt = meta.get("system_prompt")
                            run_row.user_prompt = meta.get("user_prompt")
                            run_row.raw_response = meta.get("raw_response")
                            run_row.extracted_json = json.dumps(result, ensure_ascii=False)
                            run_row.logs_json = json.dumps(logs, ensure_ascii=False)
                            run_row.error_message = None
                            session.add(run_row)
                            session.commit()
                    try:
                        increment_counter("ig_ai_debug_convo", 1)
                    except Exception:
                        pass
                except Exception as dbg_err:
                    _append_log("error", "analysis_failed", {"error": str(dbg_err)})
                    with get_session() as session:
                        run_row = session.get(IGAiDebugRun, debug_run_id)
                        if run_row:
                            run_row.status = "failed"
                            run_row.error_message = str(dbg_err)
                            run_row.completed_at = dt.datetime.utcnow()
                            run_row.logs_json = json.dumps(logs, ensure_ascii=False)
                            session.add(run_row)
                            session.commit()
                    raise
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

