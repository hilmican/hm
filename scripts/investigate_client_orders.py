#!/usr/bin/env python3
"""
Deep-dive a client's orders, payments, refunds, and Excel evidence.

Features
- Pull client/orders/payments/import rows from MySQL via app.db.
- Flag uncollected amounts, negative/refund orders, and duplicate tracking numbers.
- Show ImportRun/ImportRow provenance (filename, source, data_date, status).
- Search both local and (optionally) kubectl-copied Excel folders for matching rows.

Examples
    python scripts/investigate_client_orders.py --client-id 3375 --name "Mehmet Koka" --phone 5534910644
    python scripts/investigate_client_orders.py --client-id 3375 --tracking 5902 --fetch-from-pod ingest-worker-0
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional

from sqlmodel import select
from sqlalchemy import or_

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.db import get_session  # noqa: E402
from app.models import Client, ImportRow, ImportRun, Order, Payment  # noqa: E402
from app.services.importer.bizim import read_bizim_file  # noqa: E402
from app.services.importer.kargo import read_kargo_file  # noqa: E402
from app.utils.hashing import compute_row_hash  # noqa: E402
from app.utils.normalize import normalize_phone, normalize_text  # noqa: E402


LOCAL_EXCEL_FOLDERS = [
    (PROJECT_ROOT / "bizimexcellerimiz", "bizim"),
    (PROJECT_ROOT / "kargocununexcelleri", "kargo"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Investigate a client's orders/refunds against DB and Excel sources."
    )
    parser.add_argument("--client-id", type=int, help="Exact client.id (e.g., 3375).")
    parser.add_argument("--name", type=str, help="Client name search (ILIKE).")
    parser.add_argument("--phone", type=str, help="Client phone search (digits will be normalized).")
    parser.add_argument(
        "--tracking",
        action="append",
        default=[],
        help="Optional tracking_no hints to match (repeatable).",
    )
    parser.add_argument(
        "--excel-path",
        action="append",
        default=[],
        help="Additional Excel folder to scan (defaults include bizim/kargo folders).",
    )
    parser.add_argument(
        "--fetch-from-pod",
        type=str,
        help="If set, kubectl cp Excel dirs from this pod before scanning.",
    )
    parser.add_argument(
        "--k8s-namespace",
        default=os.getenv("K8S_NAMESPACE", "hm"),
        help="Namespace for kubectl cp (default: hm).",
    )
    parser.add_argument(
        "--remote-base",
        default="/app",
        help="Base path inside the pod that contains bizimexcellerimiz/kargocununexcelleri.",
    )
    parser.add_argument(
        "--excel-limit",
        type=int,
        default=30,
        help="Max Excel hits to print (per run).",
    )
    parser.add_argument(
        "--skip-excel",
        action="store_true",
        help="Skip Excel scanning (DB-only investigation).",
    )
    return parser.parse_args()


def find_client(session, client_id: Optional[int], name: Optional[str], phone: Optional[str]) -> Client | None:
    if client_id:
        return session.get(Client, client_id)

    if phone:
        phone_digits = normalize_phone(phone)
        if phone_digits:
            found = session.exec(
                select(Client).where(Client.phone.ilike(f"%{phone_digits}%"))
            ).first()
            if found:
                return found

    if name:
        found = session.exec(
            select(Client).where(Client.name.ilike(f"%{name}%"))
        ).first()
        if found:
            return found

    return None


def load_orders(session, client_id: int) -> tuple[list[Order], dict[int | None, list[Payment]]]:
    orders = session.exec(
        select(Order).where(Order.client_id == client_id).order_by(Order.id.asc())
    ).all()
    payments = session.exec(
        select(Payment).where(Payment.client_id == client_id)
    ).all()

    payments_by_order: dict[int | None, list[Payment]] = defaultdict(list)
    for p in payments:
        payments_by_order[p.order_id].append(p)
    return orders, payments_by_order


def load_import_rows(session, order_ids: list[int], client_id: int) -> tuple[dict[int, list[ImportRow]], dict[int, ImportRun]]:
    order_ids_set = set(order_ids)
    conditions = [ImportRow.matched_client_id == client_id]
    if order_ids_set:
        conditions.append(ImportRow.matched_order_id.in_(order_ids_set))
    rows = session.exec(select(ImportRow).where(or_(*conditions))).all()

    rows_by_order: dict[int, list[ImportRow]] = defaultdict(list)
    for row in rows:
        if row.matched_order_id:
            rows_by_order[row.matched_order_id].append(row)
        elif row.matched_client_id == client_id:
            rows_by_order[0].append(row)  # bucket for client-only matches

    run_ids = {r.import_run_id for r in rows}
    runs = {}
    if run_ids:
        for run in session.exec(select(ImportRun).where(ImportRun.id.in_(run_ids))).all():
            runs[run.id] = run
    return rows_by_order, runs


def format_money(val: float | None) -> str:
    if val is None:
        return "None"
    return f"{float(val):.2f}"


def summarize_orders(
    orders: list[Order],
    payments_by_order: dict[int | None, list[Payment]],
    rows_by_order: dict[int, list[ImportRow]],
    runs: dict[int, ImportRun],
) -> dict[str, list[Order]]:
    print("\n== Orders & Payments ==")
    problematic: dict[str, list[Order]] = defaultdict(list)
    tracking_seen: dict[str, list[int]] = defaultdict(list)

    for o in orders:
        pay_list = payments_by_order.get(o.id, [])
        paid = sum(float(p.amount or 0.0) for p in pay_list)
        total = float(o.total_amount or 0.0)
        outstanding = round(total - paid, 2)

        line = (
            f"- Order {o.id}: total={format_money(o.total_amount)}, "
            f"paid={format_money(paid)}, outstanding={format_money(outstanding)} "
            f"status={o.status} source={o.source} tracking={o.tracking_no}"
        )
        print(line)
        print(f"  dates: data={o.data_date}, shipment={o.shipment_date}, delivery={o.delivery_date}, return/switch={o.return_or_switch_date}")
        if pay_list:
            for p in pay_list:
                print(f"    Payment {p.id}: amount={format_money(p.amount)} date={p.date or p.payment_date} method={p.method}")
        else:
            print("    Payments: none")

        import_rows = rows_by_order.get(o.id, [])
        if import_rows:
            for ir in import_rows:
                run = runs.get(ir.import_run_id)
                run_info = f"run={run.id} {run.source} {run.filename} data_date={run.data_date}" if run else f"run={ir.import_run_id}"
                print(f"    ImportRow {ir.id}: status={ir.status} row_index={ir.row_index} {run_info} message={ir.message}")
        else:
            print("    ImportRow: none")

        if total < 0:
            problematic["refund_orders"].append(o)
        if outstanding != 0:
            problematic["unbalanced"].append(o)
        if o.tracking_no:
            tracking_seen[str(o.tracking_no)].append(o.id)

    dup_track = {k: v for k, v in tracking_seen.items() if len(v) > 1}
    if dup_track:
        print("\nDuplicate tracking numbers detected:")
        for trk, ids in dup_track.items():
            print(f"  tracking {trk}: orders {ids}")

    return problematic


def print_unmatched_payments(payments_by_order: dict[int | None, list[Payment]]) -> None:
    dangling = payments_by_order.get(None, [])
    if not dangling:
        return
    print("\n== Payments with no order_id ==")
    for p in dangling:
        print(f"  Payment {p.id}: amount={format_money(p.amount)} date={p.date} method={p.method}")


def kubectl_copy_excels(pod: str, namespace: str, remote_base: str) -> list[tuple[Path, str]]:
    temp_root = Path(tempfile.mkdtemp(prefix="excel-pull-"))
    folders: list[tuple[Path, str]] = []
    for dirname, source in (("bizimexcellerimiz", "bizim"), ("kargocununexcelleri", "kargo")):
        remote_dir = f"{remote_base.rstrip('/')}/{dirname}"
        dest_dir = temp_root / dirname
        dest_dir.mkdir(parents=True, exist_ok=True)
        cmd = ["kubectl", "-n", namespace, "cp", f"{pod}:{remote_dir}", str(dest_dir)]
        print(f"[kubectl] copying {remote_dir} -> {dest_dir} ...")
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"[kubectl] failed to copy {remote_dir}: {res.stderr.strip()}")
            continue
        folders.append((dest_dir, source))
    if not folders:
        shutil.rmtree(temp_root, ignore_errors=True)
    return folders


def iter_excel_sources(extra_paths: list[str], pod: str | None, namespace: str, remote_base: str) -> list[tuple[Path, str]]:
    sources: list[tuple[Path, str]] = list(LOCAL_EXCEL_FOLDERS)
    for p in extra_paths:
        sources.append((Path(p), "custom"))
    if pod:
        pulled = kubectl_copy_excels(pod, namespace, remote_base)
        sources.extend(pulled)
    return sources


def excel_matches(
    sources: list[tuple[Path, str]],
    target_name: str | None,
    target_phone: str | None,
    tracking_numbers: Iterable[str],
) -> list[dict]:
    t_name = normalize_text(target_name) if target_name else ""
    t_phone = normalize_phone(target_phone) if target_phone else ""
    t_tracking = {str(t).strip() for t in tracking_numbers if t}

    hits: list[dict] = []

    def match_row(rec: dict) -> bool:
        if t_name:
            rec_name = normalize_text(rec.get("name"))
            if t_name and t_name not in rec_name:
                return False
        if t_phone:
            rec_phone = normalize_phone(rec.get("phone"))
            if t_phone and t_phone not in rec_phone:
                return False
        if t_tracking:
            rec_tracking = str(rec.get("tracking_no") or "").strip()
            if rec_tracking and rec_tracking in t_tracking:
                return True
            # if tracking given but row has none, allow name/phone match to keep evidence
        return bool(t_name or t_phone or t_tracking)

    for folder, source in sources:
        if not folder.exists():
            print(f"[excel] skip missing folder {folder}")
            continue
        reader = read_bizim_file if source == "bizim" else read_kargo_file
        for path in sorted(folder.glob("*.xlsx")):
            try:
                records = reader(str(path))
            except Exception as e:
                print(f"[excel] failed to read {path}: {e}")
                continue
            for idx, rec in enumerate(records, start=1):
                if not match_row(rec):
                    continue
                hits.append(
                    {
                        "file": path,
                        "source": source,
                        "row_index": idx,
                        "data": rec,
                        "row_hash": compute_row_hash(rec),
                    }
                )
    return hits


def print_excel_hits(hits: list[dict], limit: int, runs: dict[int, ImportRun], rows_by_order: dict[int, list[ImportRow]]) -> None:
    if not hits:
        print("\n== Excel matches ==\nNo Excel rows matched the criteria.")
        return

    print("\n== Excel matches (cross-check with ImportRow by row_hash) ==")
    shown = 0
    for hit in hits:
        if shown >= limit:
            break
        rec = hit["data"]
        row_hash = hit["row_hash"]
        linked_rows = []
        for order_id, rows in rows_by_order.items():
            for ir in rows:
                if ir.row_hash and ir.row_hash == row_hash:
                    run = runs.get(ir.import_run_id)
                    run_info = f"run={run.id} {run.source} {run.filename}" if run else f"run={ir.import_run_id}"
                    linked_rows.append(f"ImportRow {ir.id} ({run_info}) matched_order={ir.matched_order_id}")
        print(
            f"- {hit['file'].name} [{hit['source']}] row #{hit['row_index']} "
            f"tracking={rec.get('tracking_no')} name={rec.get('name')} "
            f"payment_amount={rec.get('payment_amount')} total_amount={rec.get('total_amount')} notes={rec.get('notes')}"
        )
        print(f"    row_hash={row_hash[:16]}... matched_import_rows={linked_rows or 'none'}")
        shown += 1
    remaining = len(hits) - shown
    if remaining > 0:
        print(f"... {remaining} more hits not shown (increase --excel-limit to see all).")


def main() -> None:
    args = parse_args()
    if not (args.client_id or args.name or args.phone):
        print("Provide at least one of --client-id, --name, or --phone")
        sys.exit(1)

    with get_session() as session:
        client = find_client(session, args.client_id, args.name, args.phone)
        if not client:
            print("Client not found.")
            sys.exit(1)
        print(f"Client: {client.name} (id={client.id}, phone={client.phone}, city={client.city}, status={client.status})")

        orders, payments_by_order = load_orders(session, client.id)
        rows_by_order, runs = load_import_rows(session, [o.id for o in orders], client.id)

        problematic = summarize_orders(orders, payments_by_order, rows_by_order, runs)
        print_unmatched_payments(payments_by_order)

        if problematic:
            print("\n== Flags ==")
            for key, items in problematic.items():
                if not items:
                    continue
                ids = [o.id for o in items]
                print(f"  {key}: orders {ids}")

        excel_sources = iter_excel_sources(args.excel_path, args.fetch_from_pod, args.k8s_namespace, args.remote_base)
        if not args.skip_excel and excel_sources:
            hits = excel_matches(excel_sources, client.name, client.phone, args.tracking or [])
            print_excel_hits(hits, args.excel_limit, runs, rows_by_order)
        elif args.skip_excel:
            print("\n== Excel matches skipped (--skip-excel provided) ==")
        else:
            print("\n== Excel matches ==\nNo Excel folders available to scan.")


if __name__ == "__main__":
    main()

