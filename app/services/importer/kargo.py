from __future__ import annotations

from typing import Any
import re

from .common import read_sheet_rows, row_to_dict, parse_date, parse_float, parse_int


KARGO_MAPPING = {
	"takip no": "tracking_no",
	"kargo takip no": "tracking_no",
	"alıcı": "name",
	"alici": "name",
	"adres": "address",
	"il": "city",
	"şehir": "city",
	"açıklama": "item_name",
	"aciklama": "item_name",
	"urun": "item_name",
	"ürün": "item_name",
	"adet": "quantity",
	"tutar": "total_amount",
	"tarih": "shipment_date",
	"ödenen": "payment_amount",
	"odenen": "payment_amount",
}


def map_row(raw: dict[str, Any]) -> dict[str, Any]:
	mapped: dict[str, Any] = {}
	for k, v in raw.items():
		key = KARGO_MAPPING.get(k)
		if not key:
			continue
		mapped[key] = v
	# types
	if "shipment_date" in mapped:
		mapped["shipment_date"] = parse_date(mapped.get("shipment_date"))
	if "quantity" in mapped:
		mapped["quantity"] = parse_int(mapped.get("quantity")) or 1
	if "total_amount" in mapped:
		mapped["total_amount"] = parse_float(mapped.get("total_amount"))
	if "payment_amount" in mapped:
		mapped["payment_amount"] = parse_float(mapped.get("payment_amount"))

	# extract delivery_date from any textual field like: "7.10.2025 tarihinde ..."
	delivery_date = None
	for v in raw.values():
		if isinstance(v, str):
			m = re.search(r"(\d{1,2}[./]\d{1,2}[./]\d{4})\s+tarihinde", v)
			if m:
				delivery_date = parse_date(m.group(1))
				break
	if delivery_date:
		mapped["delivery_date"] = delivery_date

	# infer payment_method if a cell equals 'Nakit' or 'Pos'
	for v in raw.values():
		if isinstance(v, str):
			val = v.strip().lower()
			if val in ("nakit", "pos"):
				mapped["payment_method"] = "Nakit" if val == "nakit" else "Pos"
				break

	# collect notable notes
	note_bits: list[str] = []
	for v in raw.values():
		if isinstance(v, str) and ("Tahsil Edildi" in v or "ErkenOdemeKesintisi" in v):
			note_bits.append(v.strip())
	if note_bits:
		mapped["notes"] = " | ".join(dict.fromkeys(note_bits))

	# derive unit_price if possible
	qty = mapped.get("quantity") or 0
	tot = mapped.get("total_amount")
	if tot is not None and isinstance(qty, int) and qty > 0:
		mapped["unit_price"] = round(tot / qty, 2)
	return mapped


def read_kargo_file(file_path: str) -> list[dict[str, Any]]:
	headers, rows = read_sheet_rows(file_path)
	records: list[dict[str, Any]] = []
	for row in rows:
		raw = row_to_dict(headers, row)
		mapped = map_row(raw)
		records.append(mapped)
	return records
