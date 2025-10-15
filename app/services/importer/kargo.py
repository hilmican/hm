from __future__ import annotations

from typing import Any

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
	return mapped


def read_kargo_file(file_path: str) -> list[dict[str, Any]]:
	headers, rows = read_sheet_rows(file_path)
	records: list[dict[str, Any]] = []
	for row in rows:
		raw = row_to_dict(headers, row)
		mapped = map_row(raw)
		records.append(mapped)
	return records
