from __future__ import annotations

from typing import Any, Dict, List

from .common import read_sheet_rows, row_to_dict, parse_date, parse_float, parse_int, normalize_header
from ...utils.normalize import client_unique_key


BIZIM_MAPPING = {
	"alıcı": "name",
	"alici": "name",
	"alıcının adı": "name",
	"telefon": "phone",
	"tel": "phone",
	"adres": "address",
	"il": "city",
	"şehir": "city",
	"urun": "item_name",
	"ürün": "item_name",
	"aciklama": "item_name",
	"açıklama": "item_name",
	"adet": "quantity",
	"birim fiyat": "unit_price",
	"tutar": "total_amount",
	"tarih": "shipment_date",
	"gönderim tarihi": "shipment_date",
	"takip no": "tracking_no",
	"kargo takip no": "tracking_no",
}


def map_row(raw: dict[str, Any]) -> dict[str, Any]:
	mapped: dict[str, Any] = {}
	for k, v in raw.items():
		key = BIZIM_MAPPING.get(k)
		if not key:
			continue
		mapped[key] = v
	# types
	if "shipment_date" in mapped:
		mapped["shipment_date"] = parse_date(mapped.get("shipment_date"))
	if "quantity" in mapped:
		mapped["quantity"] = parse_int(mapped.get("quantity")) or 1
	if "unit_price" in mapped:
		mapped["unit_price"] = parse_float(mapped.get("unit_price"))
	if "total_amount" in mapped:
		mapped["total_amount"] = parse_float(mapped.get("total_amount"))
	# unique key hint
	mapped["unique_key"] = client_unique_key(mapped.get("name"), mapped.get("phone"))
	return mapped


def read_bizim_file(file_path: str) -> list[dict[str, Any]]:
	headers, rows = read_sheet_rows(file_path)
	records: list[dict[str, Any]] = []
	for row in rows:
		raw = row_to_dict(headers, row)
		mapped = map_row(raw)
		records.append(mapped)
	return records
