from __future__ import annotations

from typing import Any
import re

from .common import read_sheet_rows, row_to_dict, parse_date, parse_float, parse_int


KARGO_MAPPING = {
	# tracking
	"takip no": "tracking_no",
	"kargo takip no": "tracking_no",
	"gonderi no": "tracking_no",
	"gonderi barkod no": "tracking_no",
	"barkod no": "tracking_no",
	"barkodno": "tracking_no",
	"websipariskodu": "tracking_no",

	# client name/address/city
	"alıcı": "name",
	"alici": "name",
	"alici adi": "name",
	"alici adı": "name",
	"aliciadi": "name",
	"musteri": "name",
	"musteri adi": "name",
	"musteri adı": "name",
	"aliciunvan": "name",
	"adres": "address",
	"il": "city",
	"şehir": "city",

	# textual notes (not items)
	"açıklama": "notes",
	"aciklama": "notes",
	"urun": "notes",
	"ürün": "notes",
	"urun adi": "notes",
	"urun adı": "notes",
	"urunadi": "notes",
	"faturabilgisi": "notes",

	# quantities and amounts
	"adet": "quantity",
	"tutar": "total_amount",
	"faturabedeli": "fee_kargo",
	"fatura tutari": "fee_kargo",
	"faturatutari": "fee_kargo",
	"tahsilattutari": "payment_amount",
	"ödenen": "payment_amount",
	"odenen": "payment_amount",
	"odenen tutar": "payment_amount",
	"odenen tutari": "payment_amount",

	# fees (various labels)
	"komisyon": "fee_komisyon",
	"komisyon tutari": "fee_komisyon",
	"hizmet": "fee_hizmet",
	"hizmet bedeli": "fee_hizmet",
	"kargo": "fee_kargo",
	"kargo ucreti": "fee_kargo",
	"iade": "fee_iade",
	"iade tutari": "fee_iade",
	"erken odeme": "fee_erken_odeme",
	"erkenodeme": "fee_erken_odeme",
	"erken odeme kesintisi": "fee_erken_odeme",
	"erken odeme kesinti": "fee_erken_odeme",

	# dates
	"tarih": "shipment_date",
	"gonderi tarihi": "shipment_date",
	"gonderi tarih": "shipment_date",
	"teftarih": "shipment_date",
	"teslim tarihi": "delivery_date",
	"teslimat tarihi": "delivery_date",
	"teslimtarihi": "delivery_date",

	# payment method
	"odeme tipi": "payment_method",
	"odemetipi": "payment_method",

	# identifiers
	"alicikodu": "alici_kodu",
}


def map_row(raw: dict[str, Any], row_values: list[Any] | None = None) -> dict[str, Any]:
	mapped: dict[str, Any] = {}
	for k, v in raw.items():
		key = KARGO_MAPPING.get(k)
		if not key:
			continue
		mapped[key] = v
	# strong guard: never emit item_name from kargo; convert to notes if present
	if mapped.get("item_name"):
		_txt = mapped.pop("item_name")
		if _txt:
			prev = mapped.get("notes")
			mapped["notes"] = f"{prev} | {_txt}" if prev else _txt
	# types
	if "shipment_date" in mapped:
		mapped["shipment_date"] = parse_date(mapped.get("shipment_date"))
	if "delivery_date" in mapped:
		mapped["delivery_date"] = parse_date(mapped.get("delivery_date"))
	if "quantity" in mapped:
		mapped["quantity"] = parse_int(mapped.get("quantity")) or 1
	if "total_amount" in mapped:
		mapped["total_amount"] = parse_float(mapped.get("total_amount"))
	if "payment_amount" in mapped:
		mapped["payment_amount"] = parse_float(mapped.get("payment_amount"))
	# fees to floats
	for fk in ("fee_komisyon","fee_hizmet","fee_kargo","fee_iade","fee_erken_odeme"):
		if fk in mapped:
			mapped[fk] = parse_float(mapped.get(fk)) or 0.0

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
	# also scan ordered row values if provided
	if "payment_method" not in mapped and row_values:
		for v in row_values:
			if isinstance(v, str):
				val = v.strip().lower()
				if val in ("nakit", "pos"):
					mapped["payment_method"] = "Nakit" if val == "nakit" else "Pos"
					break

	# collect notable notes
	note_bits: list[str] = []
	for v in raw.values():
		if isinstance(v, str):
			vs = v.strip()
			if ("Tahsil Edildi" in vs) or ("Tahsil" in vs) or ("ErkenOdemeKesintisi" in vs) or ("Erken Odeme" in vs) or ("Erken Ödeme" in vs):
				note_bits.append(vs)
	if note_bits:
		mapped["notes"] = " | ".join(dict.fromkeys(note_bits))

	# do not derive payment_amount from total_amount; rely solely on TahsilatTutari if present

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
        mapped = map_row(raw, row_values=row)
        records.append(mapped)
    return records
