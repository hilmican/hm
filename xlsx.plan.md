<!-- Kargo KOodenenler.xlsx Integration Spec -->

## Purpose
Define comprehensive header → field mappings, parsing and linkage rules to ingest KOodenenler.xlsx (kargo) files, auto-create/link Clients, Items, Orders and Payments, and capture all valid data.

## Header normalization
- All headers are normalized via `normalize_text`: lowercased, accents removed (İ→I, Ş→S, etc.), whitespace collapsed.
- Below lists use normalized forms.

## Field mappings (Turkish → internal)

### Tracking / identifiers
- tracking_no: "takip no", "kargo takip no", "gonderi no", "gonderi barkod no", "barkod no", "barkodno"
- external refs (kept in Order.notes for now): "tf", "tfserino", "tfno", "irsaliye", "irsaliyeno"

### Dates
- shipment_date: "tarih", "gonderi tarihi", "gonderi tarih"
- delivery_date: "teslim tarihi", "teslimat tarihi", "teslimtarihi"
- delivery_date (textual extractor): from any cell matching `(dd.mm.yyyy) tarihinde`

### Client
- name: "alici", "alıcı", "alici adi", "alici adı", "aliciadi", "musteri", "musteri adi", "musteri adı"
- address: "adres"
- city: "il", "sehir", "şehir"
- phone (rare in kargo; if present): "telefon", "cep", "gsm"
- external code (kept in notes): "musteri kodu", "musteri cari"

### Item / product
- item_name: "aciklama", "açıklama", "urun", "ürün", "urun adi", "urun adı", "urunadi"
- quantity: "adet", "miktar", "tane"

### Amounts
- total_amount: "tutar", "tutar tl", "fatura tutari", "fatura tutarı", "faturatutari", "toplam tutar"
- payment_amount: "odenen", "ödenen", "odenen tutar", "odenen tutari", "tahsil tutari", "tahsil tutarı"
- fees/deductions (captured in notes for now): "kesinti tutari", "erken odeme kesintisi", "komisyon", "kargo ucreti", "kdv"

### Payment method
- payment_method: "odeme tipi", "odeme turu", "odeme yontemi" (values: "Nakit", "Pos")

## Text-based augmentation (when headers are missing/partial)
- delivery_date: regex `(\d{1,2}[./]\d{1,2}[./]\d{4})\s+tarihinde` across string cells
- payment_method: if any cell equals `"nakit"` or `"pos"` (case-insensitive)
- payment_amount heuristic: last positive float before a cell containing `"tahsil"`; prefer value ≤ total_amount if available

## Derivations
- unit_price = round(total_amount / quantity, 2) when quantity > 0 and total present
- run.data_date (kargo) = max(shipment_date) in file; UI date is ignored for kargo

## Linking and idempotency
- Client upsert key: `normalize(name) + normalize(phone)` (phone often empty in kargo → name-based)
- Item upsert key: `sku = slugify(item_name)`
- Order match: by tracking_no; if missing, create new order
- Payment idempotency: `(order_id, amount, date)` unique triple; skip insert if already present

## Validation / parsing rules
- Dates accepted: `dd.mm.yyyy`, `yyyy-mm-dd`, `dd/mm/yyyy`, `mm/dd/yyyy`
- Numbers: handle Turkish thousands/decimal separators; convert to float
- Payments: require positive amount and non-null date (`delivery_date` or `shipment_date` or `run.data_date`)

## Entity writes (kargo commit)
1) Find order by `tracking_no`.
   - If found: fill missing `total_amount`, `shipment_date`, `item_id` (upsert Item by SKU if needed). Create Payment (idempotent) if `payment_amount` > 0.
   - If not found: upsert Client and Item, create Order with `source='kargo'`, `data_date=run.data_date`, then create Payment (idempotent) if present.
2) Record ImportRow with status `created|updated|unmatched|error` and mapped_json snapshot.

## Surfaces in UI / API
- Orders Table: shows Channel (source) and Data Date
- Payments Table: links to client and order, includes amount/date/method
- Dashboard upload: prompts for date only for `bizim`; skips prompt for `kargo`

## Future enhancements
- Persist external invoice/ref (e.g., TF numbers) as structured fields
- Model structured deductions/fees (net vs gross)



