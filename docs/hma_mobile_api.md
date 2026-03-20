# HMA Mobile Stock API (magaza-satis)

Set `HMA_MOBILE_API_KEY` in the HMA server environment. When set, mobile endpoints require header:

```http
X-Mobile-API-Key: <same value>
```

If `HMA_MOBILE_API_KEY` is empty/unset, auth is disabled (development only).

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/magaza-satis/api/order-from-kargo-qr` | Start/resume draft order from carrier label QR |
| POST | `/magaza-satis/api/order-add-item` | Add line + stock out (scan `hma:item:` QR) |
| POST | `/magaza-satis/api/order-complete` | Set total, payment, mark paid |
| POST | `/magaza-satis/api/series-print-and-stock` | Seri stok girişi + QR payload listesi |

Supporting public JSON (no mobile key required unless globally enforced elsewhere):

- `GET /products`
- `GET /products/{product_id}/supplier-prices` — cari bazlı `cost` / `price` önerileri (stok ekranıyla aynı)
- `GET /inventory/attributes?product_id=`

## order-from-kargo-qr

```json
{
  "qr_content": "<raw string from label>",
  "notes": "optional",
  "fields": {
    "tracking_no": "...",
    "name": "...",
    "phone": "...",
    "address": "...",
    "city": "..."
  }
}
```

`fields` overrides parsed values when QR is partial.

Response includes `order_item_count` (0 for new, or sum of quantities when `resumed: true`).

## order-add-item

```json
{
  "order_id": 123,
  "qr_content": "hma:item:456",
  "quantity": 1
}
```

## order-complete

```json
{
  "order_id": 123,
  "total_amount": 1499.90,
  "payment_method": "cash",
  "notes": "optional"
}
```

`payment_method`: `cash` | `bank_transfer`

## series-print-and-stock

```json
{
  "product_id": 10,
  "color": "Siyah",
  "quantity_per_variant": 1,
  "unit_cost": 250.0,
  "supplier_id": null,
  "price": null,
  "cost": null,
  "dry_run": false
}
```

`unit_cost` required (> 0). Sizes are inferred from existing variants on the product.

`supplier_id`: Stok hareketine yazılır (HMA web stok ekranındaki cari seçimi).

`price`: Doluysa her oluşturulan/güncellenen varyantın satış fiyatı (`Item.price`) buna çekilir.

`cost`: Doluysa varyantın referans maliyeti (`Item.cost`) — genelde `unit_cost` ile aynı gönderilebilir.

`dry_run`: `true` ise **veritabanına yazılmaz**: yeni `Item` oluşturulmaz, `StockMovement` eklenmez, fiyat/maliyet güncellenmez. Aynı doğrulamalar çalışır; cevapta `dry_run: true` ve `qr_payloads` içinde mevcut varyantlar için gerçek `hma:item:{id}`, henüz olmayan kombinasyonlar için `would_create_variant: true` ve `qr_data` önizleme (`hma:item:DRY_RUN:...`) döner.

`stock_units`: `HMA_STOCK_UNIT_TRACKING=1` ve gerçek girişte, oluşturulan her parça için `{ stock_unit_id, item_id, sku, size, color, qr_data }` (genelde `qr_data` = `hma:unit:{id}`). Etiket yazdırmak için bu liste kullanılır.

## Flutter Web: `ClientException` / `Failed to fetch`

Üretim API’sine (`https://hma.cdn.com.tr`) **localhost** veya **LAN IP** üzerinden (`flutter run -d chrome`) istek atınca tarayıcı bazen tek satır `Failed to fetch` gösterir; gerçek sebep (CORS, timeout, TLS) gizlenir.

1. **CORS** — HMA’da `app/main.py` içinde `CORSMiddleware` vardır. Varsayılan regex `localhost`, `127.0.0.1`, `[::1]` ve birçok özel ağ adresi (ör. `192.168.x.x`) için izin verir. CDN/nginx OPTIONS veya `Access-Control-*` başlıklarını kesiyorsa ingress/proxy yapılandırmasını güncelleyin. Özel origin için pod/env’de örneğin:
   - `HMA_CORS_ORIGINS=https://hma.cdn.com.tr,http://192.168.1.10:60588` (kendi origin’inizi yazın), veya
   - `HMA_CORS_ORIGIN_REGEX` ile ek eşleşme.
2. **Ön kontrol** — Aynı oturumda `GET /inventory/attributes` çalışıp `POST /magaza-satis/api/series-print-and-stock` patlıyorsa: büyük ihtimal **preflight/timeout** veya CDN’in **uzun POST** süresi; nginx `proxy_read_timeout` / WAF limitlerine bakın.
3. **Mobil uygulama** varsayılan olarak `https://hma.cdn.com.tr` kullanır; üretimde `HMA_MOBILE_API_KEY` ve deploy’daki CORS ayarlarının uyumlu olduğundan emin olun (`mobile/README.md`).

**Not:** `series-print-and-stock` sipariş (`order` satırı) oluşturmaz; yalnızca stok girişi / dry-run doğrulaması yapar. Taslak siparişler `kargo_qr` akışında `POST .../order-from-kargo-qr` ile oluşur.

## order-add-item ve QR

- `hma:item:{id}` veya `hma:sku:...`: stoktan `quantity` adet düşer (tracking açıksa FIFO parça seçimi).
- `hma:unit:{id}`: **yalnızca `quantity: 1`**; tam o parça satılır. Parça `in_stock` değilse 400.

Bkz. [stock_units.md](stock_units.md).
