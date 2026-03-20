# Parça bazlı stok (StockUnit)

Her fiziksel adet için `stock_unit` satırı; varyant satırı `item` aynı kalır. **Muhasebe ve FIFO** hâlâ `stockmovement` toplamları üzerinden çalışır.

## Ortam

| Değişken | Anlam |
|----------|--------|
| `HMA_STOCK_UNIT_TRACKING=1` | `adjust_stock` giriş/çıkışta `stock_unit` oluşturur veya FIFO ile satar. Kapalıyken davranış eskisi gibi (sadece hareket). |

## PVC / Kubernetes (hm-app)

Kod imajda değil volume’da ise:

```bash
POD=$(kubectl get pod -n hm -l app=hm-app -o jsonpath='{.items[0].metadata.name}')
kubectl wait --for=condition=Ready "pod/$POD" -n hm --timeout=120s
kubectl cp ./app/db.py hm/$POD:/app/app/db.py -n hm -c app
kubectl cp ./scripts/backfill_stock_units.py hm/$POD:/app/scripts/backfill_stock_units.py -n hm -c app
# … diğer ilgili app/*.py dosyaları
kubectl delete pod -n hm "$POD"
```

Uygulama logunda `[db.init] WARNING: stock_unit table CREATE failed` görürseniz sunucuda bir kez `CREATE TABLE stock_unit` çalıştırın veya FK hatasını düzeltin.

## İlk kurulum (mevcut veri)

1. Uygulamayı deploy edin — `db.py` startup’ta `stock_unit` tablosunu oluşturur (hata olursa stdout’a uyarı yazılır).
2. **Backfill** (bakım penceresi):

   ```bash
   cd /path/to/hm
   export DATABASE_URL='mysql+pymysql://...'
   python3 scripts/backfill_stock_units.py --dry-run
   python3 scripts/backfill_stock_units.py
   ```

3. Doğrulama:

   ```bash
   python3 scripts/backfill_stock_units.py --reconcile
   ```

   - Çıkış **0**: `on_hand >= 0` olan tüm kalemlerde hareket özeti ile `in_stock` parça sayısı aynı.
   - **Negatif on_hand** (geçmiş veri / geç güncellenen stok): raporda listelenir, **hata sayılmaz**; `stockmovement` veya manuel düzeltmelerle düzeltilene kadar backfill bu kaleme parça eklemez.
   - Çıkış **126**: en az bir kalemde `on_hand >= 0` iken parça sayısı farklı — `backfill` veya veri incelemesi gerekir.

   **Önemli:** Negatif stoku “düzeltmek” için sahte hareket üretmeyin; operasyonel girişlerle `on_hand` 0’a gelene kadar gerçek stok hareketleri kullanın. Parça tablosu hareketlerle tutarlı kalmalıdır.

4. `HMA_STOCK_UNIT_TRACKING=1` yapın ve pod’u yenileyin.

## QR

| Biçim | Anlam |
|-------|--------|
| `hma:item:{id}` | Varyant (`item.id`). Eski etiketler. |
| `hma:unit:{id}` | Tek parça (`stock_unit.id`). Seri girişten sonra API `stock_units` listesinde döner. |

Mobil `order-add-item`: `hma:unit` okutulduğunda `quantity` **1** olmalı; çıkış bu parçaya özel yapılır.

## API

- `POST /magaza-satis/api/series-print-and-stock` yanıtında `stock_units`: her yeni giriş hareketi için üretilen parçalar (`qr_data`: `hma:unit:...`). Tracking kapalıysa liste boş olabilir.

## Etiket yazdırma (mobil / ağ yazıcı)

1. **Uygulama içi (önerilen):** Seri stok ekranında QR listesi geldikten sonra **「PDF — yazdır / paylaş」** — her QR için ~50×35 mm sayfa; macOS/iOS’ta sistem yazdır penceresi açılır. Aynı LAN’da paylaşılan veya AirPrint / sürücüsü kurulu **ağ etiket yazıcısını** burada seçebilirsiniz. Kağıt boyutu yazıcıya göre ayarlayın (ör. 62 mm sürekli rulo yazıcıda “özel boyut” veya “fit to page”).

2. **「Metinleri kopyala」:** Satırlar `SKU · beden · qr_data` biçiminde panoya gider; Brother P-touch Editor, ZebraDesigner, Excel vb. ile toplu etiket üretiminde kullanılabilir.

3. **Ham veri:** Sadece `hma:unit:…` / `hma:item:…` string’i okutma için yeterli; QR görüntüsünü üretici yazılımı veya PDF ile basın.

4. **Doğrudan RAW (gelişmiş):** Zebra/Godex vb. yazıcılar bazen TCP **9100** üzerinde **ZPL** kabul eder; bu repo o protokolü göndermez — üretici aracı veya ayrı bir script gerekir.

## Sipariş düzenleme / import (2. faz)

Bazı akışlar `stockmovement` satırlarını siliyor veya yeniden yazıyor; tracking açıkken parça durumu ile çakışma riski vardır. Bu yüzden **2. faz**da orders/importer ile `stock_unit` geri alma eşlemesi ayrı tasarlanmalıdır. Üretimde önce mobil mağaza / `adjust_stock` yollarını kullanın; reconcile ile izleyin.
