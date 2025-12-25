# Finansal Özet (2025-10-01 – 2025-12-25)

Kaynak: prod DB (`appdb_h`), bugün çekilen veriler.

## Temel metrikler
- **Toplam satış:** 2.374.996 ₺ (1.631 sipariş)
- **Tahsilatlar (Payment.amount, order_id’li ödemeler + IBAN işaretlileri):** 1.889.729 ₺  
  - Payment.gross toplamı: 1.808.146 ₺  
  - IBAN işaretli satışlar: 115.183 ₺ (ödenmiş kabul ediliyor)
- **Alacak (tahsil edilmemiş):** 485.267 ₺
- **Banka bakiyesi:** 77.811 ₺
- **Stok değeri:** 179.720 ₺
- **Maliyetler:**  
  - Ürün maliyeti (order_costs): 1.219.425 ₺  
  - Operasyonel gider: 655.541 ₺  
  - Kargo maliyeti (shipping_fee): 140.807 ₺  
  - **Toplam maliyet:** 2.015.773 ₺
- **Kesinti (COD tahsilat/kargo):** Payment.net - Payment.amount farkı ≈ 173.304 ₺ ve tamamı **fee_kargo** (Sürat COD tahsilat komisyonu; platform kesintisi yok). Kârlılıkta satış–maliyet baz alınmalı; net_amount kullanılırsa kargo iki kez düşülmüş olur.
- **Net kâr (gross tahsilat varsayımı + maliyetler):** ≈ 185.919 ₺
- **Çalışma sermayesi (alacak + stok):** 664.987 ₺
- **Serbest nakit akışı (net kâr – çalışma sermayesi):** ≈ -479.068 ₺
- **Nakit – Stok:** -101.909 ₺

## Alacak yaşlandırma (tahsil edilmemiş COD ağırlıklı)
- 2025-10: 65.950 ₺
- 2025-11: 65.459 ₺
- 2025-12: 353.858 ₺
Toplam: 485.267 ₺ (en büyük risk Aralık gönderileri).

## Gözlemler
1) **orderpayment tablosu boş.** Tahsilat izleme fiilen `payment` tablosu ve `paid_by_bank_transfer` bayrağı ile yapılıyor.  
2) **fee_* kolonları kullanılmıyor.** Payment.net < Payment.amount farkı ~173k; nedeni (kargo/transfer/kur farkı) net değil. Şu an raporda “Toplam Kesintiler” 0 çünkü fee_* boş. Net’i kâr hesaplarında kullanmak muhtemelen kargo maliyetleriyle çifte düşüş olur; bu yüzden gross’u baz almak güvenli.  
3) **shipping_company çoğu boş (1.782 kayıt).** Doldurulursa alacak takibi kargo firması bazında yapılabilir; istenirse tamamı “surat” ile setlenebilir.  
4) **Kargo ücreti hesaplama:** Raporlarda, siparişin `shipping_fee` alanı varsa o kullanılıyor; yoksa `compute_shipping_fee` ile anlık tarifeden hesaplanıyor ve %20 KDV eklenerek KPI’da maliyete yazılıyor. Yani `shipping_fee` boş olan eski siparişler **güncel tarifeyle** hesaplanıyor; bu, tarife değiştiyse geçmiş maliyeti bozabilir.

## Önerilen aksiyonlar
**Acil / kritik**
1) **COD tahsilat mutabakatı (Focus/Sürat):** Özellikle Aralık alacakları (≈354k) liste çıkarılıp ödeme dosyalarıyla eşleştirilmeli; 30+ gün geçmiş Ekim-Kasım (≈132k) için iade/kayıp/tanzim süreçleri açılmalı.  
2) **Tahsilat izleme standardı:**  
   - Kârlılık ve alacakta **Payment.amount (gross)** + IBAN işaretlileri baz alınsın.  
   - `payment.net_amount` kâra sokulmasın; net zaten fee_kargo düşülmüş tutar, kargo maliyeti ayrıca `shipping_fee` ile giderde.  
   - COD tahsilat komisyonu (fee_kargo) istersek “kesintiler” KPI’sında gösterilebilir; kâr hesabı yine satış–maliyet olmalı.

**Kısa vadeli iyileştirme**
3) **Kargo firması alanı:** shipping_company boş olanları “surat” ile güncelleyebiliriz (isteğe bağlı).  
4) **Kargo maliyeti dondurma:**  
   - `order.shipping_fee` alanını tüm siparişler için doldurun (kargo Excel importunda setlenmeli).  
   - Raporlarda `compute_shipping_fee` fallback’ini azaltmak için backfill komutu çalıştırılabilir; böylece geçmiş siparişler gelecekteki tarife değişiminden etkilenmez.

**Orta vade**
5) **Alacak yaşlandırma raporu** eklenebilir (0-30 / 31-60 / 60+), kargo firması kırılımıyla.  
6) **orderpayment kullanımına geçiş** (kargo tahsilatlarını toplu bağlamak) ileride alacak takibini basitleştirir.

## Sık sorulanlar
- **Platform kesintisi var mı?** Yok; net-gross farkı tamamen Sürat COD tahsilat komisyonu (fee_kargo), platform komisyonu değil.  
- **Kargo maliyeti anlık mı hesaplanıyor?** `shipping_fee` alanı doluysa **sabit**. Boşsa, rapor sırasında `compute_shipping_fee` ile **güncel tarifeden** hesaplanıyor; bu geçmiş maliyeti oynatabilir. Bu yüzden `shipping_fee`’yi sipariş anında setleyip (veya backfill) sabitlemek önemli.


