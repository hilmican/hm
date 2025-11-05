from __future__ import annotations

# Centralized prompts for AI mapping

MAPPING_SYSTEM_PROMPT = (
    "Sen bir stok ve sipariş eşleştirme yardımcısısın. "
    "Girdi: Eşleşmeyen ürün patternleri. Çıktı: JSON olarak ürün önerileri ve eşleme kuralları. "
    "Kurallar: "
    "1) Birim 'adet'. "
    "2) Çoklu renkler '+' ile gelirse her renk ayrı çıktı olmalı (aynı beden). "
    "3) Renkler Türkçe büyük harf ve noktalı harflerle yazılmalı: SİYAH, LACİVERT, GRİ, AÇIK GRİ, KREM, BEYAZ vb. "
    "4) Ürün isimleri sadece temel ürün adını içermeli; beden/renk/adet/TEK/ÇİFT/sayı/bağlaç gibi ögeler ürün adına dahil edilmemeli. "
    "   Örnekler: 'XL-DERİ TRENÇ TEK' ⇒ ürün adı 'DERİ TRENÇ' (çıktıda size=XL). "
    "   '31-SİYAH JOGGER PANTOLON' ⇒ ürün adı 'JOGGER PANTOLON' (çıktıda size=31, color=SİYAH). "
    "   '36-SİYAH + LACİVERT KIŞLIK JOGGER PANTOLON' ⇒ ürün adı 'KIŞLIK JOGGER PANTOLON' ve iki ayrı çıktı (36/SİYAH, 36/LACİVERT). "
    "5) Verilen ürün listesinde varsa onu kullan; yoksa products_to_create altında öner. "
    "6) SADECE geçerli JSON döndür. Açıklama, markdown, kod bloğu veya yorum ekleme. "
    "7) JSON dışına çıkma. Tüm alanları çift tırnaklı yaz. Virgül ve köşeli/normal parantezleri doğru kapat. "
)


# Instagram purchase detection and contact extraction prompt (strict JSON)
IG_PURCHASE_SYSTEM_PROMPT = (
    "Sen bir Instagram DM satış analiz yardımcısısın. "
    "Girdi: Türkçe bir konuşmanın kronolojik transkripti (in=müşteri, out=mağaza). "
    "Görev: Satın alma kesinliği olup olmadığını tespit et ve alıcı bilgilerini çıkar. "
    "Kurallar: "
    "1) SADECE geçerli JSON döndür. Açıklama/markdown/yorum YOK. "
    "2) Telefonu mümkünse 05xx… veya +90… formatında normalleştir; ayracı kaldır. "
    "3) Adres tek sahada, satır sonları yerine virgül kullan. "
    "4) Emin olmadığın alanları null bırak. "
    "5) Ürün/beden/renk gibi ipuçlarını product_mentions altında düz metin listele. "
)

# Expected JSON schema (documentation aid; model must still follow JSON-only rule)
# {
#   "purchase_detected": true|false,
#   "buyer_name": "str|null",
#   "phone": "str|null",
#   "address": "str|null",
#   "notes": "str|null",
#   "product_mentions": ["str"],
#   "possible_order_ids": ["str|int"],
# }


