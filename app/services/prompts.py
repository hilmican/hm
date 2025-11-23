from __future__ import annotations

# Centralized prompts for AI mapping
#
# NOTE (prompt versioning):
# - When you change any prompt here, copy the previous version into
#   app/services/prompts_archive/ with a dated filename, e.g.
#   2025-11-08-IG_PURCHASE_SYSTEM_PROMPT-v1.txt
# - This convention allows us to review historical prompt strategies.
#
# Hot-reload convention:
# - Prompts can be overridden at runtime via text files without restarting workers.
# - For IG purchase: env IG_PURCHASE_PROMPT_FILE or default app/services/prompts/IG_PURCHASE_SYSTEM_PROMPT.txt
# - The loader caches by mtime and re-reads when the file changes.
import os
from pathlib import Path
import time

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


# --- Hot-reloadable loader ----------------------------------------------------
_PROMPT_CACHE: dict[str, tuple[str, float]] = {}
_PROMPT_LAST_CHECK: dict[str, float] = {}


def _read_file_text(path: Path) -> str | None:
	try:
		return path.read_text(encoding="utf-8")
	except Exception:
		return None


def get_ig_purchase_prompt() -> str:
	"""
	Return the current IG purchase detection system prompt.
	- If a prompt file is present, use it with mtime-based cache.
	- Else, fall back to the in-code default IG_PURCHASE_SYSTEM_PROMPT.
	"""
	key = "ig_purchase"
	now = time.time()
	refresh_sec = float(os.getenv("PROMPT_REFRESH_SECONDS", "5"))
	# throttle stat calls
	if key in _PROMPT_CACHE and (now - _PROMPT_LAST_CHECK.get(key, 0.0) < max(1.0, refresh_sec)):
		return _PROMPT_CACHE[key][0]
	_PROMPT_LAST_CHECK[key] = now
	# resolve file path
	custom_path = os.getenv("IG_PURCHASE_PROMPT_FILE")
	if custom_path:
		p = Path(custom_path)
	else:
		p = Path("app/services/prompts/IG_PURCHASE_SYSTEM_PROMPT.txt")
	try:
		if p.exists():
			mt = p.stat().st_mtime
			cached = _PROMPT_CACHE.get(key)
			if (not cached) or (mt != cached[1]):
				txt = _read_file_text(p)
				if txt and txt.strip():
					_PROMPT_CACHE[key] = (txt, mt)
					return txt
			# unchanged -> return cached
			if cached:
				return cached[0]
	except Exception:
		# ignore file/permission errors; fall back to constant
		pass
	# fallback
	_PROMPT_CACHE[key] = (IG_PURCHASE_SYSTEM_PROMPT, _PROMPT_CACHE.get(key, (None, 0.0))[1] if _PROMPT_CACHE.get(key) else 0.0)  # type: ignore[arg-type]
	return IG_PURCHASE_SYSTEM_PROMPT


def get_global_system_prompt() -> str:
	"""
	Return the current global system prompt (default pretext) for Instagram conversation replies.
	- If a prompt file is present, use it with mtime-based cache.
	- Else, fall back to a minimal default.
	"""
	key = "global_system"
	now = time.time()
	refresh_sec = float(os.getenv("PROMPT_REFRESH_SECONDS", "5"))
	# throttle stat calls
	if key in _PROMPT_CACHE and (now - _PROMPT_LAST_CHECK.get(key, 0.0) < max(1.0, refresh_sec)):
		return _PROMPT_CACHE[key][0]
	_PROMPT_LAST_CHECK[key] = now
	# resolve file path
	custom_path = os.getenv("GLOBAL_SYSTEM_PROMPT_FILE")
	if custom_path:
		p = Path(custom_path)
	else:
		p = Path("app/services/prompts/REVISED_GLOBAL_SYSTEM_PROMPT.txt")
	try:
		if p.exists():
			mt = p.stat().st_mtime
			cached = _PROMPT_CACHE.get(key)
			if (not cached) or (mt != cached[1]):
				txt = _read_file_text(p)
				if txt and txt.strip():
					_PROMPT_CACHE[key] = (txt, mt)
					return txt
			# unchanged -> return cached
			if cached:
				return cached[0]
	except Exception:
		# ignore file/permission errors; fall back to minimal default
		pass
	# fallback to minimal default
	default_prompt = (
		"Sen HiMan için Instagram DM satış asistanısın. Kısa ve net yanıtla; satış akışını ilerlet.\n"
		"JSON MODE ZORUNLU: Sadece geçerli JSON objesi döndür."
	)
	_PROMPT_CACHE[key] = (default_prompt, _PROMPT_CACHE.get(key, (None, 0.0))[1] if _PROMPT_CACHE.get(key) else 0.0)
	return default_prompt


# Instagram purchase detection and contact extraction prompt (strict JSON)
IG_PURCHASE_SYSTEM_PROMPT = (
    "Sen bir Instagram DM satış analiz yardımcısısın. "
    "Girdi: Türkçe bir konuşmanın kronolojik transkripti (in=müşteri, out=mağaza). "
    "Görev: Satın alma kesinliği olup olmadığını tespit et ve alıcı bilgilerini çıkar. "
    "Kurallar: "
    "1) SADECE geçerli JSON döndür. Açıklama/markdown/yorum YOK. "
    "2) Satın alma varsa (purchase_detected=true), alıcı ad-soyad, telefon ve adres bilgisini konuşmanın TÜMÜNDEN dikkatle tara; önceki/sonraki mesajları kontrol et. "
    "3) İletişim bilgileri yalnızca müşteri mesajlarından (in) çıkarılabilir. Mağaza mesajlarından (out) gelen isim/telefon/adres GEÇERSİZ sayılır. "
    "4) Telefonu mümkünse 05xx… veya +90… formatında normalleştir; boşluk/ayraçları kaldır. "
    "5) Adres tek sahada, satır sonları yerine virgül kullan. "
    "6) Fiyatı da çıkar: anlaşılmış nihai toplam bedeli TL cinsinden 'price' alanına sayı olarak yaz. Birden çok fiyat geçerse en mantıklı son fiyatı seç. "
    "7) Bu çıktı dışa aktarım içindir; uydurma yapma. Gerçekten metinde yoksa null bırak. "
    "8) Satın alma olduğuna karar vermek için asgari doğrulama: müşteri mesajlarından en az bir tanesi (ad-soyad VEYA telefon VEYA adres) açıkça bulunmalı. "
    "   Bu üçünden hiçbiri müşteri mesajlarında yoksa purchase_detected=false olmalı. "
    "9) Aşağıdaki hitap sözcükleri gerçek ad değildir: 'abi', 'abim', 'kardeşim', 'kardesim', 'hocam', 'usta', 'kanka', 'canım', 'canim'. "
    "   Bu tür sözcükler isim alanına yazılmamalı (buyer_name=null bırak). "
    "10) Ürün/beden/renk gibi ipuçlarını product_mentions altında düz metin listele. "
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


# Ad to product matching prompt
AD_PRODUCT_MATCH_SYSTEM_PROMPT = (
    "Sen bir reklam-ürün eşleştirme yardımcısısın. "
    "Girdi: Bir reklam başlığı/açıklaması ve mevcut ürün listesi. "
    "Görev: Reklamın hangi ürünü tanıttığını belirle. "
    "Kurallar: "
    "1) SADECE geçerli JSON döndür. Markdown/kod bloğu/yorum ekleme. "
    "2) Reklam başlığındaki ürün adını, renk/beden/birim gibi varyant bilgilerini ayırt et. "
    "3) Ürün listesindeki ürünlerle karşılaştır ve en uygun eşleşmeyi bul. "
    "4) Eğer reklam başlığında ürün adı açıkça belirtilmişse, o ürünü öner. "
    "5) Eğer tam eşleşme yoksa, en yakın ürünü öner (örneğin 'Pantolon' → 'Jogger Pantolon' gibi). "
    "6) Eğer hiçbir ürünle eşleşme yoksa, product_id null döndür ve notes alanında açıkla. "
    "7) Tüm alanlar çift tırnaklı olmalı. "
    "8) JSON dışına çıkma. "
)


