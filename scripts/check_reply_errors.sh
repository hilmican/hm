#!/usr/bin/env bash
# Geçmiş reply worker ve ilgili loglarda API hatalarını / görsel sorunlarını listeler.
# Hangi API ne hatası vermiş görmek için:
#   ./scripts/check_reply_errors.sh          # son 1000 satır, sadece hata/uyarı
#   ./scripts/check_reply_errors.sh 2000     # son 2000 satır
#   ./scripts/check_reply_errors.sh 2000 1683  # son 2000 + conversation_id 1683

set -e
NAMESPACE="${NAMESPACE:-hm}"
TAIL="${1:-1000}"
CID="${2:-}"
LABEL="app=hm-worker-reply"

# Hata/uyarı kalıpları (kodda loglananlar):
# - Instagram: image API error, image failed, dropped all, Graph send failed, image summary sent=0
# - WhatsApp: image API error, image skipped, dropped all, text empty
# - Worker: no images delivered, some image URLs dropped, reply_text empty
# - image_urls: IMAGE_CDN_BASE_URL not set (relative URL)
PATTERNS="image API error|image failed|dropped all|Graph send failed|partial send|no images delivered|some image URLs dropped|reply_text was empty|text empty, sending only|after_absolute_filter=0|image summary sent=0|IMAGE_CDN_BASE_URL"

echo "=== Reply worker – hata/uyarı logları (son $TAIL satır) ==="
echo ""

LOG=$(kubectl logs -n "$NAMESPACE" -l "$LABEL" --tail="$TAIL" --all-containers=true 2>/dev/null || true)
if [[ -z "$LOG" ]]; then
  echo "Log alınamadı (kubectl erişimi veya pod yok?)."
  exit 0
fi

if [[ -n "$CID" ]]; then
  echo "--- conversation_id=$CID ile ilgili tüm satırlar ---"
  echo "$LOG" | grep -E "conversation_id=$CID|conversation_id=$CID[^0-9]|cid=$CID|$CID" || true
  echo ""
fi

echo "--- Hata/uyarı satırları (API + görsel) ---"
echo "$LOG" | grep -iE "$PATTERNS" || true

echo ""
echo "--- Instagram send_message özetleri (received / after_filter / summary) ---"
echo "$LOG" | grep -E "Instagram send_message:|image_urls received|after_absolute_filter|image summary" || true
