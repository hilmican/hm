#!/usr/bin/env bash
# Reply worker loglarını incelemek için: conversation_id veya genel görsel/cevap logları.
# Kullanım:
#   ./scripts/check_reply_logs.sh           # son 300 satır, ai_shadow + image
#   ./scripts/check_reply_logs.sh 1683      # conversation_id=1683 ile ilgili
#   ./scripts/check_reply_logs.sh 1683 500  # son 500 satır, cid 1683

set -e
NAMESPACE="${NAMESPACE:-hm}"
LABEL="app=hm-worker-reply"
TAIL="${2:-300}"
CID="${1:-}"

if [[ -n "$CID" ]]; then
  echo "=== Reply worker logs (conversation_id=$CID, last $TAIL lines) ==="
  kubectl logs -n "$NAMESPACE" -l "$LABEL" --tail="$TAIL" --all-containers=true 2>/dev/null | grep -E "conversation_id=$CID|conversation_id=$CID[^0-9]|cid=$CID|$CID|image_urls received|after_absolute_filter|image summary|image API error|no images delivered|ai_shadow: (sending|auto-sent|including|skipping|generating)" || true
else
  echo "=== Reply worker logs (ai_shadow + image, last $TAIL lines) ==="
  kubectl logs -n "$NAMESPACE" -l "$LABEL" --tail="$TAIL" --all-containers=true 2>/dev/null | grep -E "ai_shadow:|image_urls received|after_absolute_filter|image summary|image API error|no images delivered|Instagram send_message|worker_reply:" || true
fi
