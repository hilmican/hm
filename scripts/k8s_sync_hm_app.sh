#!/usr/bin/env bash
# hm-app PVC üzerindeki /app/app dizinine yerel app/ kodunu kopyala ve pod'u yenile.
# Kullanım: ./scripts/k8s_sync_hm_app.sh
# Ortam: K8S_NS=hm HM_APP_LABEL=app=hm-app HM_APP_CONTAINER=app

set -euo pipefail

NS="${K8S_NS:-hm}"
LABEL="${HM_APP_LABEL:-app=hm-app}"
CONT="${HM_APP_CONTAINER:-app}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_SRC="$REPO_ROOT/app"

if [[ ! -d "$APP_SRC" ]]; then
  echo "Hata: $APP_SRC bulunamadı." >&2
  exit 1
fi

echo "==> Namespace: $NS | selector: $LABEL | container: $CONT"
POD="$(kubectl get pod -n "$NS" -l "$LABEL" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -z "$POD" ]]; then
  echo "Hata: pod bulunamadı: kubectl get pods -n $NS -l $LABEL" >&2
  exit 1
fi
echo "    Pod: $POD"

echo "==> Ready bekleniyor (120s)"
kubectl wait --for=condition=Ready "pod/$POD" -n "$NS" --timeout=120s

echo "==> kubectl cp: $APP_SRC -> $NS/$POD:/app/app/"
kubectl cp "$APP_SRC/." "$NS/$POD:/app/app/" -n "$NS" -c "$CONT"

echo "==> Pod siliniyor (Deployment yeni pod açar)"
kubectl delete pod -n "$NS" "$POD" --wait=true

echo "==> Yeni pod"
NEW_POD="$(kubectl get pod -n "$NS" -l "$LABEL" -o jsonpath='{.items[0].metadata.name}')"
echo "    Pod: $NEW_POD"
kubectl wait --for=condition=Ready "pod/$NEW_POD" -n "$NS" --timeout=180s
echo "Tamam: $NEW_POD Ready"
