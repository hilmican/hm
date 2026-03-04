#!/usr/bin/env bash
# Add HIMAN_WOO_* env vars from secret hm-himan-woo to deployment hm-app (for sync-from-himan).
# Secret and env are already applied; this script is for fresh installs or re-adding after deploy overwrite.
# To create WooCommerce API key in himan.com.tr pod: see himansite/sync/README.md and create_woo_api_key.php
set -euo pipefail
NS="${K8S_NAMESPACE:-hm}"
DEPLOY="hm-app"
SECRET="hm-himan-woo"

if ! kubectl get secret "$SECRET" -n "$NS" &>/dev/null; then
  echo "Secret $SECRET not found in namespace $NS. Create it first:"
  echo "  kubectl apply -f k8s-himan-woo-secret.yaml"
  echo "  # Then create API key in WordPress pod (himansite/sync/create_woo_api_key.php) and update secret with real values."
  exit 1
fi

# Add env vars from secret if not already present
for key in HIMAN_WOO_BASE_URL HIMAN_WOO_CONSUMER_KEY HIMAN_WOO_CONSUMER_SECRET; do
  if kubectl get deployment "$DEPLOY" -n "$NS" -o jsonpath="{.spec.template.spec.containers[0].env}" | grep -q "\"name\":\"$key\""; then
    echo "Env $key already present in $DEPLOY"
  else
    kubectl patch deployment "$DEPLOY" -n "$NS" --type=json -p="[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/env/-\",\"value\":{\"name\":\"$key\",\"valueFrom\":{\"secretKeyRef\":{\"name\":\"$SECRET\",\"key\":\"$key\"}}}}]"
    echo "Added $key from secret $SECRET"
  fi
done
echo "Done. Restart to pick up env: kubectl rollout restart deployment/$DEPLOY -n $NS"
