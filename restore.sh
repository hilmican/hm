#!/usr/bin/env bash
# Stop on error and print commands (debug)
set -eo pipefail
set -x

NS=hm
K=kubectl

# 1) List deployments (sanity)
echo "[INFO] Deployments in namespace: $NS"
$K -n "$NS" get deploy -o wide || true

# Capture original replica counts so we can restore them later
ORIG_REPS=$(mktemp)
$K -n "$NS" get deploy -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.spec.replicas}{"\n"}{end}' > "$ORIG_REPS" || true
echo "[INFO] Saved original replicas to $ORIG_REPS"

# 2) Scale down all deployments except hm-redis
DEPS=$($K -n "$NS" get deploy -o name | sed -n 's#^deployment\.apps/##p' | grep -v '^hm-redis$' || true)
if [ -z "$DEPS" ]; then
  echo "[ERROR] No deployments found to scale in namespace $NS" >&2
  exit 2
fi
echo "[INFO] Scaling down deployments:"
printf '%s\n' "$DEPS" | sed 's/^/  - /'
for d in $DEPS; do
  $K -n "$NS" scale deploy "$d" --replicas=0
done

# 3) Create a work pod with the app PVC mounted
echo "[INFO] Creating work pod hm-sqlite-work"
# Ensure any previous pod is removed to avoid Completed state blocking readiness
$K -n "$NS" delete pod hm-sqlite-work --ignore-not-found || true
$K -n "$NS" apply -f k8s-sqlite-work.yaml
if ! $K -n "$NS" wait --for=condition=Ready pod/hm-sqlite-work --timeout=180s; then
  echo "[ERROR] Work pod did not become Ready within timeout. Describing pod and recent events:"
  $K -n "$NS" describe pod hm-sqlite-work || true
  echo "[DEBUG] Pod phase: $($K -n "$NS" get pod hm-sqlite-work -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  $K -n "$NS" get events --sort-by=.lastTimestamp | tail -50 || true
  echo "[HINT] Retrying with inline work pod manifest using mediatriple/hm:0.5 and no nodeName"
  # Retry path: delete and create a simple work pod using the app image (already pulled) without node pinning
  $K -n "$NS" delete pod hm-sqlite-work --ignore-not-found || true
  cat <<'YAML' | $K apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: hm-sqlite-work
  namespace: hm
spec:
  restartPolicy: Never
  securityContext:
    fsGroup: 1000
    fsGroupChangePolicy: OnRootMismatch
  volumes:
    - name: app-root
      persistentVolumeClaim:
        claimName: hm-app-root
  containers:
    - name: work
      image: mediatriple/hm:0.5
      command: ["/bin/sh","-lc"]
      args: ["sleep 3600"]
      volumeMounts:
        - name: app-root
          mountPath: /app
YAML
  if ! $K -n "$NS" wait --for=condition=Ready pod/hm-sqlite-work --timeout=180s; then
    echo "[ERROR] Inline work pod also failed to become Ready. Describing:"
    $K -n "$NS" describe pod hm-sqlite-work || true
    echo "[DEBUG] Pod phase: $($K -n "$NS" get pod hm-sqlite-work -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    exit 3
  fi
fi

# 4) Restore yesterday’s backup (fallback to latest if yesterday missing)
echo "[INFO] Restoring backup inside work pod"
# Temporarily disable nounset in case parent shell was invoked with -u
set +u || true
$K -n "$NS" exec -it hm-sqlite-work -- sh -lc '
set -e
cd /app
echo "[DEBUG] Work dir: $(pwd)"
ls -l dbbackups | head -50 || true
ts=$(date +%s)
if [ -f data/app.db ]; then
  cp data/app.db "data/app.db.pre-restore.$ts"
  echo "[INFO] Backed up current DB to data/app.db.pre-restore.$ts"
fi
# Allow overriding date via BACKUP_DATE=YYYYMMDD
Y=""; cand=""; Y_IN="${BACKUP_DATE:-}"
if [ -n "$Y_IN" ]; then Y="$Y_IN"; else
  Y=$(date -u -d "yesterday" +%Y%m%d 2>/dev/null || date -u -v-1d +%Y%m%d 2>/dev/null || gdate -u -d "yesterday" +%Y%m%d 2>/dev/null || echo "")
fi
echo "[DEBUG] Desired backup date (UTC): ${Y:-<none>}"
if [ -n "$Y" ]; then
  cand=$(ls -1t dbbackups/app-${Y}-*.db 2>/dev/null | head -1 || true)
else
  cand=""
fi
if [ -z "$cand" ]; then
  echo "[WARN] No backup found for date '$Y'. Falling back to most recent backup."
  cand=$(ls -1t dbbackups/app-*.db 2>/dev/null | head -1 || true)
fi
if [ -z "$cand" ]; then echo "[ERROR] No backups found in /app/dbbackups"; exit 4; fi
echo "[INFO] Restoring: $cand"
cp "$cand" data/app.db
rm -f data/app.db-wal data/app.db-shm
if command -v sqlite3 >/dev/null 2>&1; then
  echo "[INFO] Running integrity check"
  sqlite3 data/app.db "PRAGMA integrity_check;" || true
fi
ls -l data | sed "s/^/[DEBUG] /"
'
# Re-enable nounset if desired; ignore if not set previously
set -u || true

# 5) Remove the work pod
echo "[INFO] Deleting work pod hm-sqlite-work"
$K -n "$NS" delete pod hm-sqlite-work --ignore-not-found

# 6) Scale back up to original replicas (default 1 if unknown)
echo "[INFO] Restoring replica counts"
while IFS='|' read -r name reps; do
  [ -z "$name" ] && continue
  [ "$name" = "hm-redis" ] && continue
  desired=${reps:-1}
  if [ "$desired" = "0" ] || [ -z "$desired" ]; then desired=1; fi
  echo "[INFO] Scaling $name to $desired"
  $K -n "$NS" scale deploy "$name" --replicas="$desired"
done < "$ORIG_REPS"

# 7) Flush short-lived Redis caches (if hm-redis exists)
if $K -n "$NS" get deploy hm-redis >/dev/null 2>&1; then
  echo "[INFO] Flushing Redis cache keys"
  $K -n "$NS" exec deploy/hm-redis -- sh -lc "redis-cli --scan --pattern 'dash:*' | xargs -r -n500 redis-cli del"
  $K -n "$NS" exec deploy/hm-redis -- sh -lc "redis-cli --scan --pattern 'rep:daily:pay:*' | xargs -r -n500 redis-cli del"
fi

echo "[INFO] Restore complete."