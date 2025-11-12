#!/usr/bin/env bash
set -euo pipefail

# Rollout-restart Kubernetes deployments for this app with smart discovery.
# Defaults:
# - Namespace from $HM_K8S_NS or $K8S_NAMESPACE or "default"
# - Prefix discovery ($HM_K8S_PREFIX, default "hm-")
# - kubectl context from $KUBECTL_CONTEXT if set
#
# Features:
# - Discover deployments by prefix or label selector
# - Resolve partial names (e.g. "hm-worker" → hm-worker-enrich, hm-worker-ingest, hm-worker-media)
# - Exclude Redis by default (include via --include-redis)
# - Optional wait for readiness
# - List-only mode
#
# Usage examples:
#   ./scripts/rollout.sh -n hm -w                          # restart all hm-* (except redis) and wait
#   ./scripts/rollout.sh -n hm -d hm-app -d hm-worker       # resolve partial name and restart
#   ./scripts/rollout.sh -n hm --include-redis -w           # include hm-redis too
#   ./scripts/rollout.sh -n hm -l 'app=hm' -w               # use label-based discovery
#   ./scripts/rollout.sh -n hm --list                       # just list discovered names

NS=${HM_K8S_NS:-${K8S_NAMESPACE:-default}}
PREFIX=${HM_K8S_PREFIX:-hm-}
SELECTOR=${HM_K8S_SELECTOR:-}
WAIT=false
LIST_ONLY=false
INCLUDE_REDIS=false
CTX=${KUBECTL_CONTEXT:-}
declare -a REQ_DEPLOYS

usage() {
  cat >&2 <<EOF
Usage: $0 [-n namespace] [-p prefix] [-l selector] [-d name ...] [-w] [--list] [--include-redis] [--context ctx]
Options:
  -n, --namespace        Kubernetes namespace (default: ${NS})
  -p, --prefix           Name prefix for discovery (default: ${PREFIX})
  -l, --selector         Label selector for discovery (overrides prefix)
  -d, --deployment NAME  Deployment name or partial prefix (repeatable)
  -w, --wait             Wait for rollout status to become ready
      --list             List discovered/expanded deployments and exit
      --include-redis    Include hm-redis in operations
      --context CTX      kubectl context to use
  -h, --help             Show this help
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace) NS="$2"; shift 2 ;;
    -p|--prefix)    PREFIX="$2"; shift 2 ;;
    -l|--selector)  SELECTOR="$2"; shift 2 ;;
    -d|--deployment) REQ_DEPLOYS+=("$2"); shift 2 ;;
    -w|--wait)      WAIT=true; shift ;;
    --list)         LIST_ONLY=true; shift ;;
    --include-redis) INCLUDE_REDIS=true; shift ;;
    --context)      CTX="$2"; shift 2 ;;
    -h|--help)      usage ;;
    *) echo "Unknown argument: $1" >&2; usage ;;
  esac
done

command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found in PATH" >&2; exit 127; }

K="kubectl"
if [[ -n "$CTX" ]]; then
  K="$K --context $CTX"
fi

# Get all deployment names in namespace
ALL_RAW="$($K -n "$NS" get deploy -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
declare -a ALL
while IFS= read -r line; do
  [[ -n "$line" ]] && ALL+=("$line")
done <<< "$ALL_RAW"

if [[ ${#ALL[@]} -eq 0 ]]; then
  echo "No deployments found in namespace '$NS'." >&2
  exit 2
fi

exists() {
  local name="$1"
  local d
  for d in "${ALL[@]}"; do
    if [[ "$d" == "$name" ]]; then return 0; fi
  done
  return 1
}

starts_with() {
  local needle="$1"; local name="$2"
  case "$name" in
    ${needle}*) return 0;;
    *) return 1;;
  esac
}

discover_by_selector() {
  local sel="$1"
  local raw
  raw="$($K -n "$NS" get deploy -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  echo "$raw"
}

discover_by_prefix() {
  local pfx="$1"
  local d
  for d in "${ALL[@]}"; do
    if starts_with "$pfx" "$d"; then echo "$d"; fi
  done
}

expand_requested() {
  local r
  declare -a RESULTS
  for r in "$@"; do
    if exists "$r"; then 
      RESULTS+=("$r")
      continue
    fi
    local d
    local found=0
    for d in "${ALL[@]}"; do
      if starts_with "$r" "$d"; then 
        # Check if already in RESULTS to avoid duplicates
        local already=0
        for res in "${RESULTS[@]}"; do
          if [[ "$res" == "$d" ]]; then
            already=1
            break
          fi
        done
        if [[ $already -eq 0 ]]; then
          RESULTS+=("$d")
        fi
        found=1
      fi
    done
    if [[ $found -eq 0 ]]; then
      echo "Warning: deployment '$r' not found in namespace '$NS'" >&2
    fi
  done
  for res in "${RESULTS[@]}"; do
    echo "$res"
  done
}

declare -a TARGETS
if [[ ${#REQ_DEPLOYS[@]} -gt 0 ]]; then
  EXP_RAW="$(expand_requested "${REQ_DEPLOYS[@]}")"
elif [[ -n "$SELECTOR" ]]; then
  EXP_RAW="$(discover_by_selector "$SELECTOR")"
else
  EXP_RAW="$(discover_by_prefix "$PREFIX")"
fi

while IFS= read -r line; do
  [[ -n "$line" ]] && TARGETS+=("$line")
done <<< "$EXP_RAW"

# Deduplicate TARGETS array (compatible with older bash)
declare -a UNIQ_TARGETS
for d in "${TARGETS[@]:-}"; do
  found=0
  for u in "${UNIQ_TARGETS[@]:-}"; do
    if [[ "$u" == "$d" ]]; then
      found=1
      break
    fi
  done
  if [[ $found -eq 0 ]]; then
    UNIQ_TARGETS+=("$d")
  fi
done
TARGETS=("${UNIQ_TARGETS[@]:-}")

if ! $INCLUDE_REDIS; then
  # filter out hm-redis unless explicitly requested
  declare -a TMP
  for d in "${TARGETS[@]:-}"; do
    if [[ "$d" == hm-redis ]]; then continue; fi
    TMP+=("$d")
  done
  TARGETS=("${TMP[@]:-}")
fi

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No target deployments resolved. Try --list to inspect discovery." >&2
  exit 3
fi

echo "Namespace: $NS"
echo "Deployments: ${TARGETS[*]}"

if $LIST_ONLY; then exit 0; fi

for d in "${TARGETS[@]}"; do
  echo "→ Restarting deployment/$d"
  # Get current replica count
  current_replicas="$($K -n "$NS" get deploy "$d" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")"
  if [[ -z "$current_replicas" ]] || [[ "$current_replicas" == "0" ]]; then
    current_replicas=1
  fi
  
  echo "  Scaling down to 0 replicas..."
  $K -n "$NS" scale deploy "$d" --replicas=0
  
  echo "  Waiting for pods to terminate..."
  # Wait for all pods to be terminated
  timeout=60
  elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    pod_count="$($K -n "$NS" get pods -l "app=$d" --no-headers 2>/dev/null | grep -v "Terminating" | wc -l | tr -d ' ' || echo "0")"
    if [[ "$pod_count" == "0" ]]; then
      break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  
  if [[ $elapsed -ge $timeout ]]; then
    echo "  ⚠️  Warning: Some pods may still be terminating, proceeding anyway"
  else
    echo "  All pods terminated"
  fi
  
  # Brief pause to ensure cleanup
  sleep 2
  
  echo "  Scaling back up to $current_replicas replicas..."
  $K -n "$NS" scale deploy "$d" --replicas="$current_replicas"
done

if $WAIT; then
  for d in "${TARGETS[@]}"; do
    echo "→ Waiting for deployment/$d to become ready"
    $K -n "$NS" rollout status deploy "$d"
  done
fi

echo "Done."


