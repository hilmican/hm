#!/bin/bash
# Kubernetes diagnostic script to check pod status and logs

NAMESPACE="${NAMESPACE:-hm}"

echo "================================================================================"
echo "Kubernetes Diagnostic Report - $(date '+%Y-%m-%d %H:%M:%S')"
echo "================================================================================"
echo ""

echo "1. POD STATUS"
echo "--------------------------------------------------------------------------------"
kubectl get pods -n "$NAMESPACE" -o wide
echo ""

echo "2. PODS WITH ISSUES"
echo "--------------------------------------------------------------------------------"
kubectl get pods -n "$NAMESPACE" | grep -E "(Error|CrashLoopBackOff|Pending|ImagePullBackOff|ErrImagePull)" || echo "✓ No pods with obvious issues"
echo ""

echo "3. RECENT POD EVENTS"
echo "--------------------------------------------------------------------------------"
kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' | tail -20
echo ""

echo "4. DEPLOYMENT STATUS"
echo "--------------------------------------------------------------------------------"
kubectl get deployments -n "$NAMESPACE"
echo ""

echo "5. SERVICE STATUS"
echo "--------------------------------------------------------------------------------"
kubectl get services -n "$NAMESPACE"
echo ""

echo "6. INGRESS STATUS"
echo "--------------------------------------------------------------------------------"
kubectl get ingress -n "$NAMESPACE" 2>/dev/null || echo "No ingress found"
echo ""

echo "7. WORKER POD LOGS (last 20 lines each)"
echo "--------------------------------------------------------------------------------"
for pod in $(kubectl get pods -n "$NAMESPACE" -l app=hm-worker-ingest -o name 2>/dev/null | head -1); do
    echo "--- $pod ---"
    kubectl logs -n "$NAMESPACE" "$pod" --tail=20 2>&1 | tail -20
    echo ""
done

for pod in $(kubectl get pods -n "$NAMESPACE" -l app=hm-worker-reply -o name 2>/dev/null | head -1); do
    echo "--- $pod ---"
    kubectl logs -n "$NAMESPACE" "$pod" --tail=20 2>&1 | tail -20
    echo ""
done

echo "8. RESOURCE USAGE"
echo "--------------------------------------------------------------------------------"
kubectl top pods -n "$NAMESPACE" 2>/dev/null || echo "Metrics server not available"
echo ""

echo "9. DATABASE CONNECTION FROM POD"
echo "--------------------------------------------------------------------------------"
# Try to exec into a worker pod and test DB connection
POD=$(kubectl get pods -n "$NAMESPACE" -l app=hm-worker-reply -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$POD" ]; then
    echo "Testing DB connection from pod: $POD"
    kubectl exec -n "$NAMESPACE" "$POD" -- python -c "
import os
from sqlalchemy import create_engine, text
try:
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        engine = create_engine(db_url, pool_pre_ping=True, connect_args={'connect_timeout': 5})
        with engine.connect() as conn:
            result = conn.execute(text('SELECT 1'))
            print('✓ Database connection successful')
    else:
        print('✗ DATABASE_URL not set in pod')
except Exception as e:
    print(f'✗ Database connection failed: {e}')
" 2>&1 || echo "Could not test DB connection from pod"
else
    echo "No worker pod found to test connection"
fi
echo ""

echo "================================================================================"
echo "Diagnostic complete"
echo "================================================================================"

