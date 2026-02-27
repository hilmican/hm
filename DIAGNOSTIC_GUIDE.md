# Diagnostic Guide: MySQL Table Locks & Kubernetes Issues

This guide helps you diagnose why `hma.cdn.com.tr` can't be opened and identify MySQL table locks.

## Quick Diagnosis

### 1. Check MySQL Table Locks

Run the MySQL diagnostic script:

```bash
# Set your database URL
export DATABASE_URL="mysql+pymysql://hm:0Nh4shgeoYmhhe+!d@185.70.97.125:30336/appdb_h?charset=utf8mb4"

# Run the diagnostic
python scripts/diagnose_mysql_locks.py
```

This will show you:
- **Table locks** - Which tables are locked and by which queries
- **Long-running queries** - Queries running > 5 seconds
- **Active processes** - All current database connections
- **Locked tables** - Tables with explicit locks
- **Connection statistics** - Current connection usage

### 2. Check Kubernetes Status

Run the Kubernetes diagnostic script:

```bash
# Set namespace (default is 'hm')
export NAMESPACE=hm

# Run the diagnostic
./scripts/diagnose_k8s.sh
```

This will show you:
- Pod status and health
- Recent events and errors
- Deployment status
- Service and ingress status
- Recent logs from worker pods
- Resource usage

## Manual Commands

### MySQL - Check Table Locks Directly

```bash
# Connect to MySQL
mysql -h 185.70.97.125 -P 30336 -u hm -p appdb_h

# Then run these queries:

# 1. Check for lock waits
SELECT 
    r.trx_id waiting_trx_id,
    r.trx_mysql_thread_id waiting_thread,
    r.trx_query waiting_query,
    b.trx_id blocking_trx_id,
    b.trx_mysql_thread_id blocking_thread,
    b.trx_query blocking_query,
    l.lock_table,
    TIMESTAMPDIFF(SECOND, r.trx_wait_started, NOW()) as wait_seconds
FROM information_schema.innodb_lock_waits w
INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
INNER JOIN information_schema.innodb_locks l ON l.lock_id = w.requested_lock_id;

# 2. Check long-running queries
SELECT 
    id,
    user,
    host,
    db,
    command,
    time as duration_seconds,
    state,
    LEFT(info, 200) as query
FROM information_schema.processlist
WHERE time > 5
  AND command != 'Sleep'
ORDER BY time DESC;

# 3. Check all processes
SHOW PROCESSLIST;

# 4. Check locked tables
SHOW OPEN TABLES WHERE In_use > 0;

# 5. Check INNODB status (includes lock info)
SHOW ENGINE INNODB STATUS\G
```

### Kubernetes - Manual Checks

```bash
# Check all pods
kubectl get pods -n hm -o wide

# Check pod logs
kubectl logs -n hm <pod-name> --tail=100

# Check pod events
kubectl describe pod -n hm <pod-name>

# Check deployments
kubectl get deployments -n hm

# Check services
kubectl get services -n hm

# Check ingress (for hma.cdn.com.tr)
kubectl get ingress -n hm

# Check ingress details
kubectl describe ingress -n hm <ingress-name>
```

## Common Issues & Solutions

### Issue: Table Lock Detected

**Symptoms:**
- Queries hanging
- Timeouts
- "Lock wait timeout exceeded" errors

**Solution:**
1. Identify the blocking query from the diagnostic output
2. Check which table is locked
3. Kill the blocking process if safe:
   ```sql
   KILL <process_id>;
   ```
4. Or wait for the blocking transaction to complete

### Issue: Long-Running Query

**Symptoms:**
- One query running for a very long time
- Other queries waiting

**Solution:**
1. Check the query from diagnostic output
2. If it's safe to kill:
   ```sql
   KILL <process_id>;
   ```
3. Review the query and add indexes if needed
4. Consider optimizing the query

### Issue: Too Many Connections

**Symptoms:**
- "Too many connections" errors
- Can't connect to database

**Solution:**
1. Check connection count:
   ```sql
   SHOW STATUS LIKE 'Threads_connected';
   SHOW VARIABLES LIKE 'max_connections';
   ```
2. Kill idle connections if needed
3. Increase `max_connections` if necessary

### Issue: Kubernetes Pods Not Starting

**Symptoms:**
- Pods in CrashLoopBackOff
- Pods in Pending state
- Service unreachable

**Solution:**
1. Check pod logs: `kubectl logs -n hm <pod-name>`
2. Check pod events: `kubectl describe pod -n hm <pod-name>`
3. Check if database is reachable from pod
4. Check resource limits

### Issue: Ingress Not Working (hma.cdn.com.tr)

**Symptoms:**
- Domain not accessible
- 502/503 errors

**Solution:**
1. Check ingress: `kubectl get ingress -n hm`
2. Check ingress controller: `kubectl get pods -n ingress-nginx` (or your ingress namespace)
3. Check service endpoints: `kubectl get endpoints -n hm`
4. Check DNS resolution

## Quick Fixes

### Kill a Blocking MySQL Process

```sql
-- First, identify the process
SHOW PROCESSLIST;

-- Then kill it (replace <id> with actual process ID)
KILL <id>;
```

### Instagram Inbox: New Messages Not Appearing

Akış: Webhook (hm-app) → `raw_events` tablosu → Redis `jobs:ingest` kuyruğu → **hm-worker-ingest** pod'u işler → `message` + `conversations` güncellenir. Inbox listesi `conversations` tablosundan okunur.

**1. Ingest worker çalışıyor mu?**

```bash
kubectl get pods -n hm -l app=hm-worker-ingest
kubectl logs -n hm -l app=hm-worker-ingest --tail=100
```

Pod `Running` ve crash-loop yok olmalı. Log’da `ingest ok jid=... raw=... inserted=...` satırları yeni mesajların işlendiğini gösterir.

**2. Worker’da DATABASE_URL var mı?**

Ingest worker DB’ye bağlanmak zorunda. Eksikse pod başlarken hata alır veya işler çalışmaz:

```bash
kubectl get deployment -n hm hm-worker-ingest -o jsonpath='{.spec.template.spec.containers[0].env}' | jq '.[] | select(.name=="DATABASE_URL")'
```

Boşsa `k8s-workers.yaml` içinde `hm-worker-ingest` env’e `DATABASE_URL` ekleyip rollout yapın.

**3. Kuyruk doluluk / uygulama tarafı**

Uygulama (hm-app) aynı Redis’e yazıyor mu kontrol için queue status:

```bash
curl -s "https://hma.cdn.com.tr/queue/status"
# veya pod içinden
kubectl exec -n hm deploy/hm-app -- python -c "
from app.services.queue import _get_redis
r = _get_redis()
print('jobs:ingest length', r.llen('jobs:ingest'))
"
```

`jobs:ingest` sürekli çok yüksekse worker yetişemiyor veya hiç tüketmiyor olabilir.

**4. Son webhook’lar raw_events’e yazılmış mı?**

```bash
# MySQL’e bağlanıp
SELECT id, object, created_at FROM raw_events ORDER BY id DESC LIMIT 10;
```

Son dakikalarda satır gelmiyorsa webhook ya gelmiyor ya da signature/DB hatası var (hm-app log’larına bakın).

**5. Ingest işlenmiş mi?**

Worker log’unda ilgili `raw_event_id` için `ingest ok ... inserted=1` (veya >0) görülmeli. Hata varsa `ingest fail jid=... err=...` çıkar.

**6. Hızlı test: Ingest worker’ı yeniden başlat**

```bash
kubectl rollout restart deployment -n hm hm-worker-ingest
kubectl rollout status deployment -n hm hm-worker-ingest
kubectl logs -n hm -l app=hm-worker-ingest -f --tail=50
```

Sonra yeni bir DM atıp birkaç saniye içinde inbox’ta görünüp görünmediğine bakın.

### Instagram token süresi dolmuş / Access Denied

Worker log’unda **access denied**, **403**, **Invalid OAuth** veya **token expired** benzeri hatalar görüyorsanız Instagram/Facebook **erişim token’ı** süresi dolmuş veya iptal edilmiş olabilir.

**Token’lar:**
- **Page Access Token** (IG_PAGE_ACCESS_TOKEN): Genelde 60 gün; süresi dolunca yenilemeniz gerekir.
- **User Access Token** (IG_ACCESS_TOKEN): Kısa veya uzun ömürlü olabilir; uzun ömürlü için Meta üzerinden exchange yapılır.

**1. Hangi token’ın kullanıldığını kontrol edin (secret açıklanmaz):**

```bash
curl -s "https://hma.cdn.com.tr/ig/debug/env"
# active_path: "page" veya "user", token_len, token_suffix gösterir
```

**2. Token’ı test edin (Graph API):**

[Graph API Explorer](https://developers.facebook.com/tools/explorer/) veya:

```bash
# Page token ile örnek (IG_PAGE_ID ve token'ı kendi değerlerinizle değiştirin)
curl -s "https://graph.facebook.com/v21.0/me?access_token=TOKEN"
# Geçerli token: {"id":"...","name":"..."}
# Süresi dolmuş: {"error":{"message":"Error validating access token...","code":190}}
```

**3. Uzun ömürlü Page token almak:**

- [Meta for Developers](https://developers.facebook.com/) → Uygulamanız → Tools → Graph API Explorer.
- Sayfa token’ı ile **Page** seçin, **Get User Access Token** / **Get Page Access Token** ile gerekli izinleri seçin (`instagram_basic_manage_messages`, `instagram_manage_messages` vb.).
- **Generate Access Token** → çıkan token’ı kopyalayın.
- Uzun ömürlü token için: [Access Token Debugger](https://developers.facebook.com/tools/debug/accesstoken/) ile token’ı girip **Extend** (uzat) ile 60 günlük hale getirin.

**4. K8s secret’ı güncellemek:**

Token’ı aldıktan sonra `hm` namespace’indeki `hm-ig` secret’ında ilgili alanı güncelleyin:

```bash
# Mevcut secret'ı düzenleyin (IG_PAGE_ACCESS_TOKEN veya IG_ACCESS_TOKEN)
kubectl edit secret -n hm hm-ig
# base64 ile: echo -n "YENI_TOKEN" | base64
# veya tek satırda:
kubectl patch secret -n hm hm-ig -p '{"data":{"IG_PAGE_ACCESS_TOKEN":"'$(echo -n "YENI_TOKEN_BURAYA" | base64)'"}}'
```

**5. Pod’ları yeniden başlatın** (yeni env’i almaları için):

```bash
kubectl rollout restart deployment -n hm hm-app
kubectl rollout restart deployment -n hm hm-worker-ingest
kubectl rollout restart deployment -n hm hm-worker-enrich
kubectl rollout restart deployment -n hm hm-worker-reply
# Gerekirse diğer worker'lar
```

Bundan sonra yeni gelen mesajlar tekrar işlenmeli. (Kod tarafında, token hatası olsa bile mesajın kaydedilmesi için ingest’te enrichment hatası yakalanıyor; yine de token’ı yenilemek kullanıcı adı vb. alanların dolu gelmesi için gerekli.)

### Restart a Kubernetes Deployment

```bash
# Restart all workers
kubectl rollout restart deployment -n hm hm-worker-ingest
kubectl rollout restart deployment -n hm hm-worker-reply
kubectl rollout restart deployment -n hm hm-worker-enrich
kubectl rollout restart deployment -n hm hm-worker-media
```

### Check Database Connection from Pod

```bash
# Get a pod name
POD=$(kubectl get pods -n hm -l app=hm-worker-reply -o jsonpath='{.items[0].metadata.name}')

# Test connection
kubectl exec -n hm $POD -- python -c "
from sqlalchemy import create_engine, text
import os
engine = create_engine(os.getenv('DATABASE_URL'), pool_pre_ping=True)
with engine.connect() as conn:
    print('Connected!')
    result = conn.execute(text('SELECT 1'))
    print('Query successful!')
"
```

## Next Steps

1. Run both diagnostic scripts
2. Identify the specific table that's locked (from MySQL diagnostic)
3. Check which query is blocking (from MySQL diagnostic)
4. Check Kubernetes pod status (from K8s diagnostic)
5. Take appropriate action based on findings

