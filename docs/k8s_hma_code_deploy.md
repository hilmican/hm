# HMA (`hma.cdn.com.tr`) – Kubernetes’te kod güncelleme

## Üretim / `hma.cdn.com.tr` — yapılacak aksiyonlar

1. **Uygulama kodu (PVC)**  
   Güncel `app/` (ör. `main.py` CORS, router’lar) cluster’daki `/app/app/` ile aynı olmalı. Tek komut: repo kökünden `./scripts/k8s_sync_hm_app.sh` (aşağıdaki `kubectl cp` + pod silme ile aynı işi yapar).

2. **CORS ortam değişkenleri (çoğu senaryoda gerekmez)**  
   `app/main.py` zaten `https://hma.cdn.com.tr` + localhost/LAN regex kullanır. Yalnızca **ek tam origin** (regex’e sığmayan bir domain) gerekiyorsa deployment’a ekleyin:
   ```bash
   kubectl set env deployment/hm-app -n hm \
     HMA_CORS_ORIGINS='https://hma.cdn.com.tr,http://localhost:60588'
   ```
   Özel regex için: `HMA_CORS_ORIGIN_REGEX` (boş bırakılırsa kod içindeki varsayılan kullanılır).

3. **Ingress / CDN / nginx**  
   İstek FastAPI’ye ulaşmadan OPTIONS kesiliyorsa veya yanıttan `Access-Control-Allow-Origin` siliniyorsa tarayıcıda yine `Failed to fetch` görülür. Edge’de CORS’u **FastAPI’ye bırakın** (çoğu kurulumda yalnızca proxy pass + timeout yeterli).

4. **Mobil API anahtarı**  
   Üretimde `HMA_MOBILE_API_KEY` set ise Flutter’da aynı key `X-Mobile-API-Key` ile gönderilmeli.

5. **Doğrulama (isteğe bağlı)**  
   Preflight örneği (origin’i kendi Flutter web adresinizle değiştirin):
   ```bash
   curl -sS -D - -o /dev/null -X OPTIONS 'https://hma.cdn.com.tr/magaza-satis/api/series-print-and-stock' \
     -H 'Origin: http://localhost:60588' \
     -H 'Access-Control-Request-Method: POST' \
     -H 'Access-Control-Request-Headers: content-type,x-mobile-api-key'
   ```
   Yanıtta `access-control-allow-origin` (veya regex eşleşmesi ile aynı origin) görülmeli.

---

## Önemli: PVC üzerinde çalışan uygulama kodu

`hm-app` pod’unda `/app` **PersistentVolume** (`hm-app-root`) ile mount edilir. Init container yalnızca PVC boşsa imajdan kopyalar; doluysa **yeni Docker imajı build/push etmek tek başına kodu güncellemez**.

Önerilen akış (imaj yenilemeden):

1. Pod’un **Ready** olduğundan emin ol.
2. Yerel `app/` içeriğini pod üzerinden PVC’ye kopyala (`kubectl cp`).
3. Uygulama işlemini temiz başlatmak için **pod’u sil**; Deployment aynı PVC ile yeni pod ayağa kaldırır ve güncel dosyaları kullanır.

**Hızlı yol (önerilen):**

```bash
cd /path/to/hm
chmod +x scripts/k8s_sync_hm_app.sh
./scripts/k8s_sync_hm_app.sh
```

**Manuel:**

```bash
NS=hm
LABEL=app=hm-app
CONT=app   # deployment’taki uygulama container adı

POD=$(kubectl get pod -n "$NS" -l "$LABEL" -o jsonpath='{.items[0].metadata.name}')
kubectl wait --for=condition=Ready "pod/$POD" -n "$NS" --timeout=120s

# Tüm Python paketini senkronize et (yerel hm/app → pod /app/app)
kubectl cp /path/to/hm/app/. "$NS/$POD:/app/app/" -n "$NS" -c "$CONT"

kubectl delete pod -n "$NS" "$POD"
```

**Not:** `kubectl cp` uzun sürebilir; bitmeden pod silmeyin. İlk komut 30 sn’de kesilen ortamlarda önce `kubectl cp`’nin tamamlandığını doğrulayın, sonra `kubectl delete pod` çalıştırın.

Sadece birkaç dosya değiştiyse tek tek de kopyalanabilir:

```bash
kubectl cp ./app/services/mobile_qr.py hm/$POD:/app/app/services/mobile_qr.py -n hm -c app
kubectl cp ./app/routers/magaza_satis.py hm/$POD:/app/app/routers/magaza_satis.py -n hm -c app
```

Alternatif: `kubectl rollout restart deployment/hm-app -n hm` (aynı pod sürecini yeniden başlatır; PVC aynı kalır).

## İmaj ile güncelleme (isteğe bağlı)

İmaj değiştirmek PVC’yi otomatik güncellemez; init yalnızca boş volume’da doldurur. İmaj güncellemesi + PVC senkronu birlikte kullanılabilir.

## CORS (Flutter web / tarayıcı)

`app/main.py` içinde `CORSMiddleware` etkin: varsayılan olarak `https://hma.cdn.com.tr` ve regex ile `http(s)://localhost`, `127.0.0.1`, `[::1]`, ayrıca yaygın özel ağ hostları (`192.168.x.x`, `10.x.x.x`, `172.16–31.x.x`) ve isteğe bağlı port izinlidir.

İsteğe bağlı ek origin’ler:

- `HMA_CORS_ORIGINS` — virgülle ayrılmış liste (örn. `https://staging.example.com`)

Regex’i devre dışı bırakmak için deployment’ta boş string vermek yerine çok dar bir regex kullanın veya kodu özelleştirin.

## Mobil API ortamı

İsteğe bağlı: `HMA_MOBILE_API_KEY` tanımlanırsa mobil endpoint’ler `X-Mobile-API-Key` ister.

```bash
kubectl -n hm create secret generic hm-mobile-api \
  --from-literal=HMA_MOBILE_API_KEY='güçlü-bir-anahtar' \
  --dry-run=client -o yaml | kubectl apply -f -
```

Ardından `hm-app` deployment’a env ekle: `secretKeyRef` → `hm-mobile-api` / `HMA_MOBILE_API_KEY`.

## Flutter / yerel derleme

Flutter SDK: `~/flutter` (stable, `GIT_HTTP_VERSION=HTTP/1.1` ile clone önerilir). İlk kurulumda Dart SDK indirmesi uzun sürebilir; bozuk zip olursa:

```bash
rm -f ~/flutter/bin/cache/dart-sdk-darwin-x64.zip
rm -rf ~/flutter/bin/cache/dart-sdk
export PATH="$HOME/flutter/bin:$PATH"
flutter doctor
```

Sonra: [mobile/README.md](../mobile/README.md) içindeki `bootstrap.sh` ve `flutter build apk`.
