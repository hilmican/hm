# HMA Stok (Flutter)

## API adresi

Varsayılan API **`https://hma.cdn.com.tr`** (`lib/config/api_config.dart`). İsterseniz:

```bash
--dart-define=HMA_BASE_URL=https://hma.cdn.com.tr \
--dart-define=HMA_MOBILE_API_KEY=gizli_anahtar
```

Ayarlar ekranından Base URL / key kalıcı olarak değiştirilebilir.

## Etiket / QR yazdırma

**Seri stok** ekranında stok eklendikten (veya dry-run önizleme) sonra:

- **PDF — yazdır / paylaş:** Her kod için küçük etiket boyutlu PDF; sistem yazdır penceresinden **aynı ağdaki** AirPrint / paylaşılan etiket yazıcısını seçin.
- **Metinleri kopyala:** `SKU`, beden ve `qr_data` (ör. `hma:unit:123`) — Excel veya üretici etiket yazılımına yapıştırılabilir.

Ayrıntı: `docs/stock_units.md` → *Etiket yazdırma*.

## Kargo etiketi (OCR + kapıda ödeme)

**Kargo etiketi** akışında **Etiketi okut (kod + OCR)** tek kamera açılışında barkod/QR ve aynı kareden etiket metnini **ML Kit** okur; API’ye `ocr_text` gider. İsteğe bağlı **Sadece QR/barkod** (OCR yok). Sepet ve tamamlama ekranında alıcı / adres / içerik / tahsilat özeti gösterilir.  
**Tamamla** ekranında varsayılan **kapıda ödeme** (sipariş ödenmemiş `placeholder`); **Mağaza: ödeme şimdi alındı** açılırsa nakit/havale ile `paid` + ödeme kaydı oluşturulur. Ayrıntı: `docs/hma_mobile_api.md`.

## Xcode uyarıları (sık görülenler)

| Uyarı | Açıklama |
|--------|-----------|
| **Pods … IPHONEOS_DEPLOYMENT_TARGET 9.0 / 10.0** | `ios/Podfile` içindeki `post_install` tüm pod hedeflerini en az **12.0** yapar. `pod install` sonrası uyarılar kaybolmalı. |
| **MLKitBarcodeScanning — No platform load command** | Google’ın önceden derlenmiş `.framework` dosyasından; genelde **zararsız**, yoksayabilirsiniz. |
| **printing — `keyWindow` deprecated** | Üçüncü parti paket; yalnızca **deprecation** uyarısı, işlev genelde normal. Güncel `printing` sürümü `pubspec` ile çekiliyor. |
| **Runner / Pods — Update to recommended settings** | İsterseniz Xcode’da bir kez **Perform Changes**; `Runner` projesinde `LastUpgradeCheck` güncellendi, uyarı azalabilir. |

## iPhone’da uygulama hemen kapanıyor (crash)

1. **Temiz derleme:** Xcode → **Product → Clean Build Folder**, sonra `cd ios && pod install`, ardından yeniden Run.
2. Pod tarafında **`use_frameworks! :linkage => :static`** kullanılıyor (ML Kit + dinamik framework çakışması riskine karşı). Değişiklikten sonra mutlaka `pod install`.
3. Hâlâ kapanıyorsa Mac’te **Console** uygulaması → solda cihazınızı seçin → uygulama açılırken **Runner** / **hma** ile filtreleyip kırmızı **crash / Termination** satırına bakın; ilk satırdaki **Exception** metnini kaydedin.

## Flutter Web çalıştırma

```bash
cd /Users/hilmibaycan/Projeler-Aktif/hm/mobile
~/flutter/bin/flutter run -d chrome --web-port=60588
```

Tarayıcıdan `hma.cdn.com.tr` çağrıları **CORS** gerektirir; HMA sunucusunda `app/main.py` içindeki `CORSMiddleware` ve gerekirse `HMA_CORS_ORIGINS` / `HMA_CORS_ORIGIN_REGEX` (bkz. `docs/k8s_hma_code_deploy.md`, `docs/hma_mobile_api.md`) production’da doğru olmalıdır.

## `AppInspector` / `WipError -32000`

Hot reload sonrası DevTools gürültüsü; tam yenileme veya yeniden çalıştırma ile kaybolur.

## Web build

```bash
~/flutter/bin/flutter build web \
  --dart-define=HMA_BASE_URL=https://hma.cdn.com.tr \
  --dart-define=HMA_MOBILE_API_KEY=...
```

## iOS Simulator (Xcode)

İlk kurulum:

1. **Flutter iOS artefaktları:** `flutter precache --ios`
2. **CocoaPods:** `brew install cocoapods` (veya `gem install cocoapods`); `pod` PATH’te olsun (`/usr/local/bin` vb.).
3. **Pods:** `cd ios && pod install` (veya doğrudan `flutter run` da tetikler).

Simülatörü açın (**Simulator** uygulaması veya `open -a Simulator`), sonra:

```bash
cd /Users/hilmibaycan/Projeler-Aktif/hm/mobile
export PATH="/Users/hilmibaycan/flutter/bin:/usr/local/bin:$PATH"
flutter devices   # iPhone … (simulator) görünmeli
flutter run -d ios  # veya listedeki cihaz ID’si
```

**Not:** `mobile_scanner` (Google ML Kit) bazı **Apple Silicon + iOS 26** simülatörlerinde arm64 slice eksikliği uyarısı verir. Bu projede simülatör için **`EXCLUDED_ARCHS[sdk=iphonesimulator*]=arm64`** (Rosetta / x86_64 sim) `ios/Podfile` ve `Runner.xcodeproj` ile ayarlıdır; uyarılar görünse de derleme genelde tamamlanır. Gerçek cihazda sorun olmaz.
