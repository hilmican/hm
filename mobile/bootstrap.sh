#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if ! command -v flutter >/dev/null 2>&1; then
  echo "Flutter bulunamadı. https://docs.flutter.dev/get-started/install"
  exit 1
fi
if [[ ! -d android ]] || [[ ! -d ios ]]; then
  flutter create . --project-name hma_stock --org com.himan
fi
flutter pub get
echo "Tamam. Çalıştırma: flutter run"
