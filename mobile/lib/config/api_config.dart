/// Base URL for HMA (no trailing slash).
///
/// Varsayılan: `https://hma.cdn.com.tr`
///
/// Override: `--dart-define=HMA_BASE_URL=https://example.com`
class ApiConfig {
  static const String productionBaseUrl = 'https://hma.cdn.com.tr';

  static String get baseUrl {
    const fromEnv = String.fromEnvironment('HMA_BASE_URL', defaultValue: '');
    final trimmed = fromEnv.trim();
    if (trimmed.isNotEmpty) {
      return trimmed.replaceAll(RegExp(r'/$'), '');
    }
    return productionBaseUrl;
  }

  /// Set via `--dart-define=HMA_MOBILE_API_KEY=secret`
  static const String mobileApiKey = String.fromEnvironment(
    'HMA_MOBILE_API_KEY',
    defaultValue: '',
  );
}
