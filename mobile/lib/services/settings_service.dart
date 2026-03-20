import 'package:shared_preferences/shared_preferences.dart';

import '../config/api_config.dart';

/// Runtime overrides for API URL and mobile key (persisted).
class SettingsService {
  SettingsService._();
  static final SettingsService instance = SettingsService._();

  String _baseUrl = ApiConfig.baseUrl;
  String _mobileApiKey = ApiConfig.mobileApiKey;
  bool _stockDryRun = false;

  String get baseUrl => _baseUrl;
  String get mobileApiKey => _mobileApiKey;
  /// Stok girişinde API'ye dry_run gönderir; sunucu stok yazmaz.
  bool get stockDryRun => _stockDryRun;

  Future<void> load() async {
    final p = await SharedPreferences.getInstance();
    final b = p.getString(_kBaseUrl);
    final k = p.getString(_kMobileKey);
    if (b != null && b.trim().isNotEmpty) {
      _baseUrl = b.trim().replaceAll(RegExp(r'/$'), '');
    }
    if (k != null) {
      _mobileApiKey = k;
    }
    _stockDryRun = p.getBool(_kStockDryRun) ?? false;
  }

  Future<void> save({required String baseUrl, required String mobileApiKey}) async {
    final p = await SharedPreferences.getInstance();
    final b = baseUrl.trim().replaceAll(RegExp(r'/$'), '');
    await p.setString(_kBaseUrl, b);
    await p.setString(_kMobileKey, mobileApiKey.trim());
    _baseUrl = b.isEmpty ? ApiConfig.baseUrl : b;
    _mobileApiKey = mobileApiKey.trim();
  }

  Future<void> setStockDryRun(bool value) async {
    final p = await SharedPreferences.getInstance();
    await p.setBool(_kStockDryRun, value);
    _stockDryRun = value;
  }

  static const _kBaseUrl = 'hma_base_url';
  static const _kMobileKey = 'hma_mobile_api_key';
  static const _kStockDryRun = 'hma_stock_dry_run';
}
