import 'package:flutter/material.dart';

import '../config/api_config.dart';
import '../services/settings_service.dart';

class SettingsScreen extends StatefulWidget {
  const SettingsScreen({super.key});

  @override
  State<SettingsScreen> createState() => _SettingsScreenState();
}

class _SettingsScreenState extends State<SettingsScreen> {
  final _baseCtrl = TextEditingController();
  final _keyCtrl = TextEditingController();
  bool _saving = false;
  bool _stockDryRun = false;

  @override
  void initState() {
    super.initState();
    final s = SettingsService.instance;
    _baseCtrl.text = s.baseUrl;
    _keyCtrl.text = s.mobileApiKey;
    _stockDryRun = s.stockDryRun;
  }

  @override
  void dispose() {
    _baseCtrl.dispose();
    _keyCtrl.dispose();
    super.dispose();
  }

  Future<void> _save() async {
    setState(() => _saving = true);
    try {
      await SettingsService.instance.save(
        baseUrl: _baseCtrl.text,
        mobileApiKey: _keyCtrl.text,
      );
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Kaydedildi')),
      );
    } finally {
      if (mounted) setState(() => _saving = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('API ayarları')),
      body: _saving
          ? const Center(child: CircularProgressIndicator())
          : ListView(
              padding: const EdgeInsets.all(16),
              children: [
                const Text(
                  'Sunucu adresi ve isteğe bağlı X-Mobile-API-Key. '
                  'Boş anahtar: derleme zamanı --dart-define kullanılıyorsa o geçerlidir.',
                ),
                const SizedBox(height: 16),
                TextField(
                  controller: _baseCtrl,
                  decoration: const InputDecoration(
                    labelText: 'Base URL',
                    border: OutlineInputBorder(),
                    hintText: ApiConfig.productionBaseUrl,
                  ),
                  keyboardType: TextInputType.url,
                  autocorrect: false,
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: _keyCtrl,
                  decoration: const InputDecoration(
                    labelText: 'Mobile API key (opsiyonel)',
                    border: OutlineInputBorder(),
                  ),
                  obscureText: true,
                  autocorrect: false,
                ),
                const SizedBox(height: 24),
                SwitchListTile(
                  title: const Text('Stok girişi: test modu (dry-run)'),
                  subtitle: const Text(
                    'Açıkken stoğa yazılmaz; yalnızca doğrulama ve QR önizlemesi. '
                    'Gerçek giriş öncesi kapatın.',
                  ),
                  value: _stockDryRun,
                  onChanged: (v) async {
                    setState(() => _stockDryRun = v);
                    await SettingsService.instance.setStockDryRun(v);
                  },
                ),
                const SizedBox(height: 24),
                FilledButton(
                  onPressed: _save,
                  child: const Text('Kaydet'),
                ),
              ],
            ),
    );
  }
}
