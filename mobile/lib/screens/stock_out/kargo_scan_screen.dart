import 'package:flutter/material.dart';
import 'package:image_picker/image_picker.dart';

import '../../services/api_client.dart';
import '../../services/kargo_label_ocr.dart';
import '../../widgets/qr_scanner_widget.dart';
import 'cart_scan_screen.dart';

class KargoScanScreen extends StatefulWidget {
  const KargoScanScreen({super.key});

  @override
  State<KargoScanScreen> createState() => _KargoScanScreenState();
}

class _KargoScanScreenState extends State<KargoScanScreen> {
  final _api = HmaApiClient();
  final _manualCtrl = TextEditingController();
  bool _busy = false;

  Future<String?> _optionalOcrText() async {
    if (!mounted) return null;
    final choice = await showDialog<String>(
      context: context,
      barrierDismissible: false,
      builder: (ctx) => AlertDialog(
        title: const Text('Etiket OCR'),
        content: const Text(
          'Alıcı, telefon, adres, içerik ve tahsilat bilgisi için etiketin '
          'tamamının fotoğrafını çekebilir veya galeriden seçebilirsiniz. '
          'Atlayıp yalnızca okuttuğunuz kod ile de devam edebilirsiniz.',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, 'skip'),
            child: const Text('Atla'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(ctx, 'gallery'),
            child: const Text('Galeri'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(ctx, 'camera'),
            child: const Text('Kamera'),
          ),
        ],
      ),
    );
    if (!mounted || choice == null || choice == 'skip') return null;

    final picker = ImagePicker();
    final file = await picker.pickImage(
      source: choice == 'camera' ? ImageSource.camera : ImageSource.gallery,
      imageQuality: 88,
      maxWidth: 2000,
    );
    if (file == null) return null;
    final text = await recognizeTextFromImagePath(file.path);
    final t = text.trim();
    return t.isEmpty ? null : t;
  }

  Future<void> _startOrder(String raw) async {
    final trimmed = raw.trim();
    if (trimmed.isEmpty) return;
    setState(() => _busy = true);
    try {
      final ocrText = await _optionalOcrText();
      if (!mounted) return;

      final res = await _api.orderFromKargoQr(
        qrContent: trimmed,
        ocrText: ocrText,
      );
      final orderId = res['order_id'] as int?;
      if (orderId == null) throw Exception('order_id yok');
      if (!mounted) return;

      final initialCount =
          (res['order_item_count'] as num?)?.toInt() ?? 0;
      final rawLines = res['lines'] as List<dynamic>? ?? [];
      final initialLines = rawLines
          .map((e) => Map<String, dynamic>.from(e as Map))
          .toList();
      final preNum = res['prefill_total_amount'];
      final prefillTotal =
          preNum is num ? preNum.toDouble() : null;
      final prefillNotes = res['prefill_notes'] as String?;
      Map<String, dynamic>? labelFields;
      final lf = res['label_fields'];
      if (lf is Map) {
        labelFields = Map<String, dynamic>.from(lf);
      }

      Navigator.of(context).pushReplacement(
        MaterialPageRoute<void>(
          builder: (_) => CartScanScreen(
            orderId: orderId,
            trackingNo: res['tracking_no'] as String? ?? '',
            resumed: res['resumed'] == true,
            initialLineUnits: initialCount,
            initialLines: initialLines,
            prefillTotalAmount: prefillTotal,
            prefillNotes: prefillNotes,
            labelFields: labelFields,
          ),
        ),
      );
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Hata: $e')),
        );
        setState(() => _busy = false);
      }
    }
  }

  @override
  void dispose() {
    _manualCtrl.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Kargo etiketi')),
      body: _busy
          ? const Center(child: CircularProgressIndicator())
          : Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  const Text(
                    'Gönderi etiketindeki QR veya 1D barkod (ör. Code 128) okutun. '
                    'Takip no / müşteri bilgisi etikette veya ham metin olarak aşağıya yapıştırın.',
                  ),
                  const SizedBox(height: 16),
                  FilledButton.icon(
                    onPressed: () async {
                      if (!context.mounted) return;
                      final code = await Navigator.of(context).push<String>(
                        MaterialPageRoute(
                          builder: (_) => const QrScannerWidget(
                            title: 'Kargo etiketi — QR / barkod',
                          ),
                        ),
                      );
                      if (code != null && code.isNotEmpty) _startOrder(code);
                    },
                    icon: const Icon(Icons.qr_code_scanner),
                    label: const Text('QR / barkod okut'),
                  ),
                  const SizedBox(height: 24),
                  const Text('Veya ham metin / takip no'),
                  TextField(
                    controller: _manualCtrl,
                    maxLines: 4,
                    decoration: const InputDecoration(
                      border: OutlineInputBorder(),
                      hintText:
                          'JSON (takip, fiyat, açıklama), URL veya takip|ad|tel|...',
                    ),
                  ),
                  const SizedBox(height: 12),
                  OutlinedButton(
                    onPressed: () => _startOrder(_manualCtrl.text),
                    child: const Text('Gönder'),
                  ),
                ],
              ),
            ),
    );
  }
}
