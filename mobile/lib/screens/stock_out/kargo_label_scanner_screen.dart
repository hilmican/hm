import 'dart:io';
import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:mobile_scanner/mobile_scanner.dart';

import '../../services/kargo_label_ocr.dart';

/// Barkod/QR okuma ile aynı kamera karesinden etiket OCR (tek açılış).
class KargoScanResult {
  KargoScanResult({required this.barcode, this.ocrText});

  final String barcode;
  final String? ocrText;
}

class KargoLabelScannerScreen extends StatefulWidget {
  const KargoLabelScannerScreen({super.key});

  @override
  State<KargoLabelScannerScreen> createState() =>
      _KargoLabelScannerScreenState();
}

class _KargoLabelScannerScreenState extends State<KargoLabelScannerScreen> {
  late final MobileScannerController _controller;

  /// Sürekli güncellenir; yalnızca "Yakala" ile işlenir (odak/açı kullanıcıda).
  String? _lastBarcode;

  Uint8List? _lastImageBytes;
  DateTime? _lastImageAt;

  bool _captureBusy = false;

  @override
  void initState() {
    super.initState();
    _controller = MobileScannerController(
      /// Aynı kod tekrar tekrar okunabilsin; son kare sürekli güncellensin.
      detectionSpeed: DetectionSpeed.unrestricted,
      returnImage: true,
      /// Android: düşük önizleme (640x480) OCR için çoğu zaman yetersiz; mümkün olan en yakın çözünürlük.
      /// iOS: eklenti şu an bu alanı yok sayabilir; yine de zararsız iletilir.
      cameraResolution: Platform.isAndroid ? const Size(1920, 1080) : null,
    );
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  void _onDetect(BarcodeCapture capture) {
    if (_captureBusy) return;
    final barcodes = capture.barcodes;
    if (barcodes.isEmpty) return;
    final raw = barcodes.first.rawValue;
    if (raw == null || raw.isEmpty) return;

    final now = DateTime.now();
    final img = capture.image;

    bool changed = false;
    if (_lastBarcode != raw) {
      _lastBarcode = raw;
      changed = true;
    }
    if (img != null && img.isNotEmpty) {
      _lastImageBytes = img;
      _lastImageAt = now;
      changed = true;
    }
    if (changed && mounted) setState(() {});
  }

  Future<void> _captureAndFinish() async {
    if (_captureBusy) return;
    final code = _lastBarcode?.trim();
    if (code == null || code.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text(
            'Önce etiketteki barkod veya QR kodun kadrajda görünmesi gerekir.',
          ),
        ),
      );
      return;
    }

    setState(() => _captureBusy = true);

    String? ocrText;
    final img = _lastImageBytes;
    if (img != null && img.isNotEmpty) {
      File? tmp;
      try {
        tmp = File(
          '${Directory.systemTemp.path}/hm_kargo_${DateTime.now().millisecondsSinceEpoch}.jpg',
        );
        await tmp.writeAsBytes(img);
        final t = await recognizeTextFromImagePath(tmp.path);
        final s = t.trim();
        ocrText = s.isEmpty ? null : s;
      } catch (_) {
        ocrText = null;
      } finally {
        try {
          await tmp?.delete();
        } catch (_) {}
      }
    } else {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text(
              'Bu karede görüntü alınamadı (OCR atlanıyor). Flaşı deneyin veya biraz bekleyip tekrar yakalayın.',
            ),
          ),
        );
      }
    }

    if (!mounted) return;
    setState(() => _captureBusy = false);
    Navigator.of(context).pop<KargoScanResult>(
      KargoScanResult(barcode: code, ocrText: ocrText),
    );
  }

  String _hintLine() {
    final b = _lastBarcode;
    if (b == null || b.isEmpty) {
      return 'Kodu kadraja alın; hazır olunca aşağıdan yakalayın.';
    }
    final short = b.length > 36 ? '${b.substring(0, 36)}…' : b;
    final imgAge = _lastImageAt;
    String imgNote = '';
    if (imgAge != null) {
      final sec = DateTime.now().difference(imgAge).inSeconds;
      imgNote = sec <= 2 ? ' · kamera karesi güncel' : ' · son kare ~${sec}s önce';
    }
    return 'Kod okundu: $short$imgNote';
  }

  @override
  Widget build(BuildContext context) {
    final shadowStyle = TextStyle(
      color: Colors.white,
      shadows: [Shadow(blurRadius: 4, color: Colors.black.withValues(alpha: 0.85))],
    );

    return Scaffold(
      appBar: AppBar(title: const Text('Kargo etiketi — QR / barkod + OCR')),
      body: Stack(
        children: [
          MobileScanner(
            controller: _controller,
            onDetect: _onDetect,
          ),
          if (_captureBusy)
            Container(
              color: Colors.black54,
              alignment: Alignment.center,
              child: const Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  CircularProgressIndicator(color: Colors.white),
                  SizedBox(height: 16),
                  Text(
                    'Etiket metni okunuyor…',
                    style: TextStyle(color: Colors.white, fontSize: 16),
                  ),
                ],
              ),
            ),
          Align(
            alignment: Alignment.bottomCenter,
            child: Padding(
              padding: const EdgeInsets.fromLTRB(20, 0, 20, 28),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  Text(
                    _hintLine(),
                    textAlign: TextAlign.center,
                    style: shadowStyle.copyWith(fontSize: 15, height: 1.35),
                  ),
                  const SizedBox(height: 10),
                  Text(
                    'Odak netleşince ve yazılar okunaklı görününce '
                    '«Bu açıda yakala»ya basın — otomatik çekim yok.',
                    textAlign: TextAlign.center,
                    style: shadowStyle.copyWith(fontSize: 13, height: 1.3),
                  ),
                  const SizedBox(height: 16),
                  FilledButton.icon(
                    onPressed: _captureBusy ? null : _captureAndFinish,
                    style: FilledButton.styleFrom(
                      padding: const EdgeInsets.symmetric(vertical: 16),
                    ),
                    icon: const Icon(Icons.center_focus_strong),
                    label: const Text('Bu açıda yakala (OCR bu kareyle)'),
                  ),
                  const SizedBox(height: 12),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      IconButton(
                        onPressed: () => _controller.toggleTorch(),
                        icon: const Icon(Icons.flash_on, color: Colors.white, size: 32),
                        style: IconButton.styleFrom(backgroundColor: Colors.black54),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}
