import 'dart:io';

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
  bool _handled = false;
  bool _ocrBusy = false;

  @override
  void initState() {
    super.initState();
    _controller = MobileScannerController(
      detectionSpeed: DetectionSpeed.normal,
      returnImage: true,
    );
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  Future<void> _onDetect(BarcodeCapture capture) async {
    if (_handled) return;
    final barcodes = capture.barcodes;
    if (barcodes.isEmpty) return;
    final raw = barcodes.first.rawValue;
    if (raw == null || raw.isEmpty) return;

    _handled = true;
    if (mounted) setState(() => _ocrBusy = true);

    String? ocrText;
    final img = capture.image;
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
    }

    if (!mounted) return;
    Navigator.of(context).pop<KargoScanResult>(
      KargoScanResult(barcode: raw, ocrText: ocrText),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Kargo etiketi — QR / barkod + OCR')),
      body: Stack(
        children: [
          MobileScanner(
            controller: _controller,
            onDetect: (c) {
              _onDetect(c);
            },
          ),
          if (_ocrBusy)
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
              padding: const EdgeInsets.all(24),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Text(
                    'Etiketi kadraja alın; kod okununca aynı kareden OCR da alınır.',
                    textAlign: TextAlign.center,
                    style: TextStyle(
                      color: Colors.white,
                      shadows: [Shadow(blurRadius: 4, color: Colors.black)],
                    ),
                  ),
                  const SizedBox(height: 12),
                  IconButton(
                    onPressed: () => _controller.toggleTorch(),
                    icon: const Icon(Icons.flash_on, color: Colors.white, size: 36),
                    style: IconButton.styleFrom(backgroundColor: Colors.black54),
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
