import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter_tesseract_ocr/flutter_tesseract_ocr.dart';
import 'package:google_mlkit_text_recognition/google_mlkit_text_recognition.dart';
import 'package:image/image.dart' as im;

import 'turkish_ocr_postprocess.dart';

Future<String> _preprocessToTempJpeg(String inputPath) async {
  try {
    final bytes = await File(inputPath).readAsBytes();
    var image = im.decodeImage(bytes);
    if (image == null) return inputPath;

    const minW = 1650;
    if (image.width < minW) {
      final scale = minW / image.width;
      image = im.copyResize(
        image,
        width: minW,
        height: (image.height * scale).round(),
        interpolation: im.Interpolation.linear,
      );
    }

    image = im.grayscale(image);
    image = im.adjustColor(image, contrast: 1.14, brightness: 1.03);

    final outPath =
        '${inputPath}_hma_pre_${DateTime.now().millisecondsSinceEpoch}.jpg';
    await File(outPath).writeAsBytes(im.encodeJpg(image, quality: 92));
    return outPath;
  } catch (_) {
    return inputPath;
  }
}

Future<String> _mlKitLatin(String path) async {
  final input = InputImage.fromFilePath(path);
  final recognizer = TextRecognizer(script: TextRecognitionScript.latin);
  try {
    final recognized = await recognizer.processImage(input);
    return recognized.text;
  } finally {
    await recognizer.close();
  }
}

Future<String?> _tesseractTurEng(String path) async {
  if (kIsWeb) return null;
  try {
    final t = await FlutterTesseractOcr.extractText(
      path,
      language: 'tur+eng',
      args: {
        'preserve_interword_spaces': '1',
        'psm': '6',
      },
    );
    final s = t.trim();
    return s.isEmpty ? null : s;
  } catch (_) {
    return null;
  }
}

/// Kontrast + büyütme ön-işleme; Android/iOS'ta Tesseract `tur+eng` + ML Kit birleşimi;
/// iOS'ta `Runner/tessdata` Xcode klasör referansı + derleme senkronu gerekir. Çıktı [postProcessTurkishOcr].
Future<String> recognizeTextFromImagePath(String path) async {
  final prePath = await _preprocessToTempJpeg(path);
  final cleanup = prePath != path;

  try {
    final mlRaw = (await _mlKitLatin(prePath)).trim();

    String? tessRaw;
    if (!kIsWeb && (Platform.isAndroid || Platform.isIOS)) {
      tessRaw = await _tesseractTurEng(prePath);
    }

    String best;
    if (tessRaw != null &&
        tessRaw.length >= 8 &&
        (turkishOcrScore(tessRaw) > turkishOcrScore(mlRaw) || mlRaw.length < 12)) {
      best = tessRaw;
    } else {
      best = mlRaw;
    }

    if (best.isEmpty && tessRaw != null) {
      best = tessRaw;
    }

    return postProcessTurkishOcr(best);
  } finally {
    if (cleanup) {
      try {
        await File(prePath).delete();
      } catch (_) {}
    }
  }
}
