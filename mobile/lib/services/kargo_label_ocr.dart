import 'dart:io';
import 'dart:math' as math;

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

double _rowTopKey(List<TextLine> row) =>
    row.map((l) => l.boundingBox.top).reduce(math.min);

/// ML Kit’in düz [text] birleşimi, iki sütunlu (Gönderen | Alıcı) etikette sırayı bozabiliyor.
/// Tüm satırları geometrik olarak yatay bant + soldan sağa sıralayıp yeniden üretir.
String _mlKitReadingOrderText(RecognizedText recognized) {
  final lines = <TextLine>[];
  for (final block in recognized.blocks) {
    lines.addAll(block.lines);
  }
  if (lines.isEmpty) return recognized.text;
  if (lines.length == 1) return lines.first.text.trim();

  final heights = lines.map((l) => l.boundingBox.height).toList()..sort();
  final medH = heights[heights.length ~/ 2];
  final tol = math.max(medH * 0.35, 8.0);

  final rows = <List<TextLine>>[];
  final byTop = [...lines]..sort((a, b) => a.boundingBox.top.compareTo(b.boundingBox.top));
  for (final line in byTop) {
    final cy = line.boundingBox.center.dy;
    var placed = false;
    for (final row in rows) {
      final rowCy =
          row.map((l) => l.boundingBox.center.dy).reduce((a, b) => a + b) / row.length;
      if ((cy - rowCy).abs() <= tol) {
        row.add(line);
        placed = true;
        break;
      }
    }
    if (!placed) rows.add([line]);
  }
  for (final row in rows) {
    row.sort((a, b) => a.boundingBox.left.compareTo(b.boundingBox.left));
  }
  rows.sort((a, b) => _rowTopKey(a).compareTo(_rowTopKey(b)));

  final buf = StringBuffer();
  for (var i = 0; i < rows.length; i++) {
    if (i > 0) buf.writeln();
    final parts = rows[i]
        .map((l) => l.text.trim())
        .where((t) => t.isNotEmpty)
        .toList();
    buf.write(parts.join(' '));
  }
  return buf.toString();
}

Future<String> _mlKitLatin(String path) async {
  final input = InputImage.fromFilePath(path);
  final recognizer = TextRecognizer(script: TextRecognitionScript.latin);
  try {
    final recognized = await recognizer.processImage(input);
    final ordered = _mlKitReadingOrderText(recognized).trim();
    final plain = recognized.text.trim();
    final hasAlici =
        RegExp(r'alıcı|alici', caseSensitive: false).hasMatch(ordered);
    if (ordered.length >= plain.length * 0.85 || hasAlici) {
      return ordered;
    }
    return plain.isNotEmpty ? plain : ordered;
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
    final mlScore = kargoLabelOcrScore(mlRaw);
    final tessScore =
        tessRaw != null && tessRaw.length >= 8 ? kargoLabelOcrScore(tessRaw) : -1;
    if (tessRaw != null &&
        tessRaw.length >= 8 &&
        (tessScore > mlScore || mlRaw.length < 12)) {
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
