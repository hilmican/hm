import 'dart:io';
import 'dart:math' as math;

import 'package:flutter/foundation.dart';
import 'package:flutter_tesseract_ocr/flutter_tesseract_ocr.dart';
import 'package:google_mlkit_text_recognition/google_mlkit_text_recognition.dart';
import 'package:image/image.dart' as im;

import 'turkish_ocr_postprocess.dart';

/// `--dart-define=HMA_OCR_RECIPIENT_CROP=true` ile sağ/sütun bölgesine ek OCR geçişi.
const bool kKargoOcrRecipientCrop =
    bool.fromEnvironment('HMA_OCR_RECIPIENT_CROP', defaultValue: false);

Future<void> _debugOcrLog(
  String phase, {
  String? mlPreview,
  String? tessPreview,
  String? mergedPreview,
  int? preW,
  int? preH,
  String? variant,
}) async {
  if (!kDebugMode) return;
  final v = variant != null ? ' [$variant]' : '';
  debugPrint('[kargo_ocr]$v $phase ${preW != null && preH != null ? '${preW}x$preH ' : ''}');
  if (mlPreview != null && mlPreview.isNotEmpty) {
    final p = mlPreview.length > 280 ? '${mlPreview.substring(0, 280)}…' : mlPreview;
    debugPrint('[kargo_ocr] ml len=${mlPreview.length} :: $p');
  }
  if (tessPreview != null && tessPreview.isNotEmpty) {
    final p = tessPreview.length > 280 ? '${tessPreview.substring(0, 280)}…' : tessPreview;
    debugPrint('[kargo_ocr] tess len=${tessPreview.length} :: $p');
  }
  if (mergedPreview != null && mergedPreview.isNotEmpty) {
    final p =
        mergedPreview.length > 320 ? '${mergedPreview.substring(0, 320)}…' : mergedPreview;
    debugPrint('[kargo_ocr] merged len=${mergedPreview.length} :: $p');
  }
}

/// Ana profil: mevcut davranış. Alternatif: daha geniş + keskinleştirme + normalize.
Future<List<String>> _preprocessVariants(String inputPath) async {
  final cleanup = <String>[];
  try {
    final bytes = await File(inputPath).readAsBytes();
    var image = im.decodeImage(bytes);
    if (image == null) return [inputPath];

    Future<String> writeProfile(im.Image img, String tag) async {
      final outPath =
          '${inputPath}_hma_${tag}_${DateTime.now().millisecondsSinceEpoch}.jpg';
      await File(outPath).writeAsBytes(im.encodeJpg(img, quality: 92));
      return outPath;
    }

    String standardPath;
    {
      var img = im.Image.from(image);
      img = _resizeMinWidth(img, 1650);
      img = im.grayscale(img);
      img = im.adjustColor(img, contrast: 1.14, brightness: 1.03);
      standardPath = await writeProfile(img, 'std');
    }

    String sharpPath;
    {
      var img = im.Image.from(image);
      img = _resizeMinWidth(img, 2400);
      img = im.grayscale(img);
      img = im.normalize(img, min: 0, max: 255);
      img = im.convolution(img,
          filter: [0, -1, 0, -1, 5, -1, 0, -1, 0],
          div: 1,
          amount: 0.65);
      img = im.adjustColor(img, contrast: 1.16, brightness: 1.02);
      sharpPath = await writeProfile(img, 'sharp');
    }

    cleanup.addAll([standardPath, sharpPath]);
    return cleanup;
  } catch (_) {
    for (final p in cleanup) {
      try {
        await File(p).delete();
      } catch (_) {}
    }
    return [inputPath];
  }
}

im.Image _resizeMinWidth(im.Image image, int minW) {
  if (image.width >= minW) return image;
  final scale = minW / image.width;
  return im.copyResize(
    image,
    width: minW,
    height: (image.height * scale).round(),
    interpolation: im.Interpolation.linear,
  );
}

/// FOCUS / Sürat: gönderen–alıcı bandı tahmini (kadrajda etiket ortalandı varsayımı).
Future<String?> _preprocessRecipientCrop(String inputPath) async {
  try {
    final bytes = await File(inputPath).readAsBytes();
    var image = im.decodeImage(bytes);
    if (image == null) return null;
    final w = image.width;
    final h = image.height;
    final left = (w * 0.44).clamp(0, w - 2).toInt();
    final top = (h * 0.05).clamp(0, h - 2).toInt();
    var cw = (w * 0.56).round();
    var ch = (h * 0.42).round();
    if (left + cw > w) cw = w - left;
    if (top + ch > h) ch = h - top;
    if (cw < 80 || ch < 40) return null;

    var crop = im.copyCrop(image, x: left, y: top, width: cw, height: ch);
    crop = _resizeMinWidth(crop, 2000);
    crop = im.grayscale(crop);
    crop = im.adjustColor(crop, contrast: 1.18, brightness: 1.04);
    final outPath =
        '${inputPath}_hma_rcrop_${DateTime.now().millisecondsSinceEpoch}.jpg';
    await File(outPath).writeAsBytes(im.encodeJpg(crop, quality: 93));
    return outPath;
  } catch (_) {
    return null;
  }
}

double _rowTopKey(List<TextLine> row) =>
    row.map((l) => l.boundingBox.top).reduce(math.min);

/// ML Kit satırlarını geometrik okuma sırasına göre birleştirir.
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

Future<String> _mlKitLatin(String path, {String variantTag = ''}) async {
  final input = InputImage.fromFilePath(path);
  final recognizer = TextRecognizer(script: TextRecognitionScript.latin);
  try {
    final recognized = await recognizer.processImage(input);
    final ordered = _mlKitReadingOrderText(recognized).trim();
    final plain = recognized.text.trim();
    final hasAlici =
        RegExp(r'alıcı|alici', caseSensitive: false).hasMatch(ordered);
    final useOrdered =
        ordered.length >= plain.length * 0.85 || hasAlici;
    final out = useOrdered ? ordered : (plain.isNotEmpty ? plain : ordered);

    int? preW;
    int? preH;
    try {
      final b = await File(path).readAsBytes();
      final dec = im.decodeImage(b);
      if (dec != null) {
        preW = dec.width;
        preH = dec.height;
      }
    } catch (_) {}

    await _debugOcrLog('ml_kit',
        mlPreview: out,
        preW: preW,
        preH: preH,
        variant: variantTag);
    return out;
  } finally {
    await recognizer.close();
  }
}

Future<String?> _tesseractWithPsm(String path, String psm) async {
  if (kIsWeb) return null;
  try {
    final t = await FlutterTesseractOcr.extractText(
      path,
      language: 'tur+eng',
      args: {
        'preserve_interword_spaces': '1',
        'psm': psm,
      },
    );
    final s = t.trim();
    return s.isEmpty ? null : s;
  } catch (_) {
    return null;
  }
}

/// PSM 6, 11 ve 3 için en yüksek [kargoLabelOcrScore] çıktısını seçer.
Future<String?> _tesseractBestMultiPsm(String path, {String variantTag = ''}) async {
  if (kIsWeb) return null;
  const psms = ['6', '11', '3'];
  String? best;
  var bestScore = -1;
  for (final psm in psms) {
    final t = await _tesseractWithPsm(path, psm);
    if (t == null) continue;
    final sc = kargoLabelOcrScore(t);
    if (sc > bestScore) {
      bestScore = sc;
      best = t;
    }
  }
  await _debugOcrLog('tesseract_best',
      tessPreview: best ?? '',
      variant: variantTag.isEmpty ? null : '${variantTag}_psm_best');
  return best;
}

String _normLineKey(String s) =>
    s.toLowerCase().replaceAll(RegExp(r'\s+'), ' ').trim();

bool _lineHasKargoHints(String t) {
  if (t.length < 3) return false;
  final l = t.toLowerCase();
  return RegExp(r'alıcı|alici', caseSensitive: false).hasMatch(t) ||
      RegExp(r'\+?\s*90\s*5\d|905\d{9}|\+90\s*\d{10}').hasMatch(t) ||
      l.contains('adres') ||
      l.contains('gönderen') ||
      l.contains('gonderen') ||
      l.contains('tahsilat') ||
      l.contains('içerik') ||
      l.contains('icerik') ||
      l.contains('mah.') ||
      l.contains('mah ');
}

bool _normContainsLine(String haystackNorm, String lineNorm) {
  if (lineNorm.length < 6) return haystackNorm.contains(lineNorm);
  return haystackNorm.contains(lineNorm);
}

/// [base] üzerine, [extra] içindeki “faydalı” ve birikmiş metinde olmayan satırları ekler.
String supplementMissingLines(String base, String extra) {
  final out = StringBuffer(base.trimRight());
  var accNorm = _normLineKey(out.toString());

  for (final raw in extra.split('\n')) {
    final line = raw.trim();
    if (line.isEmpty) continue;
    if (!_lineHasKargoHints(line)) continue;
    final nk = _normLineKey(line);
    if (nk.length < 4) continue;
    if (_normContainsLine(accNorm, nk)) continue;

    if (out.isNotEmpty) out.writeln();
    out.write(line);
    accNorm = _normLineKey(out.toString());
  }
  return out.toString();
}

/// ML + Tesseract: iki taraftan faydalı satırları mümkün olduğunca birleştirilir.
String mergeEngineOutputs(String ml, String? tess) {
  if (tess == null || tess.length < 8) return ml;
  final scMl = kargoLabelOcrScore(ml);
  final scT = kargoLabelOcrScore(tess);
  var merged =
      scMl >= scT ? supplementMissingLines(ml, tess) : supplementMissingLines(tess, ml);
  merged = supplementMissingLines(merged, ml);
  merged = supplementMissingLines(merged, tess);
  return merged;
}

Future<String> _recognizePreprocessedPath(String path, {required String variantTag}) async {
  final mlRaw = (await _mlKitLatin(path, variantTag: variantTag)).trim();
  String? tessRaw;
  if (!kIsWeb && (Platform.isAndroid || Platform.isIOS)) {
    tessRaw = await _tesseractBestMultiPsm(path, variantTag: variantTag);
  }
  final merged = mergeEngineOutputs(mlRaw, tessRaw);
  await _debugOcrLog('variant_done', mergedPreview: merged, variant: variantTag);
  return merged;
}

/// Kontrast + büyütme; çoklu ön-işlem + ML Kit + Tesseract çoklu PSM + satır birleştirme.
/// `--dart-define=HMA_OCR_RECIPIENT_CROP=true` ile sağ bant OCR ekleri.
Future<String> recognizeTextFromImagePath(String path) async {
  final variantPaths = await _preprocessVariants(path);
  final cleanup = <String>{...variantPaths};
  String? cropPath;

  try {
    if (kKargoOcrRecipientCrop && !kIsWeb && (Platform.isAndroid || Platform.isIOS)) {
      cropPath = await _preprocessRecipientCrop(path);
      if (cropPath != null) cleanup.add(cropPath);
    }

    String? bestRaw;
    var bestScore = -1;

    for (var i = 0; i < variantPaths.length; i++) {
      final vp = variantPaths[i];
      final tag = i == 0 ? 'std' : 'sharp';
      int? w;
      int? h;
      try {
        final b = await File(vp).readAsBytes();
        final dec = im.decodeImage(b);
        w = dec?.width;
        h = dec?.height;
      } catch (_) {}

      final text = await _recognizePreprocessedPath(vp, variantTag: tag);
      final sc = kargoLabelOcrScore(text);
      if (sc > bestScore) {
        bestScore = sc;
        bestRaw = text;
      }
      await _debugOcrLog('variant_pick', mergedPreview: text, preW: w, preH: h, variant: tag);
    }

    if (cropPath != null) {
      final cropText = await _recognizePreprocessedPath(cropPath, variantTag: 'rcrop');
      if (bestRaw != null) {
        bestRaw = supplementMissingLines(bestRaw, cropText);
      } else {
        bestRaw = cropText;
      }
      await _debugOcrLog('after_crop', mergedPreview: bestRaw, variant: 'rcrop_merge');
    }

    var out = (bestRaw ?? '').trim();
    if (out.isEmpty && variantPaths.isNotEmpty) {
      out = await _recognizePreprocessedPath(variantPaths.first, variantTag: 'fallback');
    }

    final post = postProcessTurkishOcr(out);
    await _debugOcrLog('final_post', mergedPreview: post);
    return post;
  } finally {
    for (final p in cleanup) {
      if (p != path) {
        try {
          await File(p).delete();
        } catch (_) {}
      }
    }
  }
}
