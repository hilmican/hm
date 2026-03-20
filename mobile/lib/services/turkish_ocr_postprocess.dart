import 'dart:math' as math;

/// ML Kit / ham OCR sonrası Türkçe etiket metnine yönelik güvenli düzeltmeler.
String postProcessTurkishOcr(String raw) {
  var s = raw.replaceAll('\r\n', '\n').trim();
  if (s.isEmpty) return s;

  // Çoklu boşluk / satır
  s = s.replaceAll(RegExp(r'[ \t]{2,}'), ' ');
  s = s.split('\n').map((l) => l.trim()).where((l) => l.isNotEmpty).join('\n');

  // Sık görülen şehir/ilçe OCR hataları (kargo etiketi bağlamı)
  final wordFixes = <RegExp, String>{
    RegExp(r'Marmaraeresi', caseSensitive: false): 'Marmaraereğlisi',
    RegExp(r'Marmaraeregi', caseSensitive: false): 'Marmaraereğlisi',
    RegExp(r'Marmaraere[gğ]lisi', caseSensitive: false): 'Marmaraereğlisi',
    RegExp(r'Tekirdai\b', caseSensitive: false): 'Tekirdağ',
    RegExp(r'Tekirda[iı]\b', caseSensitive: false): 'Tekirdağ',
    RegExp(r'\bCED[Iİ]T\b'): 'CEDİT',
    RegExp(r'\bPASA\b', caseSensitive: false): 'PAŞA',
    RegExp(r'\bCAYIR\b', caseSensitive: false): 'ÇAYIR',
    RegExp(r'\bSOK\.\b'): 'SOK.',
    RegExp(r'İ\.K\.NO:', caseSensitive: false): 'İ.K.NO:',
    RegExp(r'I\.K\.NO:', caseSensitive: false): 'İ.K.NO:',
    RegExp(r'\bALI\b(?=\s+PAŞA)', caseSensitive: false): 'ALİ',
    RegExp(r'\bKUMAS\b', caseSensitive: false): 'KUMAŞ',
    RegExp(r'\bDUBLE\b', caseSensitive: false): 'DUBLE', // genelde doğru
    RegExp(r'\bS[Iİ]YAH\b', caseSensitive: false): 'SİYAH',
    RegExp(r'M-?\s*S[Iİ]YAH', caseSensitive: false): 'M-SİYAH',
  };
  for (final e in wordFixes.entries) {
    s = s.replaceAll(e.key, e.value);
  }

  // "MSSYAB" gibi birleşik hatalar: SİYAH ipucu
  s = s.replaceAll(RegExp(r'\bM\s*SSYAB\b', caseSensitive: true), 'M-SİYAH');
  s = s.replaceAll(RegExp(r'\bMSSYAB\b', caseSensitive: true), 'M-SİYAH');

  return s;
}

/// Türkçe karakter içeriğine göre skor (hangi motor çıktısı daha uygun).
int turkishOcrScore(String s) {
  if (s.isEmpty) return 0;
  const letters = 'ğüşıöçĞÜŞİÖÇıİ';
  var sc = math.min(s.length, 8000);
  for (final r in s.runes) {
    final c = String.fromCharCode(r);
    if (letters.contains(c)) sc += 4;
  }
  // Kelime ipuçları
  final low = s.toLowerCase();
  if (low.contains('mah') || low.contains('mah.')) sc += 8;
  if (low.contains('tahsilat')) sc += 10;
  if (low.contains('adres')) sc += 6;
  if (low.contains('içerik') || low.contains('icerik')) sc += 6;
  return sc;
}
