import 'dart:convert';

/// Çevrimdışı etiket özeti için QR/ham metinden takip numarası tahmini.
String? kargoTrackingHintFromQr(String raw) {
  final q = raw.trim();
  if (q.isEmpty) return null;
  final low = q.toLowerCase();
  final barkod =
      RegExp(r'[?&]barkod=([0-9]{8,20})', caseSensitive: false).firstMatch(low);
  if (barkod != null) return barkod.group(1);
  final bare = RegExp(r'(?:^|[/?&])barkod[=:]([0-9]{8,20})', caseSensitive: false)
      .firstMatch(low);
  if (bare != null) return bare.group(1);
  if (low.startsWith('{') && low.endsWith('}')) {
    try {
      final o = jsonDecode(q);
      if (o is Map) {
        for (final k in ['takip_no', 'takip', 'tracking_no', 'barkod']) {
          final v = o[k];
          if (v != null) {
            final s = v.toString().trim();
            if (RegExp(r'^\d{8,20}$').hasMatch(s)) return s;
          }
        }
      }
    } catch (_) {}
  }
  final m = RegExp(r'\b(\d{12,16})\b').firstMatch(q);
  if (m != null) return m.group(1);
  return null;
}
