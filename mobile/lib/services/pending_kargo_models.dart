import 'dart:convert';

class DesiredCartLine {
  DesiredCartLine({
    required this.lineId,
    required this.qrContent,
    required this.quantity,
  });

  final String lineId;
  final String qrContent;
  int quantity;

  Map<String, dynamic> toJson() => {
        'line_id': lineId,
        'qr_content': qrContent,
        'quantity': quantity,
      };

  static DesiredCartLine fromJson(Map<String, dynamic> m) {
    return DesiredCartLine(
      lineId: m['line_id'] as String? ?? '',
      qrContent: (m['qr_content'] as String? ?? '').trim(),
      quantity: (m['quantity'] as num?)?.toInt() ?? 1,
    );
  }
}

class PendingKargoSale {
  PendingKargoSale({
    required this.id,
    required this.createdAt,
    required this.updatedAt,
    required this.syncState,
    this.lastError,
    this.lastAttemptAt,
    this.serverOrderId,
    required this.phase,
    required this.createPayload,
    required this.desiredCart,
    this.completePayload,
    required this.finalized,
    this.ocrTextSnapshot,
    this.trackingHint,
  });

  final String id;
  final DateTime createdAt;
  final DateTime updatedAt;
  final String syncState;
  final String? lastError;
  final DateTime? lastAttemptAt;
  final int? serverOrderId;
  /// none → created → cart_applied; completed = kayıt silinir
  final String phase;
  final Map<String, dynamic> createPayload;
  final List<DesiredCartLine> desiredCart;
  final Map<String, dynamic>? completePayload;
  final bool finalized;
  final String? ocrTextSnapshot;
  final String? trackingHint;

  int get itemUnitCount =>
      desiredCart.fold<int>(0, (s, e) => s + e.quantity);

  Map<String, dynamic> labelFieldsForOfflineCard() {
    final t = trackingHint;
    final o = ocrTextSnapshot;
    return {
      if (t != null && t.isNotEmpty) 'tracking_no': t,
      if (o != null && o.isNotEmpty) 'content': o.length > 280 ? '${o.substring(0, 280)}…' : o,
    };
  }

  static PendingKargoSale fromRow(Map<String, Object?> row) {
    List<DesiredCartLine> cart = [];
    final cartRaw = row['desired_cart'] as String?;
    if (cartRaw != null && cartRaw.isNotEmpty) {
      try {
        final decoded = jsonDecode(cartRaw) as List<dynamic>;
        cart = decoded
            .map((e) => DesiredCartLine.fromJson(Map<String, dynamic>.from(e as Map)))
            .toList();
      } catch (_) {}
    }
    Map<String, dynamic>? complete;
    final cRaw = row['complete_payload'] as String?;
    if (cRaw != null && cRaw.isNotEmpty) {
      try {
        complete = Map<String, dynamic>.from(jsonDecode(cRaw) as Map);
      } catch (_) {}
    }
    Map<String, dynamic> create = {};
    final crRaw = row['create_payload'] as String?;
    if (crRaw != null && crRaw.isNotEmpty) {
      try {
        create = Map<String, dynamic>.from(jsonDecode(crRaw) as Map);
      } catch (_) {}
    }
    return PendingKargoSale(
      id: row['id'] as String,
      createdAt: DateTime.fromMillisecondsSinceEpoch(row['created_at'] as int),
      updatedAt: DateTime.fromMillisecondsSinceEpoch(row['updated_at'] as int),
      syncState: row['sync_state'] as String? ?? 'pending',
      lastError: row['last_error'] as String?,
      lastAttemptAt: row['last_attempt_at'] != null
          ? DateTime.fromMillisecondsSinceEpoch(row['last_attempt_at'] as int)
          : null,
      serverOrderId: row['server_order_id'] as int?,
      phase: row['phase'] as String? ?? 'none',
      createPayload: create,
      desiredCart: cart,
      completePayload: complete,
      finalized: (row['finalized'] as int? ?? 0) == 1,
      ocrTextSnapshot: row['ocr_text_snapshot'] as String?,
      trackingHint: row['tracking_hint'] as String?,
    );
  }
}

/// Sunucuya gönderilecek sıralı QR → birleştirilmiş adet (ekleme sırası korunur).
List<MapEntry<String, int>> mergeDesiredCartForApi(List<DesiredCartLine> lines) {
  final amounts = <String, int>{};
  final order = <String>[];
  for (final line in lines) {
    final qr = line.qrContent.trim();
    if (qr.isEmpty) continue;
    if (!amounts.containsKey(qr)) order.add(qr);
    amounts[qr] = (amounts[qr] ?? 0) + line.quantity;
  }
  return order.map((k) => MapEntry(k, amounts[k]!)).toList();
}
