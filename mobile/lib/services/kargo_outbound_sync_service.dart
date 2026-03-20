import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/foundation.dart';

import 'api_client.dart';
import 'pending_kargo_models.dart';
import 'pending_kargo_store.dart';

/// Sunucu fazları: none → created → cart_applied; tamamlanınca kayıt silinir.
class KargoOutboundSyncService {
  KargoOutboundSyncService({HmaApiClient? api}) : _api = api ?? HmaApiClient();

  final HmaApiClient _api;

  static Future<bool> _looksOnline() async {
    if (kIsWeb) return true;
    final r = await Connectivity().checkConnectivity();
    if (r.isEmpty) return false;
    return r.any((e) => e != ConnectivityResult.none);
  }

  static Future<void> maybeAutoSyncAll() async {
    if (kIsWeb) return;
    if (!await _looksOnline()) return;
    await Future<void>.delayed(const Duration(milliseconds: 400));
    if (!await _looksOnline()) return;
    await KargoOutboundSyncService().syncAllFinalized();
  }

  Future<void> syncAllFinalized() async {
    final store = PendingKargoStore.instance;
    if (!store.isReady) return;
    final list = await store.listFinalizedPending();
    for (final sale in list) {
      if (sale.syncState == 'syncing') continue;
      try {
        await syncOne(sale.id);
      } catch (_) {
        // syncOne içinde last_error güncellenir
      }
    }
  }

  Future<void> syncOne(String id) async {
    final store = PendingKargoStore.instance;
    if (!store.isReady) {
      throw StateError('Store not initialized');
    }
    var sale = await store.getRequired(id);
    if (!sale.finalized || sale.completePayload == null) {
      return;
    }

    await store.patch(
      id,
      syncState: 'syncing',
      lastAttemptAt: DateTime.now(),
      clearLastError: true,
    );
    sale = await store.getRequired(id);

    try {
      var phase = sale.phase;
      var serverOrderId = sale.serverOrderId;

      if (phase == 'none') {
        final cp = sale.createPayload;
        final res = await _api.orderFromKargoQr(
          qrContent: (cp['qr_content'] as String?)?.trim() ?? '',
          ocrText: cp['ocr_text'] as String?,
          fields: cp['fields'] is Map
              ? Map<String, dynamic>.from(cp['fields'] as Map)
              : null,
        );
        serverOrderId = res['order_id'] as int?;
        if (serverOrderId == null) {
          throw Exception('order-from-kargo-qr: order_id yok');
        }
        await store.patch(
          id,
          phase: 'created',
          serverOrderId: serverOrderId,
        );
        phase = 'created';
      }

      if (phase == 'created') {
        serverOrderId = serverOrderId ?? (await store.getRequired(id)).serverOrderId;
        if (serverOrderId == null) {
          throw Exception('Sunucu sipariş no eksik');
        }
        final cart = await _api.fetchKargoQrOrder(serverOrderId);
        final rawLines = cart['lines'] as List<dynamic>? ?? [];
        for (final line in rawLines) {
          final map = Map<String, dynamic>.from(line as Map);
          final itemId = map['item_id'] as int?;
          final q = (map['quantity'] as num?)?.toInt() ?? 0;
          if (itemId != null && q > 0) {
            await _api.orderRemoveItem(
              orderId: serverOrderId,
              itemId: itemId,
              quantity: q,
            );
          }
        }
        sale = await store.getRequired(id);
        final merged = mergeDesiredCartForApi(sale.desiredCart);
        for (final e in merged) {
          await _api.orderAddItem(
            orderId: serverOrderId,
            qrContent: e.key,
            quantity: e.value,
          );
        }
        await store.patch(id, phase: 'cart_applied');
        phase = 'cart_applied';
      }

      if (phase == 'cart_applied') {
        sale = await store.getRequired(id);
        serverOrderId = sale.serverOrderId;
        if (serverOrderId == null) {
          throw Exception('cart_applied ama server_order_id yok');
        }
        final fin = sale.completePayload!;
        final total = fin['total_amount'];
        double? totalAmount;
        if (total is num) {
          totalAmount = total.toDouble();
        } else if (total is String) {
          totalAmount = double.tryParse(total.replaceAll(',', '.'));
        }
        String? notes = fin['notes'] as String?;
        if (notes != null && notes.isEmpty) notes = null;
        final checkoutMode = fin['checkout_mode'] as String? ?? 'cod';
        final paymentMethod = fin['payment_method'] as String?;

        await _api.orderComplete(
          orderId: serverOrderId,
          totalAmount: totalAmount,
          paymentMethod: paymentMethod,
          notes: notes,
          checkoutMode: checkoutMode,
        );
        await store.delete(id);
        return;
      }

      throw Exception('Bilinmeyen phase: $phase');
    } catch (e) {
      await store.patch(
        id,
        syncState: 'failed',
        lastError: e.toString(),
      );
      rethrow;
    }
  }

}
