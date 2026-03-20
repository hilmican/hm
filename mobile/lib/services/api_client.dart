import 'dart:convert';

import 'package:flutter/foundation.dart' show kIsWeb;
import 'package:http/http.dart' as http;

import 'settings_service.dart';

class ApiException implements Exception {
  ApiException(this.statusCode, this.body);
  final int statusCode;
  final String body;

  @override
  String toString() => 'HTTP $statusCode: $body';
}

class HmaApiClient {
  HmaApiClient({String? baseUrl, String? mobileApiKey})
      : _base = (baseUrl ?? SettingsService.instance.baseUrl)
            .replaceAll(RegExp(r'/$'), ''),
        _key = mobileApiKey ?? SettingsService.instance.mobileApiKey;

  final String _base;
  final String _key;

  Map<String, String> _headers({bool jsonBody = false}) {
    final h = <String, String>{
      if (_key.isNotEmpty) 'X-Mobile-API-Key': _key,
      if (jsonBody) 'Content-Type': 'application/json',
    };
    return h;
  }

  Future<dynamic> getJson(String path) async {
    final uri = Uri.parse('$_base$path');
    try {
      final r = await http.get(uri, headers: _headers());
      return _decode(r);
    } on http.ClientException catch (e) {
      throw _wrapNetworkError(uri, e);
    }
  }

  Future<dynamic> postJson(String path, Map<String, dynamic> body) async {
    final uri = Uri.parse('$_base$path');
    try {
      final r = await http.post(
        uri,
        headers: _headers(jsonBody: true),
        body: jsonEncode(body),
      );
      return _decode(r);
    } on http.ClientException catch (e) {
      throw _wrapNetworkError(uri, e);
    }
  }

  /// Tarayıcıda gerçek sebep gizlenir ("Failed to fetch"); olası nedenleri metne dökelim.
  ApiException _wrapNetworkError(Uri uri, http.ClientException e) {
    if (kIsWeb) {
      return ApiException(
        0,
        'Bağlantı kurulamadı (${e.message}).\n\n'
        'Olası nedenler:\n'
        '• CORS: Sunucu bu sayfanın origin’ine izin vermiyor. HMA’da `HMA_CORS_ORIGINS` veya '
        '`HMA_CORS_ORIGIN_REGEX` (app/main.py); LAN IP ile açıyorsanız (ör. 192.168.x.x) localhost '
        'regex’i yetmez, origin’i env ile ekleyin.\n'
        '• Ağ / VPN / güvenlik duvarı hma.cdn.com.tr engelliyor olabilir.\n'
        '• Uzun süren istek (nginx / CDN timeout) — seri stok endpoint’i çok varyantta yavaşlayabilir.\n\n'
        'Hedef: $uri',
      );
    }
    return ApiException(0, e.message);
  }

  dynamic _decode(http.Response r) {
    final text = r.body;
    if (r.statusCode >= 200 && r.statusCode < 300) {
      if (text.isEmpty) return null;
      return jsonDecode(text) as dynamic;
    }
    throw ApiException(r.statusCode, text);
  }

  // --- Domain calls ---

  Future<List<Map<String, dynamic>>> fetchProducts() async {
    final data = await getJson('/products?limit=500') as Map<String, dynamic>;
    final list = data['products'] as List<dynamic>? ?? [];
    return list.cast<Map<String, dynamic>>();
  }

  Future<Map<String, dynamic>> fetchAttributes(int productId) async {
    return await getJson('/inventory/attributes?product_id=$productId')
        as Map<String, dynamic>;
  }

  /// Cari bazlı alış/satış önerileri (HMA stok ekranıyla aynı kaynak).
  Future<Map<String, dynamic>> fetchSupplierPrices(int productId) async {
    return await getJson('/products/$productId/supplier-prices')
        as Map<String, dynamic>;
  }

  Future<Map<String, dynamic>> seriesPrintAndStock({
    required int productId,
    required String color,
    required int quantityPerVariant,
    required double unitCost,
    int? supplierId,
    double? price,
    double? itemCost,
    bool dryRun = false,
  }) async {
    return await postJson('/magaza-satis/api/series-print-and-stock', {
      'product_id': productId,
      'color': color,
      'quantity_per_variant': quantityPerVariant,
      'unit_cost': unitCost,
      if (supplierId != null) 'supplier_id': supplierId,
      if (price != null) 'price': price,
      if (itemCost != null) 'cost': itemCost,
      if (dryRun) 'dry_run': true,
    }) as Map<String, dynamic>;
  }

  Future<Map<String, dynamic>> orderFromKargoQr({
    required String qrContent,
    Map<String, String>? fields,
  }) async {
    return await postJson('/magaza-satis/api/order-from-kargo-qr', {
      'qr_content': qrContent,
      if (fields != null && fields.isNotEmpty) 'fields': fields,
    }) as Map<String, dynamic>;
  }

  Future<Map<String, dynamic>> orderAddItem({
    required int orderId,
    required String qrContent,
    int quantity = 1,
  }) async {
    return await postJson('/magaza-satis/api/order-add-item', {
      'order_id': orderId,
      'qr_content': qrContent,
      'quantity': quantity,
    }) as Map<String, dynamic>;
  }

  Future<Map<String, dynamic>> orderComplete({
    required int orderId,
    required double totalAmount,
    required String paymentMethod,
    String? notes,
  }) async {
    return await postJson('/magaza-satis/api/order-complete', {
      'order_id': orderId,
      'total_amount': totalAmount,
      'payment_method': paymentMethod,
      if (notes != null && notes.isNotEmpty) 'notes': notes,
    }) as Map<String, dynamic>;
  }
}
