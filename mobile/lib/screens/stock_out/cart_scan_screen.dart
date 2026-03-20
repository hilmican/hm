import 'package:flutter/material.dart';

import '../../services/api_client.dart';
import '../../widgets/qr_scanner_widget.dart';
import 'order_complete_screen.dart';

class CartScanScreen extends StatefulWidget {
  const CartScanScreen({
    super.key,
    required this.orderId,
    required this.trackingNo,
    this.resumed = false,
    this.initialLineUnits = 0,
  });

  final int orderId;
  final String trackingNo;
  final bool resumed;
  final int initialLineUnits;

  @override
  State<CartScanScreen> createState() => _CartScanScreenState();
}

class _CartScanScreenState extends State<CartScanScreen> {
  final _api = HmaApiClient();
  late int _lineUnits;

  @override
  void initState() {
    super.initState();
    _lineUnits = widget.initialLineUnits;
  }

  Future<void> _scanLine() async {
    final code = await Navigator.of(context).push<String>(
      MaterialPageRoute(
        builder: (_) => const QrScannerWidget(
          title: 'Stok QR/barkod (hma:item:…)',
        ),
      ),
    );
    if (code == null || code.isEmpty) return;
    try {
      final res = await _api.orderAddItem(
        orderId: widget.orderId,
        qrContent: code,
        quantity: 1,
      );
      final cnt = (res['order_item_count'] as num?)?.toInt() ?? 0;
      if (mounted) {
        setState(() => _lineUnits = cnt);
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Eklendi · toplam adet: $cnt')),
        );
      }
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Hata: $e')),
        );
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('Sipariş #${widget.orderId}')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            if (widget.resumed) const Chip(label: Text('Taslak devam ediyor')),
            Text('Takip: ${widget.trackingNo}'),
            Text('Sepetteki toplam birim (sunucu): $_lineUnits'),
            const SizedBox(height: 24),
            FilledButton.icon(
              onPressed: _scanLine,
              icon: const Icon(Icons.add),
              label: const Text('Stok QR/barkod okut (ürün ekle)'),
            ),
            const Spacer(),
            FilledButton(
              style: FilledButton.styleFrom(
                backgroundColor: Theme.of(context).colorScheme.tertiary,
              ),
              onPressed: () {
                Navigator.of(context).push(
                  MaterialPageRoute<void>(
                    builder: (_) => OrderCompleteScreen(
                      orderId: widget.orderId,
                      trackingNo: widget.trackingNo,
                    ),
                  ),
                );
              },
              child: const Padding(
                padding: EdgeInsets.symmetric(vertical: 14),
                child: Text('Bedel belirle ve bitir'),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
