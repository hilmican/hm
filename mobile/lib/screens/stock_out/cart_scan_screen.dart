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
    this.initialLines = const [],
    this.prefillTotalAmount,
    this.prefillNotes,
  });

  final int orderId;
  final String trackingNo;
  final bool resumed;
  final int initialLineUnits;
  final List<Map<String, dynamic>> initialLines;
  final double? prefillTotalAmount;
  final String? prefillNotes;

  @override
  State<CartScanScreen> createState() => _CartScanScreenState();
}

class _CartScanScreenState extends State<CartScanScreen> {
  final _api = HmaApiClient();
  late int _lineUnits;
  late List<Map<String, dynamic>> _lines;
  bool _loadingCart = false;

  @override
  void initState() {
    super.initState();
    _lineUnits = widget.initialLineUnits;
    _lines = List<Map<String, dynamic>>.from(widget.initialLines);
    _refreshCart();
  }

  Future<void> _refreshCart() async {
    setState(() => _loadingCart = true);
    try {
      final res = await _api.fetchKargoQrOrder(widget.orderId);
      final raw = res['lines'] as List<dynamic>? ?? [];
      if (!mounted) return;
      setState(() {
        _lines = raw.map((e) => Map<String, dynamic>.from(e as Map)).toList();
        _lineUnits = (res['order_item_count'] as num?)?.toInt() ?? 0;
        _loadingCart = false;
      });
    } catch (_) {
      if (mounted) setState(() => _loadingCart = false);
    }
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
      final raw = res['lines'] as List<dynamic>? ?? [];
      final cnt = (res['order_item_count'] as num?)?.toInt() ?? 0;
      if (mounted) {
        setState(() {
          _lineUnits = cnt;
          _lines =
              raw.map((e) => Map<String, dynamic>.from(e as Map)).toList();
        });
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Eklendi · toplam birim: $cnt')),
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

  Future<void> _removeLine(Map<String, dynamic> line) async {
    final itemId = line['item_id'] as int?;
    final q = (line['quantity'] as num?)?.toInt() ?? 0;
    if (itemId == null || q <= 0) return;

    final removeQty = await showDialog<int>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Satırı kaldır'),
        content: Text(
          '${line['name'] ?? line['sku'] ?? itemId} · adet: $q\n'
          'Kaç adet çıkarılsın?',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, 0),
            child: const Text('İptal'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(ctx, 1),
            child: const Text('1 adet'),
          ),
          if (q > 1)
            FilledButton(
              onPressed: () => Navigator.pop(ctx, q),
              child: const Text('Tümünü'),
            ),
        ],
      ),
    );
    if (removeQty == null || removeQty <= 0) return;

    try {
      final res = await _api.orderRemoveItem(
        orderId: widget.orderId,
        itemId: itemId,
        quantity: removeQty,
      );
      final raw = res['lines'] as List<dynamic>? ?? [];
      final cnt = (res['order_item_count'] as num?)?.toInt() ?? 0;
      if (mounted) {
        setState(() {
          _lineUnits = cnt;
          _lines =
              raw.map((e) => Map<String, dynamic>.from(e as Map)).toList();
        });
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
            const SizedBox(height: 8),
            Row(
              children: [
                Text('Sepet: $_lineUnits birim',
                    style: Theme.of(context).textTheme.titleSmall),
                if (_loadingCart) ...[
                  const SizedBox(width: 12),
                  const SizedBox(
                    width: 16,
                    height: 16,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
                ],
                const Spacer(),
                IconButton(
                  tooltip: 'Listeyi yenile',
                  onPressed: _loadingCart ? null : _refreshCart,
                  icon: const Icon(Icons.refresh),
                ),
              ],
            ),
            if (widget.prefillTotalAmount != null)
              Text(
                'Etiket tutarı: ${widget.prefillTotalAmount!.toStringAsFixed(2)} TL',
                style: TextStyle(
                  color: Theme.of(context).colorScheme.primary,
                  fontWeight: FontWeight.w500,
                ),
              ),
            const SizedBox(height: 8),
            Expanded(
              child: _lines.isEmpty
                  ? const Center(
                      child: Text(
                        'Henüz ürün yok.\nAşağıdan stok QR okutun.',
                        textAlign: TextAlign.center,
                      ),
                    )
                  : ListView.separated(
                      itemCount: _lines.length,
                      separatorBuilder: (_, __) => const Divider(height: 1),
                      itemBuilder: (context, i) {
                        final line = _lines[i];
                        final name = line['name'] as String? ??
                            line['sku']?.toString() ??
                            '#${line['item_id']}';
                        final sku = line['sku'] as String? ?? '';
                        final sz = line['size'] as String?;
                        final col = line['color'] as String?;
                        final q = (line['quantity'] as num?)?.toInt() ?? 0;
                        return ListTile(
                          title: Text(name),
                          subtitle: Text(
                            [
                              if (sku.isNotEmpty) sku,
                              if (sz != null && sz.isNotEmpty) sz,
                              if (col != null && col.isNotEmpty) col,
                            ].join(' · '),
                          ),
                          trailing: Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              Text('$q', style: const TextStyle(fontSize: 18)),
                              IconButton(
                                icon: const Icon(Icons.remove_circle_outline),
                                onPressed: () => _removeLine(line),
                              ),
                            ],
                          ),
                        );
                      },
                    ),
            ),
            FilledButton.icon(
              onPressed: _scanLine,
              icon: const Icon(Icons.add),
              label: const Text('Stok QR/barkod okut (ürün ekle)'),
            ),
            const SizedBox(height: 12),
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
                      prefillTotalAmount: widget.prefillTotalAmount,
                      prefillNotes: widget.prefillNotes,
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
