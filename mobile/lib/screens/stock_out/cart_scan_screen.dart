import 'package:flutter/material.dart';
import 'package:uuid/uuid.dart';

import '../../services/api_client.dart';
import '../../services/pending_kargo_models.dart';
import '../../services/pending_kargo_store.dart';
import '../../widgets/qr_scanner_widget.dart';
import 'order_complete_screen.dart';

const _uuid = Uuid();

class CartScanScreen extends StatefulWidget {
  const CartScanScreen({
    super.key,
    this.orderId,
    this.localPendingId,
    required this.trackingNo,
    this.resumed = false,
    this.initialLineUnits = 0,
    this.initialLines = const [],
    this.prefillTotalAmount,
    this.prefillNotes,
    this.labelFields,
  }) : assert(
          orderId != null || localPendingId != null,
          'orderId veya localPendingId gerekli',
        );

  /// Çevrimiçi sipariş; çevrimdışı modda null.
  final int? orderId;

  /// Yerel taslak kimliği (çevrimdışı).
  final String? localPendingId;

  final String trackingNo;
  final bool resumed;
  final int initialLineUnits;
  final List<Map<String, dynamic>> initialLines;
  final double? prefillTotalAmount;
  final String? prefillNotes;
  final Map<String, dynamic>? labelFields;

  bool get isOffline => localPendingId != null;

  @override
  State<CartScanScreen> createState() => _CartScanScreenState();
}

class _CartScanScreenState extends State<CartScanScreen> {
  final _api = HmaApiClient();
  late int _lineUnits;
  late List<Map<String, dynamic>> _lines;
  bool _loadingCart = false;
  Map<String, dynamic>? _labelFields;

  @override
  void initState() {
    super.initState();
    _lineUnits = widget.initialLineUnits;
    _lines = List<Map<String, dynamic>>.from(widget.initialLines);
    _labelFields = widget.labelFields;
    if (widget.isOffline) {
      _loadOfflineCart();
    } else {
      _refreshCart();
    }
  }

  Future<void> _loadOfflineCart() async {
    final id = widget.localPendingId;
    if (id == null) return;
    final sale = await PendingKargoStore.instance.get(id);
    if (!mounted || sale == null) return;
    setState(() {
      _lines = sale.desiredCart.map(_desiredLineToMap).toList();
      _lineUnits = sale.itemUnitCount;
      if (_labelFields == null || _labelFields!.isEmpty) {
        _labelFields = sale.labelFieldsForOfflineCard();
      }
    });
  }

  Map<String, dynamic> _desiredLineToMap(DesiredCartLine L) {
    final qr = L.qrContent;
    final short = qr.length > 52 ? '${qr.substring(0, 52)}…' : qr;
    return {
      'name': short,
      'sku': '',
      'line_id': L.lineId,
      'quantity': L.quantity,
      'item_id': null,
      'full_qr': qr,
    };
  }

  List<DesiredCartLine> _mapsToDesired(List<Map<String, dynamic>> maps) {
    final out = <DesiredCartLine>[];
    for (final m in maps) {
      final lid = m['line_id'] as String?;
      final qr = m['full_qr'] as String? ?? m['name'] as String? ?? '';
      final q = (m['quantity'] as num?)?.toInt() ?? 1;
      if (lid != null && qr.isNotEmpty) {
        out.add(DesiredCartLine(lineId: lid, qrContent: qr, quantity: q));
      }
    }
    return out;
  }

  Future<void> _persistOfflineCart() async {
    final id = widget.localPendingId;
    if (id == null) return;
    final desired = _mapsToDesired(_lines);
    await PendingKargoStore.instance.updateDesiredCart(id, desired);
  }

  Future<void> _refreshCart() async {
    if (widget.isOffline) {
      await _loadOfflineCart();
      return;
    }
    final oid = widget.orderId;
    if (oid == null) return;
    setState(() => _loadingCart = true);
    try {
      final res = await _api.fetchKargoQrOrder(oid);
      final raw = res['lines'] as List<dynamic>? ?? [];
      if (!mounted) return;
      final lf = res['label_fields'];
      setState(() {
        _lines = raw.map((e) => Map<String, dynamic>.from(e as Map)).toList();
        _lineUnits = (res['order_item_count'] as num?)?.toInt() ?? 0;
        _loadingCart = false;
        if (lf is Map) {
          _labelFields = Map<String, dynamic>.from(lf);
        }
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
    if (widget.isOffline) {
      final trimmed = code.trim();
      final line = DesiredCartLine(
        lineId: _uuid.v4(),
        qrContent: trimmed,
        quantity: 1,
      );
      setState(() {
        _lines = [..._lines, _desiredLineToMap(line)];
        _lineUnits += 1;
      });
      await _persistOfflineCart();
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Yerelde eklendi · toplam birim: $_lineUnits')),
        );
      }
      return;
    }
    final oid = widget.orderId;
    if (oid == null) return;
    try {
      final res = await _api.orderAddItem(
        orderId: oid,
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
    final lineId = line['line_id'] as String?;
    final fullQr = line['full_qr'] as String? ?? line['name'] as String? ?? '';
    final q = (line['quantity'] as num?)?.toInt() ?? 0;
    if (q <= 0) return;

    if (widget.isOffline) {
      if (lineId == null) return;
      final removeQty = await showDialog<int>(
        context: context,
        builder: (ctx) => AlertDialog(
          title: const Text('Satırı kaldır'),
          content: Text(
            '${line['name'] ?? fullQr} · adet: $q\nKaç adet çıkarılsın?',
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

      final list = <Map<String, dynamic>>[];
      for (final m in _lines) {
        if (m['line_id'] != lineId) {
          list.add(m);
          continue;
        }
        final remain = (m['quantity'] as num?)?.toInt() ?? 0;
        final newQ = remain - removeQty;
        if (newQ > 0) {
          final copy = Map<String, dynamic>.from(m);
          copy['quantity'] = newQ;
          list.add(copy);
        }
      }
      setState(() {
        _lines = list;
        _lineUnits = list.fold<int>(
          0,
          (s, m) => s + ((m['quantity'] as num?)?.toInt() ?? 0),
        );
      });
      await _persistOfflineCart();
      return;
    }

    if (itemId == null) return;

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

    final oid = widget.orderId;
    if (oid == null) return;
    try {
      final res = await _api.orderRemoveItem(
        orderId: oid,
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

  Widget _labelCard(BuildContext context) {
    final f = _labelFields;
    if (f == null || f.isEmpty) return const SizedBox.shrink();
    String? s(dynamic v) => v == null ? null : v.toString().trim();
    final name = s(f['recipient_name']);
    final phone = s(f['phone']);
    final addr = s(f['address']);
    final content = s(f['content']);
    final cod = f['cod_amount'];
    final codStr = cod is num
        ? '${cod.toDouble().toStringAsFixed(2)} TL'
        : (cod != null ? cod.toString() : null);
    final track = s(f['tracking_no']);
    if (name == null &&
        phone == null &&
        addr == null &&
        content == null &&
        codStr == null &&
        (track == null || track.isEmpty)) {
      return const SizedBox.shrink();
    }
    final theme = Theme.of(context);
    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              widget.isOffline ? 'Etiket (çevrimdışı)' : 'Etiket (OCR)',
              style: theme.textTheme.titleSmall,
            ),
            if (track != null && track.isNotEmpty) Text('Takip no: $track'),
            if (name != null && name.isNotEmpty) Text('Alıcı: $name'),
            if (phone != null && phone.isNotEmpty) Text('Tel: $phone'),
            if (addr != null && addr.isNotEmpty) Text('Adres: $addr'),
            if (content != null && content.isNotEmpty) Text('İçerik: $content'),
            if (codStr != null) Text('Tahsilat: $codStr'),
          ],
        ),
      ),
    );
  }

  String _title() {
    if (widget.isOffline) {
      return 'Yerel taslak';
    }
    return 'Sipariş #${widget.orderId}';
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text(_title())),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            if (widget.resumed) const Chip(label: Text('Taslak devam ediyor')),
            if (widget.isOffline)
              const Chip(
                avatar: Icon(Icons.cloud_off, size: 18),
                label: Text('Çevrimdışı — internet gelince gönderilecek'),
              ),
            Text('Takip: ${widget.trackingNo}'),
            const SizedBox(height: 8),
            _labelCard(context),
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
                            '#${line['item_id'] ?? line['line_id']}';
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
                      localPendingId: widget.localPendingId,
                      trackingNo: widget.trackingNo,
                      prefillTotalAmount: widget.prefillTotalAmount,
                      prefillNotes: widget.prefillNotes,
                      labelFields: _labelFields ?? widget.labelFields,
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
