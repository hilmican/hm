import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:pdf/pdf.dart';
import 'package:pdf/widgets.dart' as pw;
import 'package:printing/printing.dart';
import 'package:qr_flutter/qr_flutter.dart';

import '../../services/api_client.dart';
import '../../services/settings_service.dart';

class _SupplierOption {
  const _SupplierOption({required this.id, required this.name});
  final int id;
  final String name;
}

class SeriesPrintScreen extends StatefulWidget {
  const SeriesPrintScreen({
    super.key,
    required this.productId,
    required this.productName,
    this.defaultPrice,
    this.defaultCost,
  });

  final int productId;
  final String productName;
  final double? defaultPrice;
  final double? defaultCost;

  @override
  State<SeriesPrintScreen> createState() => _SeriesPrintScreenState();
}

class _SeriesPrintScreenState extends State<SeriesPrintScreen> {
  final _api = HmaApiClient();
  List<String> _colors = [];
  String? _color;
  final _qtyCtrl = TextEditingController(text: '1');
  late final TextEditingController _costCtrl;
  late final TextEditingController _priceCtrl;

  List<_SupplierOption> _supplierOptions = [];
  List<Map<String, dynamic>> _supplierPriceRows = [];
  int? _supplierId;

  bool _loadingAttr = true;
  List<Map<String, dynamic>> _qrPayloads = [];
  List<Map<String, dynamic>> _stockUnitPayloads = [];
  String? _error;
  bool _submitting = false;
  bool _pdfBusy = false;

  static String _numToField(double? v) {
    if (v == null || v == 0) return '';
    if (v == v.roundToDouble()) return v.round().toString();
    return v.toString();
  }

  @override
  void initState() {
    super.initState();
    _costCtrl = TextEditingController(text: _numToField(widget.defaultCost));
    _priceCtrl = TextEditingController(text: _numToField(widget.defaultPrice));
    _loadAttr();
  }

  @override
  void dispose() {
    _qtyCtrl.dispose();
    _costCtrl.dispose();
    _priceCtrl.dispose();
    super.dispose();
  }

  void _applySupplierSuggestion() {
    final sid = _supplierId;
    if (sid == null || _supplierPriceRows.isEmpty) return;

    Map<String, dynamic>? entry;
    for (final r in _supplierPriceRows) {
      if (_supplierIdOf(r) != sid) continue;
      if (r['item_id'] == null) {
        entry = r;
        break;
      }
    }
    if (entry == null) {
      for (final r in _supplierPriceRows) {
        if (_supplierIdOf(r) != sid) continue;
        if (r['cost'] != null) {
          entry = r;
          break;
        }
      }
    }

    if (entry == null) return;
    final c = entry['cost'];
    final pr = entry['price'];
    if (c is num && c.toDouble() > 0) {
      _costCtrl.text = _numToField(c.toDouble());
    }
    if (pr is num && pr.toDouble() > 0) {
      _priceCtrl.text = _numToField(pr.toDouble());
    }
  }

  int? _supplierIdOf(Map<String, dynamic> r) {
    final s = r['supplier_id'];
    if (s is int) return s;
    if (s is num) return s.toInt();
    return null;
  }

  Future<void> _loadAttr() async {
    setState(() {
      _loadingAttr = true;
      _error = null;
    });
    try {
      final m = await _api.fetchAttributes(widget.productId);
      final sp = await _api.fetchSupplierPrices(widget.productId);

      final colors = (m['colors'] as List<dynamic>? ?? []).map((e) => e.toString()).toList();
      final rows = (sp['supplier_prices'] as List<dynamic>? ?? [])
          .map((e) => Map<String, dynamic>.from(e as Map))
          .toList();

      // Web inventory_table: sadece cost tanımlı cariler listede
      final withCostIds = <int>{};
      for (final r in rows) {
        if (r['cost'] == null) continue;
        final sid = _supplierIdOf(r);
        if (sid != null) withCostIds.add(sid);
      }

      final names = <int, String>{};
      for (final r in rows) {
        final sid = _supplierIdOf(r);
        if (sid == null || !withCostIds.contains(sid)) continue;
        final n = r['supplier_name'] as String?;
        if (n != null && n.isNotEmpty) names[sid] = n;
      }

      final options = withCostIds.map((id) => _SupplierOption(id: id, name: names[id] ?? 'Cari #$id')).toList()
        ..sort((a, b) => a.name.toLowerCase().compareTo(b.name.toLowerCase()));

      int? chosen = _supplierId;
      if (options.length == 1) {
        chosen = options.first.id;
      }

      if (!mounted) return;
      setState(() {
        _colors = colors;
        _color = colors.isNotEmpty ? colors.first : null;
        _supplierPriceRows = rows;
        _supplierOptions = options;
        _supplierId = chosen;
        _loadingAttr = false;
      });

      if (chosen != null) {
        _applySupplierSuggestion();
        setState(() {});
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _error = e.toString();
          _loadingAttr = false;
        });
      }
    }
  }

  Future<void> _submit() async {
    final color = _color;
    if (color == null || color.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Renk seçin veya üründe renk tanımlı değil.')),
      );
      return;
    }
    if (_supplierOptions.isNotEmpty && _supplierId == null) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Cari seçin (HMA’daki stok ekranında olduğu gibi).')),
      );
      return;
    }
    final qty = int.tryParse(_qtyCtrl.text) ?? 0;
    final cost = double.tryParse(_costCtrl.text.replaceAll(',', '.')) ?? 0;
    if (qty <= 0 || cost <= 0) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Adet ve birim maliyet > 0 olmalı.')),
      );
      return;
    }

    final priceStr = _priceCtrl.text.trim();
    double? salePrice;
    if (priceStr.isNotEmpty) {
      salePrice = double.tryParse(priceStr.replaceAll(',', '.'));
    }

    setState(() {
      _submitting = true;
      _error = null;
      _qrPayloads = [];
      _stockUnitPayloads = [];
    });
    try {
      final dryRun = SettingsService.instance.stockDryRun;
      final res = await _api.seriesPrintAndStock(
        productId: widget.productId,
        color: color,
        quantityPerVariant: qty,
        unitCost: cost,
        supplierId: _supplierId,
        price: salePrice,
        itemCost: cost,
        dryRun: dryRun,
      );
      final list = (res['qr_payloads'] as List<dynamic>? ?? [])
          .map((e) => Map<String, dynamic>.from(e as Map))
          .toList();
      final units = (res['stock_units'] as List<dynamic>? ?? [])
          .map((e) => Map<String, dynamic>.from(e as Map))
          .toList();
      if (!mounted) return;
      setState(() {
        _qrPayloads = list;
        _stockUnitPayloads = units;
        _submitting = false;
      });
      final serverDry = res['dry_run'] == true;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            serverDry
                ? 'Dry-run tamam: stok değişmedi. Önizleme QR’larına bakın.'
                : 'Stoğa eklendi. QR’ları yazdırın veya ekran görüntüsü alın.',
          ),
        ),
      );
    } catch (e) {
      setState(() {
        _error = e.toString();
        _submitting = false;
      });
    }
  }

  bool get _hasAnyLabels => _qrPayloads.isNotEmpty || _stockUnitPayloads.isNotEmpty;

  Future<void> _copyAllQrLines() async {
    final buf = StringBuffer();
    for (final row in _qrPayloads) {
      final data = row['qr_data'] as String? ?? '';
      final sku = row['sku'] as String? ?? '';
      final size = row['size'] as String? ?? '';
      buf.writeln('$sku\t$size\t$data');
    }
    for (final row in _stockUnitPayloads) {
      final data = row['qr_data'] as String? ?? '';
      final sku = row['sku'] as String? ?? '';
      final size = row['size'] as String? ?? '';
      final uid = row['stock_unit_id']?.toString() ?? '';
      buf.writeln('unit:$uid\t$sku\t$size\t$data');
    }
    await Clipboard.setData(ClipboardData(text: buf.toString().trim()));
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('SKU · beden · qr_data panoya kopyalandı (Excel / etiket yazılımı için)')),
    );
  }

  /// Her etiket ~50×35 mm; yazıcı diyalogunda doğru kağıt / ölçek seçin.
  Future<Uint8List> _buildLabelsPdfBytes() async {
    final doc = pw.Document();
    const pageW = 50 * PdfPageFormat.mm;
    const pageH = 35 * PdfPageFormat.mm;

    void addPage(String title, String data) {
      doc.addPage(
        pw.Page(
          pageFormat: PdfPageFormat(pageW, pageH, marginAll: 4),
          build: (ctx) => pw.Center(
            child: pw.Column(
              mainAxisAlignment: pw.MainAxisAlignment.center,
              children: [
                pw.BarcodeWidget(
                  barcode: pw.Barcode.qrCode(),
                  data: data,
                  width: 22 * PdfPageFormat.mm,
                  height: 22 * PdfPageFormat.mm,
                ),
                pw.SizedBox(height: 1 * PdfPageFormat.mm),
                pw.Text(
                  title,
                  style: const pw.TextStyle(fontSize: 7),
                  maxLines: 2,
                  textAlign: pw.TextAlign.center,
                ),
                pw.Text(
                  data,
                  style: pw.TextStyle(fontSize: 5, color: PdfColors.grey800),
                  maxLines: 2,
                  textAlign: pw.TextAlign.center,
                ),
              ],
            ),
          ),
        ),
      );
    }

    for (final row in _qrPayloads) {
      final data = row['qr_data'] as String? ?? '';
      final sku = row['sku'] as String? ?? '';
      final size = row['size'] as String? ?? '';
      addPage('$sku · $size', data);
    }
    for (final row in _stockUnitPayloads) {
      final data = row['qr_data'] as String? ?? '';
      final sku = row['sku'] as String? ?? '';
      final size = row['size'] as String? ?? '';
      final uid = row['stock_unit_id']?.toString() ?? '';
      addPage('parça $uid · $sku · $size', data);
    }

    return doc.save();
  }

  Future<void> _openLabelsPdf() async {
    setState(() => _pdfBusy = true);
    try {
      final bytes = await _buildLabelsPdfBytes();
      await Printing.layoutPdf(
        onLayout: (PdfPageFormat format) async => bytes,
      );
    } catch (e, st) {
      debugPrint('PDF yazdırma: $e\n$st');
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('PDF / yazdırma hatası: $e')),
      );
    } finally {
      if (mounted) setState(() => _pdfBusy = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final dryRun = SettingsService.instance.stockDryRun;
    return Scaffold(
      appBar: AppBar(title: Text(widget.productName)),
      body: _loadingAttr
          ? const Center(child: CircularProgressIndicator())
          : SingleChildScrollView(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  if (_error != null) ...[
                    Text(_error!, style: TextStyle(color: Theme.of(context).colorScheme.error)),
                    const SizedBox(height: 8),
                  ],
                  if (dryRun) ...[
                    Card(
                      color: Theme.of(context).colorScheme.tertiaryContainer,
                      child: Padding(
                        padding: const EdgeInsets.all(12),
                        child: Text(
                          'TEST MODU: Stok hareketi yapılmaz. Ayarlardan kapatabilirsiniz.',
                          style: TextStyle(
                            color: Theme.of(context).colorScheme.onTertiaryContainer,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ),
                    ),
                    const SizedBox(height: 12),
                  ],
                  const Text('Renk', style: TextStyle(fontWeight: FontWeight.bold)),
                  if (_colors.isEmpty)
                    const Text('Bu ürün için renk listesi boş. Önce HMA’da varyant oluşturun.')
                  else
                    DropdownButtonFormField<String>(
                      value: _color,
                      items: _colors
                          .map((c) => DropdownMenuItem(value: c, child: Text(c)))
                          .toList(),
                      onChanged: (v) => setState(() => _color = v),
                    ),
                  const SizedBox(height: 16),
                  const Text('Cari (tedarikçi)', style: TextStyle(fontWeight: FontWeight.bold)),
                  const SizedBox(height: 4),
                  if (_supplierOptions.isEmpty)
                    Text(
                      'Bu ürün için cari maliyeti tanımlı değil. '
                      'HMA ürün / maliyet ekranında cari bazlı alış ekleyebilirsiniz; '
                      'yine de stok girişi cari olmadan yapılabilir.',
                      style: TextStyle(fontSize: 13, color: Theme.of(context).colorScheme.onSurfaceVariant),
                    )
                  else
                    DropdownButtonFormField<int>(
                      value: _supplierId,
                      items: [
                        const DropdownMenuItem<int>(
                          value: null,
                          child: Text('Cari seçin'),
                        ),
                        ..._supplierOptions.map(
                          (s) => DropdownMenuItem(value: s.id, child: Text(s.name)),
                        ),
                      ],
                      onChanged: (v) {
                        setState(() => _supplierId = v);
                        _applySupplierSuggestion();
                      },
                    ),
                  const SizedBox(height: 16),
                  TextField(
                    controller: _qtyCtrl,
                    keyboardType: TextInputType.number,
                    decoration: const InputDecoration(
                      labelText: 'Varyant başına adet',
                      border: OutlineInputBorder(),
                    ),
                  ),
                  const SizedBox(height: 16),
                  TextField(
                    controller: _priceCtrl,
                    keyboardType: const TextInputType.numberWithOptions(decimal: true),
                    decoration: const InputDecoration(
                      labelText: 'Satış fiyatı (varyant, opsiyonel)',
                      hintText: 'Ürün / cari önerisinden doldurulur, düzenleyebilirsiniz',
                      border: OutlineInputBorder(),
                    ),
                  ),
                  const SizedBox(height: 16),
                  TextField(
                    controller: _costCtrl,
                    keyboardType: const TextInputType.numberWithOptions(decimal: true),
                    decoration: const InputDecoration(
                      labelText: 'Birim maliyet (alış, stok hareketi)',
                      hintText: 'Cari maliyet veya ürün varsayılanından',
                      border: OutlineInputBorder(),
                    ),
                  ),
                  const SizedBox(height: 24),
                  FilledButton(
                    onPressed: _submitting ? null : _submit,
                    child: _submitting
                        ? const SizedBox(
                            height: 22,
                            width: 22,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : Text(dryRun ? 'Test: doğrula ve QR önizle (stok yok)' : 'Stoğa ekle ve QR üret'),
                  ),
                  const SizedBox(height: 24),
                  if (_hasAnyLabels) ...[
                    const Text('Yazdırma', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16)),
                    const SizedBox(height: 6),
                    Text(
                      'Ağdaki etiket yazıcısı: PDF ile sistem yazdır penceresini açıp yazıcıyı seçin '
                      '(Aynı LAN’daki AirPrint / IPP / paylaşılan yazıcı). '
                      'Üretici yazılımı kullanıyorsanız “metinleri kopyala” ile içe aktarın.',
                      style: TextStyle(
                        fontSize: 12,
                        color: Theme.of(context).colorScheme.onSurfaceVariant,
                      ),
                    ),
                    const SizedBox(height: 10),
                    Wrap(
                      spacing: 8,
                      runSpacing: 8,
                      children: [
                        OutlinedButton.icon(
                          onPressed: _pdfBusy ? null : _copyAllQrLines,
                          icon: const Icon(Icons.copy, size: 20),
                          label: const Text('Metinleri kopyala'),
                        ),
                        FilledButton.tonalIcon(
                          onPressed: _pdfBusy ? null : _openLabelsPdf,
                          icon: _pdfBusy
                              ? const SizedBox(
                                  width: 18,
                                  height: 18,
                                  child: CircularProgressIndicator(strokeWidth: 2),
                                )
                              : const Icon(Icons.print, size: 20),
                          label: Text(_pdfBusy ? 'PDF…' : 'PDF — yazdır / paylaş'),
                        ),
                      ],
                    ),
                    const SizedBox(height: 20),
                  ],
                  if (_qrPayloads.isNotEmpty) ...[
                    const Text('Etiket QR’ları', style: TextStyle(fontWeight: FontWeight.bold, fontSize: 18)),
                    const SizedBox(height: 8),
                    ..._qrPayloads.map((row) {
                      final data = row['qr_data'] as String? ?? '';
                      final sku = row['sku'] as String? ?? '';
                      final size = row['size'] as String? ?? '';
                      final wouldCreate = row['would_create_variant'] == true;
                      return Card(
                        margin: const EdgeInsets.only(bottom: 16),
                        child: Padding(
                          padding: const EdgeInsets.all(12),
                          child: Column(
                            children: [
                              Text('$sku · $size', style: const TextStyle(fontWeight: FontWeight.w600)),
                              if (wouldCreate)
                                Padding(
                                  padding: const EdgeInsets.only(top: 6),
                                  child: Text(
                                    'Simülasyon: bu bedende henüz varyant yok; gerçek girişte oluşur.',
                                    style: TextStyle(
                                      fontSize: 12,
                                      color: Theme.of(context).colorScheme.primary,
                                    ),
                                  ),
                                ),
                              const SizedBox(height: 8),
                              QrImageView(
                                data: data,
                                size: 180,
                                backgroundColor: Colors.white,
                              ),
                              SelectableText(data, style: const TextStyle(fontSize: 12)),
                            ],
                          ),
                        ),
                      );
                    }),
                  ],
                  if (_stockUnitPayloads.isNotEmpty) ...[
                    const SizedBox(height: 24),
                    const Text(
                      'Parça etiketleri (tek ürün = tek QR, sunucuda HMA_STOCK_UNIT_TRACKING=1)',
                      style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16),
                    ),
                    const SizedBox(height: 8),
                    ..._stockUnitPayloads.map((row) {
                      final data = row['qr_data'] as String? ?? '';
                      final sku = row['sku'] as String? ?? '';
                      final size = row['size'] as String? ?? '';
                      final uid = row['stock_unit_id']?.toString() ?? '';
                      return Card(
                        margin: const EdgeInsets.only(bottom: 12),
                        child: Padding(
                          padding: const EdgeInsets.all(12),
                          child: Column(
                            children: [
                              Text(
                                'parça $uid · $sku · $size',
                                style: const TextStyle(fontWeight: FontWeight.w600),
                              ),
                              const SizedBox(height: 8),
                              QrImageView(
                                data: data,
                                size: 160,
                                backgroundColor: Colors.white,
                              ),
                              SelectableText(data, style: const TextStyle(fontSize: 11)),
                            ],
                          ),
                        ),
                      );
                    }),
                  ],
                ],
              ),
            ),
    );
  }
}
