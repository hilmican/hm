import 'package:flutter/material.dart';

import '../../services/api_client.dart';

class OrderCompleteScreen extends StatefulWidget {
  const OrderCompleteScreen({
    super.key,
    required this.orderId,
    required this.trackingNo,
    this.prefillTotalAmount,
    this.prefillNotes,
    this.labelFields,
  });

  final int orderId;
  final String trackingNo;
  final double? prefillTotalAmount;
  final String? prefillNotes;
  final Map<String, dynamic>? labelFields;

  @override
  State<OrderCompleteScreen> createState() => _OrderCompleteScreenState();
}

class _OrderCompleteScreenState extends State<OrderCompleteScreen> {
  final _api = HmaApiClient();
  final _totalCtrl = TextEditingController();
  final _notesCtrl = TextEditingController();
  String _method = 'cash';
  /// Mağaza: ödeme şimdi alındı. Kapalı = kapıda ödeme (sipariş ödenmemiş placeholder).
  bool _storePaid = false;
  bool _submitting = false;

  @override
  void initState() {
    super.initState();
    final p = widget.prefillTotalAmount;
    if (p != null) {
      _totalCtrl.text = p.toStringAsFixed(2);
    }
    final n = widget.prefillNotes;
    if (n != null && n.isNotEmpty) {
      _notesCtrl.text = n;
    }
  }

  @override
  void dispose() {
    _totalCtrl.dispose();
    _notesCtrl.dispose();
    super.dispose();
  }

  Widget _labelSummary() {
    final f = widget.labelFields;
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
    if (name == null &&
        phone == null &&
        addr == null &&
        content == null &&
        codStr == null) {
      return const SizedBox.shrink();
    }
    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Etiket özeti',
              style: Theme.of(context).textTheme.titleSmall,
            ),
            if (name != null && name.isNotEmpty) Text('Alıcı: $name'),
            if (phone != null && phone.isNotEmpty) Text('Tel: $phone'),
            if (addr != null && addr.isNotEmpty) Text('Adres: $addr'),
            if (content != null && content.isNotEmpty) Text('İçerik: $content'),
            if (codStr != null) Text('Tahsilat (KOD): $codStr'),
          ],
        ),
      ),
    );
  }

  Future<void> _submit() async {
    final raw = _totalCtrl.text.replaceAll(',', '.').trim();
    double? total;
    if (raw.isEmpty) {
      total = null;
    } else {
      total = double.tryParse(raw);
      if (total == null || total < 0) {
        if (!mounted) return;
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Geçerli tutar girin veya boş bırakın (etiket tutarı kullanılır)'),
          ),
        );
        return;
      }
    }

    if (_storePaid && _method != 'cash' && _method != 'bank_transfer') {
      return;
    }

    setState(() => _submitting = true);
    try {
      await _api.orderComplete(
        orderId: widget.orderId,
        totalAmount: total,
        paymentMethod: _storePaid ? _method : null,
        notes: _notesCtrl.text.trim().isEmpty ? null : _notesCtrl.text.trim(),
        checkoutMode: _storePaid ? 'store_paid' : 'cod',
      );
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            _storePaid ? 'Sipariş tamamlandı (ödendi)' : 'Kaydedildi (kapıda ödeme — ödenmedi)',
          ),
        ),
      );
      Navigator.of(context).popUntil((r) => r.isFirst);
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Hata: $e')),
        );
        setState(() => _submitting = false);
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Tamamla')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Text('Sipariş #${widget.orderId} · ${widget.trackingNo}'),
            const SizedBox(height: 12),
            _labelSummary(),
            TextField(
              controller: _totalCtrl,
              keyboardType: const TextInputType.numberWithOptions(decimal: true),
              decoration: const InputDecoration(
                labelText: 'Toplam tahsilat (TL) — boşsa etiket tutarı',
                border: OutlineInputBorder(),
              ),
            ),
            const SizedBox(height: 12),
            SwitchListTile(
              title: const Text('Mağaza: ödeme şimdi alındı'),
              subtitle: const Text(
                'Açık değilse kapıda ödeme — sipariş ödenmemiş olarak kaydedilir (Excel kargo gibi).',
              ),
              value: _storePaid,
              onChanged: (v) => setState(() => _storePaid = v),
            ),
            if (_storePaid) ...[
              const Text('Ödeme yöntemi'),
              RadioListTile<String>(
                title: const Text('Nakit'),
                value: 'cash',
                groupValue: _method,
                onChanged: (v) => setState(() => _method = v!),
              ),
              RadioListTile<String>(
                title: const Text('Havale / IBAN'),
                value: 'bank_transfer',
                groupValue: _method,
                onChanged: (v) => setState(() => _method = v!),
              ),
            ],
            TextField(
              controller: _notesCtrl,
              maxLines: 2,
              decoration: const InputDecoration(
                labelText: 'Not (isteğe bağlı)',
                border: OutlineInputBorder(),
              ),
            ),
            const Spacer(),
            FilledButton(
              onPressed: _submitting ? null : _submit,
              child: _submitting
                  ? const SizedBox(
                      height: 22,
                      width: 22,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : Text(_storePaid ? 'Ödemeyi kaydet' : 'Kapıda ödeme — kaydet'),
            ),
          ],
        ),
      ),
    );
  }
}
