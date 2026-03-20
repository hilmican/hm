import 'package:flutter/material.dart';

import '../../services/api_client.dart';

class OrderCompleteScreen extends StatefulWidget {
  const OrderCompleteScreen({
    super.key,
    required this.orderId,
    required this.trackingNo,
    this.prefillTotalAmount,
    this.prefillNotes,
  });

  final int orderId;
  final String trackingNo;
  final double? prefillTotalAmount;
  final String? prefillNotes;

  @override
  State<OrderCompleteScreen> createState() => _OrderCompleteScreenState();
}

class _OrderCompleteScreenState extends State<OrderCompleteScreen> {
  final _api = HmaApiClient();
  final _totalCtrl = TextEditingController();
  final _notesCtrl = TextEditingController();
  String _method = 'cash';
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
          const SnackBar(content: Text('Geçerli tutar girin veya boş bırakın (etiket tutarı kullanılır)')),
        );
        return;
      }
    }
    setState(() => _submitting = true);
    try {
      await _api.orderComplete(
        orderId: widget.orderId,
        totalAmount: total,
        paymentMethod: _method,
        notes: _notesCtrl.text.trim().isEmpty ? null : _notesCtrl.text.trim(),
      );
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Sipariş tamamlandı')),
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
            const SizedBox(height: 16),
            TextField(
              controller: _totalCtrl,
              keyboardType: const TextInputType.numberWithOptions(decimal: true),
              decoration: const InputDecoration(
                labelText: 'Toplam tahsilat (TL) — boşsa etiket tutarı',
                border: OutlineInputBorder(),
              ),
            ),
            const SizedBox(height: 16),
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
                  : const Text('Siparişi kaydet'),
            ),
          ],
        ),
      ),
    );
  }
}
