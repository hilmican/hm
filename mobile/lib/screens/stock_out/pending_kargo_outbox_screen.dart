import 'package:flutter/material.dart';

import '../../services/kargo_outbound_sync_service.dart';
import '../../services/pending_kargo_models.dart';
import '../../services/pending_kargo_store.dart';
import 'cart_scan_screen.dart';

/// Bekleyen kargo gönderimleri: manuel senkron ve hata gösterimi.
class PendingKargoOutboxScreen extends StatefulWidget {
  const PendingKargoOutboxScreen({super.key});

  @override
  State<PendingKargoOutboxScreen> createState() => _PendingKargoOutboxScreenState();
}

class _PendingKargoOutboxScreenState extends State<PendingKargoOutboxScreen> {
  bool _busy = false;
  List<PendingKargoSale>? _list;

  @override
  void initState() {
    super.initState();
    PendingKargoStore.instance.addListener(_onStore);
    _refresh();
  }

  @override
  void dispose() {
    PendingKargoStore.instance.removeListener(_onStore);
    super.dispose();
  }

  void _onStore() {
    _refresh();
  }

  Future<void> _refresh() async {
    final l = await PendingKargoStore.instance.listAll();
    if (mounted) setState(() => _list = l);
  }

  Future<void> _runSync(PendingKargoSale sale) async {
    if (!sale.finalized) return;
    setState(() => _busy = true);
    try {
      await KargoOutboundSyncService().syncOne(sale.id);
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Gönderildi')),
        );
      }
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('Hata: $e')),
        );
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _syncAll() async {
    setState(() => _busy = true);
    try {
      await KargoOutboundSyncService().syncAllFinalized();
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Kuyruk işlendi')),
        );
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _continueDraft(PendingKargoSale sale) async {
    if (sale.finalized) return;
    final track = sale.trackingHint ?? '—';
    await Navigator.of(context).push<void>(
      MaterialPageRoute(
        builder: (_) => CartScanScreen(
          localPendingId: sale.id,
          trackingNo: track.length > 40 ? '${track.substring(0, 40)}…' : track,
          labelFields: sale.labelFieldsForOfflineCard(),
        ),
      ),
    );
  }

  Future<void> _deleteDraft(PendingKargoSale sale) async {
    final ok = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Taslağı sil'),
        content: const Text('Tamamlanmamış yerel kayıt silinsin mi?'),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx, false), child: const Text('İptal')),
          FilledButton(onPressed: () => Navigator.pop(ctx, true), child: const Text('Sil')),
        ],
      ),
    );
    if (ok != true) return;
    await PendingKargoStore.instance.delete(sale.id);
  }

  @override
  Widget build(BuildContext context) {
    final list = _list;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Gönderim bekleyenler'),
        actions: [
          if (!_busy)
            TextButton(
              onPressed: _syncAll,
              child: const Text('Tümünü gönder'),
            ),
        ],
      ),
      body: _busy
          ? const Center(child: CircularProgressIndicator())
          : list == null
              ? const Center(child: CircularProgressIndicator())
              : list.isEmpty
                  ? const Center(child: Text('Bekleyen kayıt yok'))
                  : ListView.separated(
                      padding: const EdgeInsets.all(16),
                      itemCount: list.length,
                      separatorBuilder: (_, __) => const SizedBox(height: 8),
                      itemBuilder: (context, i) {
                        final s = list[i];
                        final track = s.trackingHint ?? '—';
                        final buf = StringBuffer()
                          ..write(
                            s.finalized ? 'Gönderim bekliyor' : 'Taslak (tamamlanmadı)',
                          )
                          ..write(' · ')
                          ..write(s.itemUnitCount)
                          ..write(' birim');
                        if (s.serverOrderId != null) {
                          buf.write(' · srv #${s.serverOrderId}');
                        }
                        if (s.lastError != null && s.lastError!.isNotEmpty) {
                          buf.write('\n${s.lastError}');
                        }
                        return Card(
                          child: ListTile(
                            onTap: !s.finalized ? () => _continueDraft(s) : null,
                            title: Text('Takip: $track'),
                            subtitle: Text(buf.toString()),
                            isThreeLine: true,
                            trailing: Row(
                              mainAxisSize: MainAxisSize.min,
                              children: [
                                if (!s.finalized)
                                  IconButton(
                                    tooltip: 'Taslağı sil',
                                    onPressed: () => _deleteDraft(s),
                                    icon: const Icon(Icons.delete_outline),
                                  ),
                                if (s.finalized)
                                  FilledButton(
                                    onPressed:
                                        s.syncState == 'syncing' ? null : () => _runSync(s),
                                    child: const Text('Gönder'),
                                  ),
                              ],
                            ),
                          ),
                        );
                      },
                    ),
    );
  }
}
