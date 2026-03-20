import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';

import '../services/kargo_outbound_sync_service.dart';
import '../services/pending_kargo_store.dart';
import 'settings_screen.dart';
import 'stock_in/product_select_screen.dart';
import 'stock_out/kargo_scan_screen.dart';
import 'stock_out/pending_kargo_outbox_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> with WidgetsBindingObserver {
  int _finalizedPending = 0;
  int _drafts = 0;
  StreamSubscription<List<ConnectivityResult>>? _connSub;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    PendingKargoStore.instance.addListener(_onStore);
    _bumpCounts();
    if (!kIsWeb) {
      KargoOutboundSyncService.maybeAutoSyncAll();
      _connSub = Connectivity().onConnectivityChanged.listen((_) {
        KargoOutboundSyncService.maybeAutoSyncAll();
      });
    }
  }

  @override
  void dispose() {
    _connSub?.cancel();
    WidgetsBinding.instance.removeObserver(this);
    PendingKargoStore.instance.removeListener(_onStore);
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed && !kIsWeb) {
      KargoOutboundSyncService.maybeAutoSyncAll();
    }
  }

  void _onStore() {
    _bumpCounts();
  }

  Future<void> _bumpCounts() async {
    if (!PendingKargoStore.instance.isReady) {
      if (mounted) {
        setState(() {
          _finalizedPending = 0;
          _drafts = 0;
        });
      }
      return;
    }
    final a = await PendingKargoStore.instance.countFinalizedPending();
    final b = await PendingKargoStore.instance.countDrafts();
    if (mounted) {
      setState(() {
        _finalizedPending = a;
        _drafts = b;
      });
    }
  }

  Future<void> _openOutbox() async {
    await Navigator.of(context).push<void>(
      MaterialPageRoute(
        builder: (_) => const PendingKargoOutboxScreen(),
      ),
    );
    await _bumpCounts();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('HMA Stok'),
        actions: [
          IconButton(
            icon: const Icon(Icons.settings_outlined),
            onPressed: () {
              Navigator.of(context).push(
                MaterialPageRoute<void>(
                  builder: (_) => const SettingsScreen(),
                ),
              );
            },
            tooltip: 'API ayarları',
          ),
        ],
      ),
      body: SafeArea(
        child: Padding(
          padding: const EdgeInsets.all(24),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              if (!kIsWeb && PendingKargoStore.instance.isReady) ...[
                if (_finalizedPending > 0)
                  Material(
                    color: Theme.of(context).colorScheme.errorContainer,
                    borderRadius: BorderRadius.circular(12),
                    child: InkWell(
                      onTap: _openOutbox,
                      borderRadius: BorderRadius.circular(12),
                      child: Padding(
                        padding: const EdgeInsets.all(16),
                        child: Row(
                          children: [
                            Icon(
                              Icons.cloud_upload,
                              color: Theme.of(context).colorScheme.onErrorContainer,
                            ),
                            const SizedBox(width: 12),
                            Expanded(
                              child: Text(
                                'İnternete gönderilmeyi bekleyen $_finalizedPending sipariş var. '
                                'Dokun: manuel gönder veya listeyi aç.',
                                style: TextStyle(
                                  color: Theme.of(context).colorScheme.onErrorContainer,
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                            ),
                            Icon(
                              Icons.chevron_right,
                              color: Theme.of(context).colorScheme.onErrorContainer,
                            ),
                          ],
                        ),
                      ),
                    ),
                  ),
                if (_drafts > 0)
                  Padding(
                    padding: const EdgeInsets.only(top: 8),
                    child: OutlinedButton.icon(
                      onPressed: _openOutbox,
                      icon: const Icon(Icons.edit_note),
                      label: Text(
                        'Tamamlanmamış kargo taslağı: $_drafts · Ana sayfa',
                      ),
                    ),
                  ),
                const SizedBox(height: 16),
              ],
              const Text(
                'Stok girişi ve kargo etiketi ile satış tamamlama.',
                style: TextStyle(fontSize: 16),
              ),
              const SizedBox(height: 32),
              FilledButton.icon(
                onPressed: () {
                  Navigator.of(context).push(
                    MaterialPageRoute<void>(
                      builder: (_) => const ProductSelectScreen(),
                    ),
                  );
                },
                icon: const Icon(Icons.add_box_outlined),
                label: const Padding(
                  padding: EdgeInsets.symmetric(vertical: 12),
                  child: Text('Stok giriş — seri QR'),
                ),
              ),
              const SizedBox(height: 16),
              OutlinedButton.icon(
                onPressed: () {
                  Navigator.of(context).push(
                    MaterialPageRoute<void>(
                      builder: (_) => const KargoScanScreen(),
                    ),
                  );
                },
                icon: const Icon(Icons.local_shipping_outlined),
                label: const Padding(
                  padding: EdgeInsets.symmetric(vertical: 12),
                  child: Text('Stok çıkış — kargo + sepet'),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
