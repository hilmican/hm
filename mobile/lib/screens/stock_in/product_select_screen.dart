import 'package:flutter/material.dart';

import '../../services/api_client.dart';
import 'series_print_screen.dart';

class ProductSelectScreen extends StatefulWidget {
  const ProductSelectScreen({super.key});

  @override
  State<ProductSelectScreen> createState() => _ProductSelectScreenState();
}

double? _toDouble(dynamic v) {
  if (v == null) return null;
  if (v is num) return v.toDouble();
  return double.tryParse(v.toString());
}

class _ProductSelectScreenState extends State<ProductSelectScreen> {
  final _api = HmaApiClient();
  List<Map<String, dynamic>> _products = [];
  bool _loading = true;
  String? _error;

  @override
  void initState() {
    super.initState();
    _load();
  }

  Future<void> _load() async {
    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      final rows = await _api.fetchProducts();
      setState(() {
        _products = rows;
        _loading = false;
      });
    } catch (e) {
      setState(() {
        _error = e.toString();
        _loading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Ürün seç')),
      body: _loading
          ? const Center(child: CircularProgressIndicator())
          : _error != null
              ? Center(
                  child: Padding(
                    padding: const EdgeInsets.all(24),
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        Text(_error!, textAlign: TextAlign.center),
                        const SizedBox(height: 16),
                        FilledButton(onPressed: _load, child: const Text('Yenile')),
                      ],
                    ),
                  ),
                )
              : ListView.builder(
                  itemCount: _products.length,
                  itemBuilder: (context, i) {
                    final p = _products[i];
                    final id = p['id'] as int? ?? 0;
                    final name = p['name'] as String? ?? '#$id';
                    return ListTile(
                      title: Text(name),
                      subtitle: Text('id: $id'),
                      onTap: () {
                        Navigator.of(context).push(
                          MaterialPageRoute<void>(
                            builder: (_) => SeriesPrintScreen(
                              productId: id,
                              productName: name,
                              defaultPrice: _toDouble(p['default_price']),
                              defaultCost: _toDouble(p['default_cost']),
                            ),
                          ),
                        );
                      },
                    );
                  },
                ),
    );
  }
}
