import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';

import 'screens/home_screen.dart';
import 'services/settings_service.dart';

Future<void> main() async {
  await runZonedGuarded(() async {
    WidgetsFlutterBinding.ensureInitialized();
    FlutterError.onError = (details) {
      FlutterError.presentError(details);
      if (kDebugMode) {
        debugPrint(details.summary.toString());
      }
    };

    try {
      await SettingsService.instance.load();
    } catch (e, st) {
      debugPrint('Ayarlar yüklenemedi (ilk açılışta normal olabilir): $e\n$st');
    }

    runApp(const HmaStockApp());
  }, (error, stack) {
    debugPrint('Yakalanmayan zone hatası: $error\n$stack');
  });
}

class HmaStockApp extends StatelessWidget {
  const HmaStockApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'HMA Stok',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.indigo),
        useMaterial3: true,
      ),
      home: const HomeScreen(),
    );
  }
}
