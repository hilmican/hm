import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

import 'package:hma_stock/main.dart';
import 'package:hma_stock/services/settings_service.dart';

void main() {
  testWidgets('Ana ekran yüklenir', (WidgetTester tester) async {
    TestWidgetsFlutterBinding.ensureInitialized();
    SharedPreferences.setMockInitialValues({});
    await SettingsService.instance.load();
    await tester.pumpWidget(const HmaStockApp());
    await tester.pump(const Duration(seconds: 1));
    expect(find.text('HMA Stok'), findsOneWidget);
  });
}
