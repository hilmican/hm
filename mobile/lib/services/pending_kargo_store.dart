import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:sqflite/sqflite.dart';
import 'package:uuid/uuid.dart';

import 'kargo_tracking_hint.dart';
import 'pending_kargo_models.dart';
const _uuid = Uuid();

/// Çevrimdışı kargo satış taslakları ve gönderim kuyruğu (iOS / Android / masaüstü).
class PendingKargoStore extends ChangeNotifier {
  PendingKargoStore._();
  static final instance = PendingKargoStore._();

  Database? _db;

  bool get isReady => _db != null;

  Future<void> init() async {
    if (kIsWeb) {
      return;
    }
    if (_db != null) return;
    final dir = await getApplicationDocumentsDirectory();
    final path = p.join(dir.path, 'pending_kargo.db');
    _db = await openDatabase(
      path,
      version: 1,
      onCreate: (db, version) async {
        await db.execute('''
CREATE TABLE pending_kargo_sales (
  id TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  sync_state TEXT NOT NULL,
  last_error TEXT,
  last_attempt_at INTEGER,
  server_order_id INTEGER,
  phase TEXT NOT NULL,
  create_payload TEXT NOT NULL,
  desired_cart TEXT NOT NULL,
  complete_payload TEXT,
  finalized INTEGER NOT NULL DEFAULT 0,
  ocr_text_snapshot TEXT,
  tracking_hint TEXT
)
''');
      },
    );
    // Uygulama kapanırken kalan "syncing" kayıtları tekrar denenebilir olsun.
    await _db!.update(
      'pending_kargo_sales',
      {'sync_state': 'pending'},
      where: 'sync_state = ?',
      whereArgs: ['syncing'],
    );
  }

  Future<void> disposeDb() async {
    await _db?.close();
    _db = null;
  }

  Future<int> countFinalizedPending() async {
    final db = _db;
    if (db == null) return 0;
    final r = await db.rawQuery(
      'SELECT COUNT(*) as c FROM pending_kargo_sales WHERE finalized = 1',
    );
    return Sqflite.firstIntValue(r) ?? 0;
  }

  Future<int> countDrafts() async {
    final db = _db;
    if (db == null) return 0;
    final r = await db.rawQuery(
      'SELECT COUNT(*) as c FROM pending_kargo_sales WHERE finalized = 0',
    );
    return Sqflite.firstIntValue(r) ?? 0;
  }

  Future<List<PendingKargoSale>> listAll() async {
    final db = _db;
    if (db == null) return [];
    final rows = await db.query('pending_kargo_sales', orderBy: 'created_at ASC');
    return rows.map(PendingKargoSale.fromRow).toList();
  }

  Future<List<PendingKargoSale>> listFinalizedPending() async {
    final db = _db;
    if (db == null) return [];
    final rows = await db.query(
      'pending_kargo_sales',
      where: 'finalized = 1',
      orderBy: 'created_at ASC',
    );
    return rows.map(PendingKargoSale.fromRow).toList();
  }

  Future<PendingKargoSale?> get(String id) async {
    final db = _db;
    if (db == null) return null;
    final rows = await db.query(
      'pending_kargo_sales',
      where: 'id = ?',
      whereArgs: [id],
      limit: 1,
    );
    if (rows.isEmpty) return null;
    return PendingKargoSale.fromRow(rows.first);
  }

  Future<PendingKargoSale> getRequired(String id) async {
    final s = await get(id);
    if (s == null) throw StateError('pending sale not found: $id');
    return s;
  }

  /// Ağ yokken kargo ekranından sepete geçiş.
  Future<String> insertDraft({
    required String qrContent,
    String? ocrText,
    Map<String, dynamic>? fields,
  }) async {
    final db = _db;
    if (db == null) {
      throw StateError('Offline kuyruk bu platformda kullanılamıyor (web).');
    }
    final id = _uuid.v4();
    final now = DateTime.now().millisecondsSinceEpoch;
    final createPayload = <String, dynamic>{
      'qr_content': qrContent.trim(),
      if (ocrText != null && ocrText.isNotEmpty) 'ocr_text': ocrText,
      if (fields != null && fields.isNotEmpty) 'fields': fields,
    };
    final hint = kargoTrackingHintFromQr(qrContent) ?? kargoTrackingHintFromQr(ocrText ?? '');
    await db.insert('pending_kargo_sales', {
      'id': id,
      'created_at': now,
      'updated_at': now,
      'sync_state': 'pending',
      'last_error': null,
      'last_attempt_at': null,
      'server_order_id': null,
      'phase': 'none',
      'create_payload': jsonEncode(createPayload),
      'desired_cart': jsonEncode(<dynamic>[]),
      'complete_payload': null,
      'finalized': 0,
      'ocr_text_snapshot': ocrText,
      'tracking_hint': hint,
    });
    notifyListeners();
    return id;
  }

  Future<void> updateDesiredCart(String id, List<DesiredCartLine> lines) async {
    final db = _db;
    if (db == null) return;
    await db.update(
      'pending_kargo_sales',
      {
        'desired_cart': jsonEncode(lines.map((e) => e.toJson()).toList()),
        'updated_at': DateTime.now().millisecondsSinceEpoch,
      },
      where: 'id = ?',
      whereArgs: [id],
    );
    notifyListeners();
  }

  Future<void> setFinalized(
    String id, {
    required Map<String, dynamic> completePayload,
  }) async {
    final db = _db;
    if (db == null) return;
    await db.update(
      'pending_kargo_sales',
      {
        'complete_payload': jsonEncode(completePayload),
        'finalized': 1,
        'sync_state': 'pending',
        'updated_at': DateTime.now().millisecondsSinceEpoch,
      },
      where: 'id = ?',
      whereArgs: [id],
    );
    notifyListeners();
  }

  Future<void> patch(
    String id, {
    String? syncState,
    String? lastError,
    DateTime? lastAttemptAt,
    int? serverOrderId,
    String? phase,
    bool clearLastError = false,
  }) async {
    final db = _db;
    if (db == null) return;
    final map = <String, Object?>{
      'updated_at': DateTime.now().millisecondsSinceEpoch,
    };
    if (syncState != null) map['sync_state'] = syncState;
    if (clearLastError || lastError != null) {
      map['last_error'] = clearLastError ? null : lastError;
    }
    if (lastAttemptAt != null) {
      map['last_attempt_at'] = lastAttemptAt.millisecondsSinceEpoch;
    }
    if (serverOrderId != null) map['server_order_id'] = serverOrderId;
    if (phase != null) map['phase'] = phase;
    await db.update(
      'pending_kargo_sales',
      map,
      where: 'id = ?',
      whereArgs: [id],
    );
    notifyListeners();
  }

  Future<void> delete(String id) async {
    final db = _db;
    if (db == null) return;
    await db.delete('pending_kargo_sales', where: 'id = ?', whereArgs: [id]);
    notifyListeners();
  }
}
