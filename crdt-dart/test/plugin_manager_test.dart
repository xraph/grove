import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

ChangeRecord makeChange({String table = 't'}) => ChangeRecord(
  table: table,
  pk: '1',
  field: 'f',
  crdtType: CrdtType.lww,
  hlc: HLC(BigInt.from(100), 0, 'n'),
  nodeId: 'n',
  value: const JsonValue('v'),
);

WriteEvent makeWriteEvent({Object? value = 'v'}) => WriteEvent(
  table: 't',
  pk: '1',
  field: 'f',
  crdtType: CrdtType.lww,
  value: value,
  change: makeChange(),
  previousState: null,
);

WriteEvent withValue(WriteEvent e, Object? value) => WriteEvent(
  table: e.table,
  pk: e.pk,
  field: e.field,
  crdtType: e.crdtType,
  value: value,
  change: e.change,
  previousState: e.previousState,
);

MergeEvent makeMergeEvent({FieldState? local, bool conflictDetected = false}) =>
    MergeEvent(
      table: 't',
      pk: '1',
      field: 'f',
      local: local,
      remote: makeChange(),
      conflictDetected: conflictDetected,
    );

DocumentState makeDocState() => DocumentState(table: 't', pk: '1');

/// A plugin whose hooks are closures, so a test sets only the ones it needs.
/// Unset hooks keep the base class pass-through.
final class _Plugin extends StorePlugin {
  _Plugin(
    this.name, {
    this.onInit,
    this.onDestroy,
    this.onBeforeWrite,
    this.onAfterWrite,
    this.onBeforeMerge,
    this.onAfterMerge,
    this.onBeforePull,
    this.onAfterPull,
    this.onBeforePush,
    this.onAfterPush,
    this.onTransformDocument,
    this.onTransformCollection,
    this.onBeforePresenceUpdate,
    this.onOnPresenceEvent,
    this.onBeforePersist,
    this.onAfterHydrate,
  });

  @override
  final String name;

  final void Function()? onInit;
  final void Function()? onDestroy;
  final WriteEvent? Function(WriteEvent e)? onBeforeWrite;
  final void Function(WriteEvent e)? onAfterWrite;
  final ChangeRecord? Function(MergeEvent e)? onBeforeMerge;
  final void Function(MergeEvent e)? onAfterMerge;
  final PullEvent? Function(PullEvent e)? onBeforePull;
  final void Function(PullEvent e, List<ChangeRecord> changes)? onAfterPull;
  final List<ChangeRecord>? Function(List<ChangeRecord> changes)? onBeforePush;
  final void Function(int pushed, List<ChangeRecord> changes)? onAfterPush;
  final Map<String, Object?>? Function(
    String table,
    String pk,
    Map<String, Object?> doc,
  )?
  onTransformDocument;
  final List<Map<String, Object?>> Function(
    String table,
    List<Map<String, Object?>> docs,
  )?
  onTransformCollection;
  final Object? Function(String topic, Object? data)? onBeforePresenceUpdate;
  final void Function(PresenceEvent e)? onOnPresenceEvent;
  final DocumentState Function(String table, String pk, DocumentState doc)?
  onBeforePersist;
  final DocumentState Function(String table, String pk, DocumentState doc)?
  onAfterHydrate;

  @override
  void init() => onInit != null ? onInit!() : super.init();

  @override
  void destroy() => onDestroy != null ? onDestroy!() : super.destroy();

  @override
  WriteEvent? beforeWrite(WriteEvent e) =>
      onBeforeWrite != null ? onBeforeWrite!(e) : super.beforeWrite(e);

  @override
  void afterWrite(WriteEvent e) =>
      onAfterWrite != null ? onAfterWrite!(e) : super.afterWrite(e);

  @override
  ChangeRecord? beforeMerge(MergeEvent e) =>
      onBeforeMerge != null ? onBeforeMerge!(e) : super.beforeMerge(e);

  @override
  void afterMerge(MergeEvent e) =>
      onAfterMerge != null ? onAfterMerge!(e) : super.afterMerge(e);

  @override
  PullEvent? beforePull(PullEvent e) =>
      onBeforePull != null ? onBeforePull!(e) : super.beforePull(e);

  @override
  void afterPull(PullEvent e, List<ChangeRecord> changes) => onAfterPull != null
      ? onAfterPull!(e, changes)
      : super.afterPull(e, changes);

  @override
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) =>
      onBeforePush != null ? onBeforePush!(changes) : super.beforePush(changes);

  @override
  void afterPush(int pushed, List<ChangeRecord> changes) => onAfterPush != null
      ? onAfterPush!(pushed, changes)
      : super.afterPush(pushed, changes);

  @override
  Map<String, Object?>? transformDocument(
    String table,
    String pk,
    Map<String, Object?> doc,
  ) => onTransformDocument != null
      ? onTransformDocument!(table, pk, doc)
      : super.transformDocument(table, pk, doc);

  @override
  List<Map<String, Object?>> transformCollection(
    String table,
    List<Map<String, Object?>> docs,
  ) => onTransformCollection != null
      ? onTransformCollection!(table, docs)
      : super.transformCollection(table, docs);

  @override
  Object? beforePresenceUpdate(String topic, Object? data) =>
      onBeforePresenceUpdate != null
      ? onBeforePresenceUpdate!(topic, data)
      : super.beforePresenceUpdate(topic, data);

  @override
  void onPresenceEvent(PresenceEvent e) => onOnPresenceEvent != null
      ? onOnPresenceEvent!(e)
      : super.onPresenceEvent(e);

  @override
  DocumentState beforePersist(String table, String pk, DocumentState doc) =>
      onBeforePersist != null
      ? onBeforePersist!(table, pk, doc)
      : super.beforePersist(table, pk, doc);

  @override
  DocumentState afterHydrate(String table, String pk, DocumentState doc) =>
      onAfterHydrate != null
      ? onAfterHydrate!(table, pk, doc)
      : super.afterHydrate(table, pk, doc);
}

final class _Throwing extends StorePlugin {
  @override
  String get name => 'throwing';

  @override
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) =>
      throw StateError('boom');
}

/// Throws from every hook, init and destroy.
final class _ThrowsEverywhere extends StorePlugin {
  @override
  String get name => 'bad';

  @override
  void init() => throw StateError('init');

  @override
  void destroy() => throw StateError('destroy');

  @override
  WriteEvent? beforeWrite(WriteEvent e) => throw StateError('beforeWrite');

  @override
  void afterWrite(WriteEvent e) => throw StateError('afterWrite');

  @override
  ChangeRecord? beforeMerge(MergeEvent e) => throw StateError('beforeMerge');

  @override
  void afterMerge(MergeEvent e) => throw StateError('afterMerge');

  @override
  PullEvent? beforePull(PullEvent e) => throw StateError('beforePull');

  @override
  void afterPull(PullEvent e, List<ChangeRecord> changes) =>
      throw StateError('afterPull');

  @override
  List<ChangeRecord>? beforePush(List<ChangeRecord> changes) =>
      throw StateError('beforePush');

  @override
  void afterPush(int pushed, List<ChangeRecord> changes) =>
      throw StateError('afterPush');

  @override
  Map<String, Object?>? transformDocument(
    String table,
    String pk,
    Map<String, Object?> doc,
  ) => throw StateError('transformDocument');

  @override
  List<Map<String, Object?>> transformCollection(
    String table,
    List<Map<String, Object?>> docs,
  ) => throw StateError('transformCollection');

  @override
  Object? beforePresenceUpdate(String topic, Object? data) =>
      throw StateError('beforePresenceUpdate');

  @override
  void onPresenceEvent(PresenceEvent e) => throw StateError('onPresenceEvent');

  @override
  DocumentState beforePersist(String table, String pk, DocumentState doc) =>
      throw StateError('beforePersist');

  @override
  DocumentState afterHydrate(String table, String pk, DocumentState doc) =>
      throw StateError('afterHydrate');
}

void main() {
  group('PluginManager', () {
    test('registers and retrieves plugins by name', () {
      final pm = PluginManager();
      final plugin = _Plugin('test');
      pm.use(plugin);
      expect(pm.get<StorePlugin>('test'), same(plugin));
    });

    test('removes plugins by name and calls destroy', () {
      final pm = PluginManager();
      var destroyed = 0;
      final plugin = _Plugin('test', onDestroy: () => destroyed++);
      pm.use(plugin);

      pm.remove('test');

      expect(destroyed, 1);
      expect(pm.get<StorePlugin>('test'), isNull);
    });

    test('calls init on registration', () {
      final pm = PluginManager();
      var inits = 0;
      final plugin = _Plugin('test', onInit: () => inits++);
      pm.use(plugin);

      expect(inits, 1);
    });

    group('WriteHook', () {
      test('calls beforeWrite on registered plugins', () {
        final pm = PluginManager();
        final calls = <WriteEvent>[];
        pm.use(
          _Plugin(
            'w',
            onBeforeWrite: (e) {
              calls.add(e);
              return e;
            },
          ),
        );

        final event = makeWriteEvent();
        pm.dispatchBeforeWrite(event);

        expect(calls, hasLength(1));
        expect(calls.single, same(event));
      });

      test('allows plugins to transform write events', () {
        final pm = PluginManager();
        pm.use(_Plugin('w', onBeforeWrite: (e) => withValue(e, 'transformed')));

        final result = pm.dispatchBeforeWrite(makeWriteEvent());
        expect(result, isNotNull);
        expect(result!.value, 'transformed');
      });

      test('allows plugins to reject writes by returning null', () {
        final pm = PluginManager();
        pm.use(_Plugin('w', onBeforeWrite: (_) => null));

        final result = pm.dispatchBeforeWrite(makeWriteEvent());
        expect(result, isNull);
      });

      test('calls afterWrite after successful writes', () {
        final pm = PluginManager();
        final calls = <WriteEvent>[];
        pm.use(_Plugin('w', onAfterWrite: calls.add));

        final event = makeWriteEvent();
        pm.dispatchAfterWrite(event);

        expect(calls, hasLength(1));
        expect(calls.single, same(event));
      });

      test('chains multiple write hooks in order', () {
        final pm = PluginManager();
        final order = <int>[];

        pm.use(
          _Plugin(
            'w1',
            onBeforeWrite: (e) {
              order.add(1);
              return withValue(e, 'from-1');
            },
          ),
        );
        pm.use(
          _Plugin(
            'w2',
            onBeforeWrite: (e) {
              order.add(2);
              return withValue(e, 'from-2');
            },
          ),
        );

        final result = pm.dispatchBeforeWrite(makeWriteEvent());

        expect(order, [1, 2]);
        expect(result!.value, 'from-2');
      });
    });

    group('MergeHook', () {
      test('calls beforeMerge on incoming changes', () {
        final pm = PluginManager();
        var calls = 0;
        pm.use(
          _Plugin(
            'm',
            onBeforeMerge: (e) {
              calls++;
              return e.remote;
            },
          ),
        );

        pm.dispatchBeforeMerge(makeMergeEvent());

        expect(calls, 1);
      });

      test('allows plugins to reject changes by returning null', () {
        final pm = PluginManager();
        pm.use(_Plugin('m', onBeforeMerge: (_) => null));

        final result = pm.dispatchBeforeMerge(makeMergeEvent());
        expect(result, isNull);
      });

      test('calls afterMerge with conflict info', () {
        final pm = PluginManager();
        final calls = <MergeEvent>[];
        pm.use(_Plugin('m', onAfterMerge: calls.add));

        final event = makeMergeEvent(conflictDetected: true);
        pm.dispatchAfterMerge(event);

        expect(calls, hasLength(1));
        expect(calls.single.conflictDetected, isTrue);
      });

      test('sets conflictDetected when local state exists', () {
        final pm = PluginManager();
        final calls = <MergeEvent>[];
        pm.use(_Plugin('m', onAfterMerge: calls.add));

        final localFs = FieldState(
          type: CrdtType.lww,
          hlc: HLC(BigInt.from(50), 0, 'a'),
          nodeId: 'a',
          value: const JsonValue('old'),
        );
        final event = makeMergeEvent(local: localFs, conflictDetected: true);
        pm.dispatchAfterMerge(event);

        expect(calls.single.conflictDetected, isTrue);
        expect(calls.single.local, same(localFs));
      });
    });

    group('ReadHook', () {
      test('transforms documents via transformDocument', () {
        final pm = PluginManager();
        pm.use(
          _Plugin(
            'r',
            onTransformDocument: (table, pk, doc) => {...doc, 'extra': true},
          ),
        );

        final result = pm.dispatchTransformDocument('t', '1', {
          'name': 'Alice',
        });
        expect(result, {'name': 'Alice', 'extra': true});
      });

      test('filters documents returning null', () {
        final pm = PluginManager();
        pm.use(_Plugin('r', onTransformDocument: (table, pk, doc) => null));

        final result = pm.dispatchTransformDocument('t', '1', {
          'name': 'Alice',
        });
        expect(result, isNull);
      });

      test('transforms collections via transformCollection', () {
        final pm = PluginManager();
        pm.use(
          _Plugin(
            'r',
            onTransformCollection: (table, docs) => docs.sublist(0, 1),
          ),
        );

        final docs = <Map<String, Object?>>[
          {'id': 1},
          {'id': 2},
          {'id': 3},
        ];
        final result = pm.dispatchTransformCollection('t', docs);
        expect(result, hasLength(1));
        expect(result[0], {'id': 1});
      });
    });

    group('SyncHook', () {
      test('calls beforePull and afterPull', () {
        final pm = PluginManager();
        var before = 0;
        var after = 0;
        pm.use(
          _Plugin(
            's',
            onBeforePull: (e) {
              before++;
              return e;
            },
            onAfterPull: (e, changes) => after++,
          ),
        );

        const pullEvent = PullEvent(tables: ['users']);
        pm.dispatchBeforePull(pullEvent);
        expect(before, 1);

        pm.dispatchAfterPull(pullEvent, []);
        expect(after, 1);
      });

      test('calls beforePush and afterPush', () {
        final pm = PluginManager();
        var before = 0;
        var after = 0;
        pm.use(
          _Plugin(
            's',
            onBeforePush: (changes) {
              before++;
              return changes;
            },
            onAfterPush: (pushed, changes) => after++,
          ),
        );

        final changes = [makeChange()];
        pm.dispatchBeforePush(changes);
        expect(before, 1);

        pm.dispatchAfterPush(1, changes);
        expect(after, 1);
      });

      test('allows plugins to cancel pull by returning null', () {
        final pm = PluginManager();
        pm.use(_Plugin('s', onBeforePull: (_) => null));

        final result = pm.dispatchBeforePull(
          const PullEvent(tables: ['users']),
        );
        expect(result, isNull);
      });

      test('allows plugins to filter push changes', () {
        // The dispatch passes a plugin's filtered list through, as crdt-js
        // does. The class docs on StorePlugin.beforePush say a plugin must not
        // do it, because the sync engine would drop the withheld records.
        final pm = PluginManager();
        pm.use(
          _Plugin(
            's',
            onBeforePush: (changes) => [
              for (final c in changes)
                if (c.table != 'secret') c,
            ],
          ),
        );

        final changes = [
          makeChange(table: 'users'),
          makeChange(table: 'secret'),
        ];
        final result = pm.dispatchBeforePush(changes);
        expect(result, hasLength(1));
        expect(result![0].table, 'users');
      });
    });

    group('PresenceHook', () {
      test('calls beforePresenceUpdate', () {
        final pm = PluginManager();
        final calls = <(String, Object?)>[];
        pm.use(
          _Plugin(
            'p',
            onBeforePresenceUpdate: (topic, data) {
              calls.add((topic, data));
              return data;
            },
          ),
        );

        pm.dispatchBeforePresenceUpdate('room', {
          'cursor': {'x': 10, 'y': 20},
        });
        expect(calls, hasLength(1));
        expect(calls.single.$1, 'room');
        expect(calls.single.$2, {
          'cursor': {'x': 10, 'y': 20},
        });
      });

      test('calls onPresenceEvent', () {
        final pm = PluginManager();
        final calls = <PresenceEvent>[];
        pm.use(_Plugin('p', onOnPresenceEvent: calls.add));

        const event = PresenceEvent(type: 'join', nodeId: 'n1', topic: 'room');
        pm.dispatchOnPresenceEvent(event);
        expect(calls, hasLength(1));
        expect(calls.single, same(event));
      });

      test('allows plugins to reject presence updates', () {
        final pm = PluginManager();
        // Differs from crdt-js, which rejects with null: Dart uses presenceRejected,
        // because null is also a legitimate leave payload.
        pm.use(
          _Plugin(
            'p',
            onBeforePresenceUpdate: (topic, data) => presenceRejected,
          ),
        );

        final result = pm.dispatchBeforePresenceUpdate('room', {
          'cursor': {'x': 0, 'y': 0},
        });
        expect(result, same(presenceRejected));
      });
    });

    group('StorageHook', () {
      test('transforms documents before persist', () {
        final pm = PluginManager();
        pm.use(
          _Plugin(
            'st',
            onBeforePersist: (table, pk, doc) => doc.copyWith(tombstone: true),
          ),
        ); // e.g. encrypt

        final doc = makeDocState();
        final result = pm.dispatchBeforePersist('t', '1', doc);
        expect(result.tombstone, isTrue);
      });

      test('transforms documents after hydrate', () {
        final pm = PluginManager();
        pm.use(
          _Plugin(
            'st',
            onAfterHydrate: (table, pk, doc) {
              return doc.copyWith(
                fields: {
                  ...doc.fields,
                  'injected': FieldState(
                    type: CrdtType.lww,
                    hlc: HLC(BigInt.one, 0, 'n'),
                    nodeId: 'n',
                    value: const JsonValue('hydrated'),
                  ),
                },
              );
            },
          ),
        );

        final doc = makeDocState();
        final result = pm.dispatchAfterHydrate('t', '1', doc);
        expect(result.fields['injected'], isNotNull);
        expect(result.fields['injected']!.value, const JsonValue('hydrated'));
      });
    });
  });

  test('a throwing hook is reported and fails closed', () {
    // Differs from crdt-js (and from the first draft of this case, which
    // expected a pass-through): a beforePush that throws cancels the push.
    final errors = <Object>[];
    final m = PluginManager(onPluginError: (e, name) => errors.add(e));
    m.use(_Throwing());
    final c = ChangeRecord(
      table: 't',
      pk: '1',
      field: 'f',
      crdtType: CrdtType.lww,
      hlc: HLC.zero,
      nodeId: 'a',
    );
    expect(m.dispatchBeforePush([c]), isNull);
    expect(errors, hasLength(1));
  });

  group('dispatch semantics beyond the crdt-js cases', () {
    test('the first null from a before hook stops the later plugins', () {
      final pm = PluginManager();
      var later = 0;
      pm.use(
        _Plugin(
          'a',
          onBeforeWrite: (_) => null,
          onBeforePull: (_) => null,
          onBeforePush: (_) => null,
        ),
      );
      pm.use(
        _Plugin(
          'b',
          onBeforeWrite: (e) {
            later++;
            return e;
          },
          onBeforePull: (e) {
            later++;
            return e;
          },
          onBeforePush: (c) {
            later++;
            return c;
          },
        ),
      );
      expect(pm.dispatchBeforeWrite(makeWriteEvent()), isNull);
      expect(pm.dispatchBeforePull(const PullEvent(tables: [])), isNull);
      expect(pm.dispatchBeforePush([makeChange()]), isNull);
      expect(later, 0);
    });

    test('after hooks all run, in registration order', () {
      final pm = PluginManager();
      final order = <String>[];
      pm.use(_Plugin('a', onAfterWrite: (_) => order.add('a')));
      pm.use(_Plugin('b', onAfterWrite: (_) => order.add('b')));
      pm.dispatchAfterWrite(makeWriteEvent());
      expect(order, ['a', 'b']);
    });

    test('beforeMerge sees the change the previous hook returned', () {
      final pm = PluginManager();
      final seen = <String>[];
      pm.use(
        _Plugin(
          'a',
          onBeforeMerge: (e) {
            seen.add(e.remote.table);
            return e.remote.copyWith(table: 'rewritten');
          },
        ),
      );
      pm.use(
        _Plugin(
          'b',
          onBeforeMerge: (e) {
            seen.add(e.remote.table);
            return e.remote;
          },
        ),
      );
      final event = makeMergeEvent();
      final result = pm.dispatchBeforeMerge(event);
      expect(seen, ['t', 'rewritten']);
      expect(result!.table, 'rewritten');
      expect(event.remote.table, 'rewritten');
    });

    test('the store can fill result and winnerNodeId before afterMerge', () {
      final pm = PluginManager();
      MergeEvent? seen;
      pm.use(_Plugin('m', onAfterMerge: (e) => seen = e));
      final event = makeMergeEvent();
      event
        ..result = FieldState(type: CrdtType.lww, hlc: HLC.zero, nodeId: 'w')
        ..winnerNodeId = 'w';
      pm.dispatchAfterMerge(event);
      expect(seen!.winnerNodeId, 'w');
      expect(seen!.result!.nodeId, 'w');
    });

    test('afterPull and afterPush receive the changes', () {
      final pm = PluginManager();
      List<ChangeRecord>? pulled;
      (int, List<ChangeRecord>)? pushed;
      pm.use(
        _Plugin(
          's',
          onAfterPull: (e, changes) => pulled = changes,
          onAfterPush: (n, changes) => pushed = (n, changes),
        ),
      );
      final changes = [makeChange()];
      pm.dispatchAfterPull(const PullEvent(tables: []), changes);
      pm.dispatchAfterPush(1, changes);
      expect(pulled, same(changes));
      expect(pushed!.$1, 1);
      expect(pushed!.$2, same(changes));
    });

    test(
      'a null transformDocument stops the chain and read hooks chain otherwise',
      () {
        final pm = PluginManager();
        var later = 0;
        pm.use(_Plugin('a', onTransformDocument: (t, pk, d) => {...d, 'a': 1}));
        pm.use(
          _Plugin(
            'b',
            onTransformDocument: (t, pk, d) {
              expect(d['a'], 1);
              return null;
            },
          ),
        );
        pm.use(
          _Plugin(
            'c',
            onTransformDocument: (t, pk, d) {
              later++;
              return d;
            },
          ),
        );
        expect(pm.dispatchTransformDocument('t', '1', {}), isNull);
        expect(later, 0);
      },
    );

    test('transformCollection chains each hook over the previous result', () {
      final pm = PluginManager();
      pm.use(_Plugin('a', onTransformCollection: (t, docs) => docs.sublist(1)));
      pm.use(_Plugin('b', onTransformCollection: (t, docs) => docs.sublist(1)));
      final result = pm.dispatchTransformCollection('t', [
        {'id': 1},
        {'id': 2},
        {'id': 3},
      ]);
      expect(result, [
        {'id': 3},
      ]);
    });

    test('persist and hydrate hooks chain', () {
      final pm = PluginManager();
      pm.use(
        _Plugin('a', onBeforePersist: (t, pk, d) => d.copyWith(pk: '${d.pk}a')),
      );
      pm.use(
        _Plugin('b', onBeforePersist: (t, pk, d) => d.copyWith(pk: '${d.pk}b')),
      );
      expect(pm.dispatchBeforePersist('t', '1', makeDocState()).pk, '1ab');
    });

    test('presence: null is a payload, not a cancel, and the sentinel stops the chain', () {
      final pm = PluginManager();
      final seen = <Object?>[];
      pm.use(
        _Plugin(
          'a',
          onBeforePresenceUpdate: (t, d) {
            seen.add(d);
            return null;
          },
        ),
      );
      pm.use(
        _Plugin(
          'b',
          onBeforePresenceUpdate: (t, d) {
            seen.add(d);
            return d;
          },
        ),
      );
      expect(pm.dispatchBeforePresenceUpdate('room', {'x': 1}), isNull);
      expect(seen, [
        {'x': 1},
        null,
      ]);

      var later = 0;
      final pm2 = PluginManager();
      pm2.use(_Plugin('a', onBeforePresenceUpdate: (t, d) => presenceRejected));
      pm2.use(
        _Plugin(
          'b',
          onBeforePresenceUpdate: (t, d) {
            later++;
            return d;
          },
        ),
      );
      expect(
        pm2.dispatchBeforePresenceUpdate('room', 1),
        same(presenceRejected),
      );
      expect(later, 0);
    });

    test(
      'presence: a null payload passes through a manager with no plugins',
      () {
        expect(
          PluginManager().dispatchBeforePresenceUpdate('room', null),
          isNull,
        );
      },
    );

    test('a plugin that overrides nothing passes everything through', () {
      final pm = PluginManager();
      pm.use(_Plugin('plain'));
      final w = makeWriteEvent();
      expect(pm.dispatchBeforeWrite(w), same(w));
      final merge = makeMergeEvent();
      expect(pm.dispatchBeforeMerge(merge), same(merge.remote));
      const pull = PullEvent(tables: ['t']);
      expect(pm.dispatchBeforePull(pull), same(pull));
      final changes = [makeChange()];
      expect(pm.dispatchBeforePush(changes), same(changes));
      final doc = <String, Object?>{'a': 1};
      expect(pm.dispatchTransformDocument('t', '1', doc), same(doc));
      final docs = <Map<String, Object?>>[doc];
      expect(pm.dispatchTransformCollection('t', docs), same(docs));
      expect(pm.dispatchBeforePresenceUpdate('r', 7), 7);
      final state = makeDocState();
      expect(pm.dispatchBeforePersist('t', '1', state), same(state));
      expect(pm.dispatchAfterHydrate('t', '1', state), same(state));
    });
  });

  group('plugin errors', () {
    late List<(String, String)> errors;
    late PluginManager pm;

    setUp(() {
      errors = [];
      pm = PluginManager(onPluginError: (e, name) => errors.add((name, '$e')));
      pm.use(_ThrowsEverywhere());
      errors.clear(); // the init error is covered below
    });

    test('hooks that can reject or hide data fail closed and are reported with the plugin name', () {
      expect(pm.dispatchBeforeWrite(makeWriteEvent()), isNull);
      expect(pm.dispatchBeforeMerge(makeMergeEvent()), isNull);
      expect(pm.dispatchBeforePull(const PullEvent(tables: [])), isNull);
      expect(pm.dispatchBeforePush([makeChange()]), isNull);
      expect(pm.dispatchTransformDocument('t', '1', {'secret': 1}), isNull);
      expect(
        pm.dispatchTransformCollection('t', [
          {'secret': 1},
        ]),
        isEmpty,
      );
      expect(pm.dispatchBeforePresenceUpdate('r', 5), same(presenceRejected));

      expect(errors.map((e) => e.$1).toSet(), {'bad'});
      expect(errors.map((e) => e.$2), [
        for (final hook in [
          'beforeWrite',
          'beforeMerge',
          'beforePull',
          'beforePush',
          'transformDocument',
          'transformCollection',
          'beforePresenceUpdate',
        ])
          'Bad state: $hook',
      ]);
    });

    test('a throwing encryptor rethrows after reporting, so the caller persists nothing', () {
      final doc = makeDocState();
      expect(
        () => pm.dispatchBeforePersist('t', '1', doc),
        throwsA(isA<StateError>()),
      );
      expect(errors.single, ('bad', 'Bad state: beforePersist'));
    });

    test('a throwing decryptor rethrows after reporting, so nothing undecrypted is served', () {
      final doc = makeDocState();
      expect(
        () => pm.dispatchAfterHydrate('t', '1', doc),
        throwsA(isA<StateError>()),
      );
      expect(errors.single, ('bad', 'Bad state: afterHydrate'));
    });

    test('a throwing encryptor stops the chain: the later plugin never sees the document', () {
      var later = 0;
      pm.use(
        _Plugin(
          'after',
          onBeforePersist: (t, pk, d) {
            later++;
            return d;
          },
        ),
      );
      expect(
        () => pm.dispatchBeforePersist('t', '1', makeDocState()),
        throwsStateError,
      );
      expect(later, 0);
    });

    test('notification hooks are reported and the other plugins still run', () {
      final order = <String>[];
      pm.use(_Plugin('c', onAfterWrite: (_) => order.add('c')));
      pm.use(_Plugin('d', onAfterWrite: (_) => order.add('d')));
      final w = makeWriteEvent();
      pm.dispatchAfterWrite(w);
      final merge = makeMergeEvent();
      pm.dispatchAfterMerge(merge);
      pm.dispatchAfterPull(const PullEvent(tables: []), []);
      pm.dispatchAfterPush(1, [makeChange()]);
      pm.dispatchOnPresenceEvent(
        const PresenceEvent(type: 'join', nodeId: 'n', topic: 'r'),
      );
      expect(order, ['c', 'd']);
      expect(errors.map((e) => e.$2), [
        for (final hook in [
          'afterWrite',
          'afterMerge',
          'afterPull',
          'afterPush',
          'onPresenceEvent',
        ])
          'Bad state: $hook',
      ]);
    });

    test('a failing validator means a later plugin never sees the write', () {
      var later = 0;
      pm.use(
        _Plugin(
          'b',
          onBeforeWrite: (e) {
            later++;
            return e;
          },
        ),
      );
      expect(pm.dispatchBeforeWrite(makeWriteEvent()), isNull);
      expect(later, 0);
    });

    test('a throw after earlier plugins ran cancels the whole chain', () {
      final pm2 = PluginManager(onPluginError: (e, name) {});
      pm2.use(_Plugin('a', onBeforeWrite: (e) => withValue(e, 'from-a')));
      pm2.use(_ThrowsEverywhere());
      pm2.use(_Plugin('c', onBeforeWrite: (e) => withValue(e, '${e.value}+c')));
      expect(pm2.dispatchBeforeWrite(makeWriteEvent()), isNull);
    });

    test('without a handler errors are still handled by the same policy', () {
      final quiet = PluginManager();
      quiet.use(_ThrowsEverywhere());
      expect(quiet.dispatchBeforePush([makeChange()]), isNull);
      expect(
        () => quiet.dispatchBeforePersist('t', '1', makeDocState()),
        throwsStateError,
      );
      quiet.remove('bad');
    });

    test('a handler that throws does not break the policy', () {
      final loud = PluginManager(
        onPluginError: (e, name) => throw StateError('handler'),
      );
      loud.use(_Throwing());
      expect(loud.dispatchBeforePush([makeChange()]), isNull);
      loud.use(_ThrowsEverywhere());
      expect(
        () => loud.dispatchBeforePersist('t', '1', makeDocState()),
        throwsA(predicate((e) => '$e' == 'Bad state: beforePersist')),
      );
    });

    test('an init that throws is reported and the plugin stays registered', () {
      final errs = <String>[];
      final pm2 = PluginManager(onPluginError: (e, name) => errs.add(name));
      final p = _ThrowsEverywhere();
      pm2.use(p);
      expect(errs, ['bad']);
      expect(pm2.get<StorePlugin>('bad'), same(p));
    });

    test(
      'a destroy that throws is reported and the plugin is still removed',
      () {
        pm.remove('bad');
        expect(errors.single.$1, 'bad');
        expect(pm.get<StorePlugin>('bad'), isNull);
      },
    );

    test('destroy calls every plugin, survives one that throws, and empties the manager', () {
      var destroyed = 0;
      pm.use(_Plugin('a', onDestroy: () => destroyed++));
      pm.use(_Plugin('c', onDestroy: () => destroyed++));
      pm.destroy();
      expect(destroyed, 2);
      expect(errors.map((e) => e.$1), ['bad']);
      expect(pm.all(), isEmpty);
    });
  });

  group('registry', () {
    test('get returns the first plugin of a name, and null for a missing name or the wrong type', () {
      final pm = PluginManager();
      final first = _Plugin('x');
      pm.use(first);
      pm.use(_Plugin('x'));
      expect(pm.get<_Plugin>('x'), same(first));
      expect(pm.get<_Plugin>('missing'), isNull);
      expect(pm.get<_Throwing>('x'), isNull);
    });

    test(
      'remove takes out one plugin of a name and ignores an unknown name',
      () {
        final pm = PluginManager();
        var destroyed = 0;
        pm.use(_Plugin('x', onDestroy: () => destroyed++));
        pm.use(_Plugin('x', onDestroy: () => destroyed++));
        pm.remove('nope');
        expect(destroyed, 0);
        pm.remove('x');
        expect(destroyed, 1);
        expect(pm.all(), hasLength(1));
      },
    );

    test('all lists plugins in registration order, as a copy', () {
      final pm = PluginManager();
      final a = _Plugin('a');
      final b = _Plugin('b');
      pm.use(a);
      pm.use(b);
      final list = pm.all();
      expect(list, [a, b]);
      expect(() => list.add(_Plugin('c')), throwsUnsupportedError);
      pm.remove('a');
      expect(list, [a, b]);
      expect(pm.all(), [b]);
    });

    test('a hook that removes its own plugin during dispatch does not break the loop', () {
      final pm = PluginManager();
      final order = <String>[];
      pm.use(
        _Plugin(
          'a',
          onAfterWrite: (_) {
            order.add('a');
            pm.remove('a');
          },
        ),
      );
      pm.use(_Plugin('b', onAfterWrite: (_) => order.add('b')));
      pm.dispatchAfterWrite(makeWriteEvent());
      expect(order, ['a', 'b']);
      expect(pm.all(), hasLength(1));
    });

    test('presenceRejected is one stable value', () {
      expect(presenceRejected, same(presenceRejected));
      expect(presenceRejected.toString(), 'presenceRejected');
      expect(presenceRejected, isNot(isNull));
    });
  });
}
