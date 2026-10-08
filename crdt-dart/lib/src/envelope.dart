/// How request and response bodies are framed on a server.
///
/// New in the Dart port. crdt-js speaks only Grove's native framing; a
/// foundry server wraps the same change records in camelCase envelopes.
library;

import 'go_json.dart';
import 'hlc.dart';
import 'presence_types.dart';
import 'sync_types.dart';
import 'types.dart';
import 'wire_helpers.dart';

/// How pull, push and presence bodies are framed on a server.
///
/// The decoders take already-parsed JSON and throw [FormatException] for a
/// value of the wrong shape.
abstract interface class SyncEnvelope {
  /// Encodes a pull body.
  String encodePull(PullRequest req);

  /// Decodes a pull response.
  PullResponse decodePull(Object? json);

  /// Encodes a push body.
  String encodePush(PushRequest req);

  /// Decodes a push response.
  PushResponse decodePush(Object? json);

  /// Encodes a presence update body.
  String encodePresenceUpdate(PresenceUpdate update);
}

/// Grove's native framing.
const SyncEnvelope groveEnvelope = GroveEnvelope();

/// foundry-scene-perception's camelCase DTO framing.
const SyncEnvelope camelDtoEnvelope = CamelDtoEnvelope();

/// Grove's native framing (`crdt.PullRequest` and friends).
final class GroveEnvelope implements SyncEnvelope {
  /// Creates the envelope.
  const GroveEnvelope();

  @override
  String encodePull(PullRequest req) => encodeWire(req.toJson());

  @override
  PullResponse decodePull(Object? json) => PullResponse.fromJson(json);

  @override
  String encodePush(PushRequest req) => encodeWire(req.toJson());

  @override
  PushResponse decodePush(Object? json) => PushResponse.fromJson(json);

  @override
  String encodePresenceUpdate(PresenceUpdate update) =>
      encodeWire(update.toJson());
}

/// foundry's DTO framing: camelCase envelopes around grove change records.
///
/// A response that omits `latestHlc` decodes it as [HLC.zero]. A numeric
/// `latestHlc.ts` goes through `num`, so on the web it may round; such a pull
/// response says so in `PullResponse.latestHlcExact`, and the sync engine
/// backs off a margin before using it as a cursor.
final class CamelDtoEnvelope implements SyncEnvelope {
  /// Creates the envelope.
  const CamelDtoEnvelope();

  // `ts` is written as a raw number literal so it is exact on the web, where
  // a Dart `int` is a double.
  static Map<String, Object?> _hlc(HLC h) => {
    'ts': RawJson(h.ts.toString()),
    'counter': h.c,
    'nodeId': h.node,
  };

  static HLC? _readHlc(Object? j) {
    if (j == null) return null;
    final m = wireObj(j);
    final ts = m['ts'];
    return HLC(
      switch (ts) {
        null => BigInt.zero,
        final String s => BigInt.parse(s),
        final num n when n.isFinite => BigInt.from(n),
        _ => throw FormatException('crdt: "ts" must be a number, got $ts'),
      },
      wireInt(m, 'counter'),
      wireStr(m, 'nodeId'),
    );
  }

  @override
  String encodePull(PullRequest req) => encodeWire({
    if (!req.since.isZero) 'since': _hlc(req.since),
    if (req.filter != null) 'filter': req.filter!.toJson(),
  });

  @override
  PullResponse decodePull(Object? json) {
    final m = wireObj(json);
    final latest = m['latestHlc'];
    return PullResponse(
      changes: wireList(m['changes'], ChangeRecord.fromJson),
      latestHlc: _readHlc(latest),
      latestHlcExact: !(latest is Map && latest['ts'] is num),
    );
  }

  @override
  String encodePush(PushRequest req) => encodeWire({
    'changes': [for (final c in req.changes) c.toJson()],
    'nodeId': req.nodeId,
  });

  @override
  PushResponse decodePush(Object? json) {
    final m = wireObj(json);
    return PushResponse(
      merged: wireInt(m, 'merged'),
      latestHlc: _readHlc(m['latestHlc']),
    );
  }

  @override
  String encodePresenceUpdate(PresenceUpdate update) =>
      encodeWire({'nodeId': update.nodeId, 'data': update.data});
}
