import 'package:meta/meta.dart';

import 'wire_helpers.dart';

/// Compares two strings by Unicode code point, which is Go's byte order for
/// UTF-8 strings. Returns -1, 0 or 1.
///
/// An unpaired surrogate compares as U+FFFD, because Go has already decoded it
/// to that rune.
int compareGoStrings(String a, String b) {
  final ai = a.runes.iterator;
  final bi = b.runes.iterator;
  while (true) {
    final aHas = ai.moveNext();
    final bHas = bi.moveNext();
    if (!aHas) return bHas ? -1 : 0;
    if (!bHas) return 1;
    final d = _goRune(ai.current) - _goRune(bi.current);
    if (d != 0) return d < 0 ? -1 : 1;
  }
}

int _goRune(int r) => r >= 0xD800 && r <= 0xDFFF ? 0xFFFD : r;

final BigInt _int64Min = BigInt.parse('-9223372036854775808');
final BigInt _int64Max = BigInt.parse('9223372036854775807');
final RegExp _decimalInt = RegExp(r'^[+-]?[0-9]+$');
const int _uint32Limit = 4294967296;

BigInt _parseTs(String s) {
  // Go parity: strconv.ParseInt(s, 10, 64) takes an optional sign and decimal
  // digits only, and rejects anything outside int64.
  if (!_decimalInt.hasMatch(s)) throw FormatException('crdt: hlc ts "$s"');
  final v = BigInt.parse(s);
  if (v < _int64Min || v > _int64Max) {
    throw FormatException('crdt: hlc ts "$s" out of range');
  }
  return v;
}

int _parseCounter(Object? raw) {
  // Go parity: the counter is a uint32, so it is an integer in [0, 2^32).
  // The web parser yields a double for integral values.
  final int? v = switch (raw) {
    null => 0,
    final int i => i,
    final double d when d.isFinite && d == d.truncateToDouble() => d.toInt(),
    _ => null,
  };
  if (v == null || v < 0 || v >= _uint32Limit) {
    throw FormatException('crdt: hlc counter $raw');
  }
  return v;
}

/// A hybrid logical clock value. Mirrors Go `crdt.HLC`.
///
/// Ordering is timestamp, then counter, then node id.
@immutable
final class HLC implements Comparable<HLC> {
  /// Creates an HLC from its three parts.
  HLC(this.ts, this.c, this.node);

  /// The zero value: no timestamp, no counter, no node.
  static final HLC zero = HLC(BigInt.zero, 0, '');

  /// Physical time in nanoseconds since the Unix epoch.
  final BigInt ts;

  /// Logical counter.
  final int c;

  /// The node that produced this value.
  final String node;

  /// The Go `HLC.String()` form, built once: list walks and set keys ask for
  /// it per node on every read, and a 19-digit [BigInt] is slow to print on
  /// the web.
  late final String _goString = 'HLC{ts:$ts c:$c node:$node}';

  /// Whether this is the zero value.
  bool get isZero => ts == BigInt.zero && c == 0 && node.isEmpty;

  /// Whether this value is strictly after [other].
  bool isAfter(HLC other) => compareTo(other) > 0;

  @override
  int compareTo(HLC other) {
    final t = ts.compareTo(other.ts);
    if (t != 0) return t < 0 ? -1 : 1;
    if (c != other.c) return c < other.c ? -1 : 1;
    return compareGoStrings(node, other.node);
  }

  /// The Go wire form: `ts` as a decimal string.
  Map<String, Object?> toJson() => {'ts': ts.toString(), 'c': c, 'node': node};

  /// Decodes the Go wire form, accepting a string, empty or numeric `ts`.
  ///
  /// Strict like Go: a string `ts` is a signed decimal integer that fits
  /// int64, and `c` is an integer in [0, 2^32). Anything else throws a
  /// [FormatException].
  static HLC fromJson(Object? json) {
    if (json == null) return zero;
    final m = wireObj(json);
    final raw = m['ts'];
    final BigInt ts = switch (raw) {
      null => BigInt.zero,
      final String s when s.isEmpty => BigInt.zero,
      final String s => _parseTs(s),
      final int i => BigInt.from(i),
      final double d => BigInt.from(d),
      _ => throw FormatException('crdt: hlc ts $raw'),
    };
    return HLC(ts, _parseCounter(m['c']), wireStr(m, 'node'));
  }

  @override
  bool operator ==(Object other) =>
      other is HLC && other.ts == ts && other.c == c && other.node == node;

  @override
  int get hashCode => Object.hash(ts, c, node);

  @override
  String toString() => hlcString(this);
}

/// The Go `HLC.String()` form, used as a map key throughout the protocol.
String hlcString(HLC h) => h._goString;

/// Returns [a] when it is strictly after [b], otherwise [b].
HLC hlcMax(HLC a, HLC b) => a.isAfter(b) ? a : b;

final BigInt _nsPerMs = BigInt.from(1000000);

/// Generates monotonically increasing HLC values. Port of Go `HybridClock`.
final class HybridClock {
  /// Creates a clock for [nodeId]. [nowMs] returns wall time in
  /// milliseconds and defaults to the system clock.
  HybridClock(String nodeId, {int maxDriftMs = 5000, int Function()? nowMs})
      : _nodeId = nodeId,
        _maxDriftNs = BigInt.from(maxDriftMs) * _nsPerMs,
        _nowMs = nowMs ?? _systemNowMs;

  /// The node id stamped on every value this clock issues.
  String get nodeId => _nodeId;

  /// Switches to [nodeId] and forgets the last issued value. See the
  /// `ClockSkew` recovery in `SyncEngine`.
  void rebase(String nodeId) {
    _nodeId = nodeId;
    _lastTs = BigInt.zero;
    _lastC = 0;
  }

  String _nodeId;
  final BigInt _maxDriftNs;
  int Function() _nowMs;
  BigInt _lastTs = BigInt.zero;
  int _lastC = 0;

  static int _systemNowMs() => DateTime.now().millisecondsSinceEpoch;

  /// Replaces the wall clock source. Used by `ClockSkew`.
  set nowMs(int Function() fn) => _nowMs = fn;

  /// The last value issued or merged.
  HLC get last => HLC(_lastTs, _lastC, nodeId);

  /// A new value causally after everything this clock has seen.
  HLC now() {
    final physical = BigInt.from(_nowMs()) * _nsPerMs;
    if (physical > _lastTs) {
      _lastTs = physical;
      _lastC = 0;
    } else {
      _lastC += 1;
    }
    return last;
  }

  /// Merges a remote value so the next [now] is after both. Port of Go
  /// `HybridClock.Update`, including the drift clamp.
  void update(HLC remote) {
    final physical = BigInt.from(_nowMs()) * _nsPerMs;
    var remoteTs = remote.ts;
    final maxAllowed = physical + _maxDriftNs;
    if (remoteTs > maxAllowed) remoteTs = maxAllowed;

    if (physical > _lastTs && physical > remoteTs) {
      _lastTs = physical;
      _lastC = 0;
    } else if (_lastTs == remoteTs) {
      _lastC = (_lastC > remote.c ? _lastC : remote.c) + 1;
    } else if (_lastTs > remoteTs) {
      _lastC += 1;
    } else {
      _lastTs = remoteTs;
      _lastC = remote.c + 1;
    }
  }
}
