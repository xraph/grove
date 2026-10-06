import 'dart:convert';

import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

HLC h(int ts, int c, String node) => HLC(BigInt.from(ts), c, node);

void main() {
  group('hlcCompare', () {
    test('returns -1 when a.ts < b.ts', () {
      expect(h(100, 0, 'a').compareTo(h(200, 0, 'b')), -1);
    });
    test('returns 1 when a.ts > b.ts', () {
      expect(h(200, 0, 'a').compareTo(h(100, 0, 'b')), 1);
    });
    test('returns 0 for identical HLCs', () {
      final a = h(100, 1, 'a');
      expect(a.compareTo(a), 0);
    });
    test('breaks tie by counter when timestamps are equal', () {
      expect(h(100, 1, 'a').compareTo(h(100, 2, 'b')), -1);
    });
    test('breaks tie by node ID when timestamp and counter are equal', () {
      expect(h(100, 1, 'a').compareTo(h(100, 1, 'b')), -1);
    });
    test('returns 1 when node a > node b with same ts and counter', () {
      expect(h(100, 1, 'b').compareTo(h(100, 1, 'a')), 1);
    });
  });

  group('hlcAfter', () {
    test('returns true when a is strictly after b', () {
      expect(h(200, 0, 'a').isAfter(h(100, 0, 'a')), isTrue);
    });
    test('returns false when a equals b', () {
      expect(h(100, 0, 'a').isAfter(h(100, 0, 'a')), isFalse);
    });
    test('returns false when a is before b', () {
      expect(h(100, 0, 'a').isAfter(h(200, 0, 'a')), isFalse);
    });
  });

  group('hlcIsZero', () {
    test('returns true for HLC_ZERO', () {
      expect(HLC.zero.isZero, isTrue);
    });
    test('returns false when ts is non-zero', () {
      expect(h(1, 0, '').isZero, isFalse);
    });
    test('returns false when counter is non-zero', () {
      expect(h(0, 1, '').isZero, isFalse);
    });
    test('returns false when node is non-empty', () {
      expect(h(0, 0, 'x').isZero, isFalse);
    });
  });

  group('hlcMax', () {
    test('returns the greater HLC', () {
      final b = h(200, 0, 'b');
      expect(identical(hlcMax(h(100, 0, 'a'), b), b), isTrue);
    });
    test('returns b when they are equal', () {
      final b = h(100, 0, 'a');
      expect(identical(hlcMax(h(100, 0, 'a'), b), b), isTrue);
    });
    test('returns the one with higher node on tiebreak', () {
      final b = h(100, 0, 'b');
      expect(identical(hlcMax(h(100, 0, 'a'), b), b), isTrue);
    });
  });

  group('hlcString', () {
    test('produces deterministic Go-compatible string', () {
      expect(hlcString(h(12345, 7, 'n1')), 'HLC{ts:12345 c:7 node:n1}');
    });
    test('handles HLC_ZERO', () {
      expect(hlcString(HLC.zero), 'HLC{ts:0 c:0 node:}');
    });
  });

  group('HybridClock', () {
    group('constructor', () {
      test('initializes with given nodeID', () {
        expect(HybridClock('node-1').nodeId, 'node-1');
      });
    });

    group('now()', () {
      test('returns HLC with correct nodeID', () {
        expect(HybridClock('node-1', nowMs: () => 1000).now().node, 'node-1');
      });
      test('returns monotonically increasing HLCs', () {
        final clock = HybridClock('node-1');
        final h1 = clock.now();
        final h2 = clock.now();
        expect(h2.isAfter(h1), isTrue);
      });
      test('increments counter when physical clock has not advanced', () {
        final clock = HybridClock('node-1', nowMs: () => 1000);
        final h1 = clock.now();
        final h2 = clock.now();
        expect(h1.c, 0);
        expect(h2.c, 1);
        expect(h1.ts, h2.ts);
      });
      test('resets counter when physical clock advances', () {
        var time = 1000;
        final clock = HybridClock('node-1', nowMs: () => time);
        clock.now();
        clock.now();
        time = 2000;
        final h3 = clock.now();
        expect(h3.c, 0);
        expect(h3.ts, BigInt.from(2000 * 1000000));
      });
      test('converts milliseconds to nanoseconds', () {
        final clock = HybridClock('node-1', nowMs: () => 1000);
        expect(clock.now().ts, BigInt.from(1000 * 1000000));
      });
    });

    group('update()', () {
      test('advances past remote when physical clock is ahead of both', () {
        var time = 2000;
        final clock = HybridClock('node-1', nowMs: () => time);
        clock.now();
        time = 3000;
        final remote = h(1000 * 1000000, 5, 'node-2');
        clock.update(remote);
        expect(clock.now().isAfter(remote), isTrue);
      });
      test('merges when local and remote have same timestamp', () {
        final clock = HybridClock('node-1', nowMs: () => 1000);
        clock.now();
        final remote = h(1000 * 1000000, 10, 'node-2');
        clock.update(remote);
        expect(clock.now().isAfter(remote), isTrue);
      });
      test('increments local counter when local is ahead of remote', () {
        final clock = HybridClock('node-1', nowMs: () => 1000);
        clock.now();
        clock.now();
        final remote = h(500 * 1000000, 0, 'node-2');
        clock.update(remote);
        expect(clock.now().isAfter(remote), isTrue);
      });
      // Differs from the TS case, which fixes nowFn at 1000, calls now() once
      // and merges a remote at 2000 ms. This one advances the wall clock to the
      // remote's timestamp first; it reaches the same adopt-remote branch.
      test('adopts remote timestamp when remote is ahead', () {
        var time = 1000;
        final clock = HybridClock('node-1', nowMs: () => time);
        clock.now();
        clock.now();
        time = 2000;
        final remote = h(2000 * 1000000, 5, 'node-2');
        clock.update(remote);
        expect(clock.now().isAfter(remote), isTrue);
      });
      test('ensures now() is causally after update(remote)', () {
        final clock = HybridClock('node-1');
        final local = clock.now();
        final remote = HLC(local.ts + BigInt.from(1000000), 5, 'node-2');
        clock.update(remote);
        expect(clock.now().isAfter(remote), isTrue);
      });
      test('clamps remote timestamp to maxDrift', () {
        const fixed = 1000;
        final clock = HybridClock('node-1', maxDriftMs: 1000, nowMs: () => fixed);
        clock.update(h((fixed + 10000) * 1000000, 0, 'node-2'));
        expect(clock.now().ts <= BigInt.from((fixed + 1000) * 1000000), isTrue);
      });
      test('handles remote with higher counter at same timestamp', () {
        final clock = HybridClock('node-1', nowMs: () => 1000);
        clock.now();
        clock.update(h(1000 * 1000000, 10, 'node-2'));
        expect(clock.now().c, greaterThanOrEqualTo(12));
      });
    });
  });

  group('Go wire parity', () {
    test('encodes ts as an exact decimal string', () {
      final v = HLC(BigInt.parse('1712345678901234567'), 3, 'n');
      expect(jsonEncode(v.toJson()), '{"ts":"1712345678901234567","c":3,"node":"n"}');
    });
    test('decodes the string form without precision loss', () {
      final v = HLC.fromJson(jsonDecode('{"ts":"1712345678901234567","c":3,"node":"n"}'));
      expect(v.ts, BigInt.parse('1712345678901234567'));
      expect(v.c, 3);
      expect(v.node, 'n');
    });
    test('decodes the legacy numeric form', () {
      expect(HLC.fromJson(jsonDecode('{"ts":12345,"c":0,"node":"n"}')).ts, BigInt.from(12345));
    });
    test('decodes an empty ts as zero', () {
      expect(HLC.fromJson(jsonDecode('{"ts":"","c":0,"node":""}')).isZero, isTrue);
    });
    test('compares node ids by code point like Go', () {
      // Go parity: HLC.Compare compares NodeID by UTF-8 bytes, so U+FFFD sorts
      // before U+1F600. TS compares UTF-16 units, which puts it after.
      expect(compareGoStrings('\u{FFFD}', '\u{1F600}'), -1);
      expect(h(1, 0, '\u{FFFD}').compareTo(h(1, 0, '\u{1F600}')), -1);
    });
    test('orders an unpaired surrogate as U+FFFD', () {
      // Go parity: Go has decoded the surrogate to U+FFFD before comparing.
      expect(compareGoStrings('\uD800', '\u{FFFD}'), 0);
      expect(compareGoStrings('\uD800', '\uE000'), 1);
      expect(compareGoStrings('\uDC00', '\u{1F600}'), -1);
    });
    test('accepts a signed decimal ts within int64', () {
      HLC ts(String s) => HLC.fromJson(jsonDecode('{"ts":"$s","c":0,"node":""}'));
      expect(ts('+5').ts, BigInt.from(5));
      expect(ts('-7').ts, BigInt.from(-7));
      expect(ts('9223372036854775807').ts, BigInt.parse('9223372036854775807'));
      expect(ts('-9223372036854775808').ts, BigInt.parse('-9223372036854775808'));
    });
    test('rejects a ts string Go rejects', () {
      // Go parity: strconv.ParseInt(s, 10, 64).
      for (final bad in ['0x10', ' 12 ', '1_0', '1.5', '+', '-', '9223372036854775808', '99999999999999999999']) {
        expect(
          () => HLC.fromJson(jsonDecode('{"ts":"$bad","c":0,"node":""}')),
          throwsFormatException,
          reason: bad,
        );
      }
    });
    test('accepts a counter in [0, 2^32) and rejects the rest', () {
      // Go parity: HLC.Counter is a uint32.
      HLC counter(String c) => HLC.fromJson(jsonDecode('{"ts":"1","c":$c,"node":""}'));
      expect(counter('0').c, 0);
      expect(counter('4294967295').c, 4294967295);
      for (final bad in ['-3', '2.7', '4294967296', '"3"', 'true']) {
        expect(() => counter(bad), throwsFormatException, reason: bad);
      }
    });
    test('treats a missing counter as zero', () {
      expect(HLC.fromJson(jsonDecode('{"ts":"1","node":"n"}')).c, 0);
    });
    test('toString is the Go HLC.String form', () {
      expect(h(9, 2, 'x').toString(), 'HLC{ts:9 c:2 node:x}');
    });
  });

  group('rebase', () {
    test('switches the node id and lets time move backwards without reusing an HLC', () {
      var now = 9000;
      final clock = HybridClock('dev', nowMs: () => now);
      final future = clock.now();
      now = 1000;
      clock.rebase('dev~1');
      final next = clock.now();
      expect(next.node, 'dev~1');
      expect(next.ts, BigInt.from(1000 * 1000000));
      expect(next == future, isFalse);
      expect(clock.nodeId, 'dev~1');
    });
  });
}
