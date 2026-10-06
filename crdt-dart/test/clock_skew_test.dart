import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  test('parses an IMF-fixdate', () {
    expect(
      parseHttpDate('Sun, 06 Nov 1994 08:49:37 GMT'),
      DateTime.utc(1994, 11, 6, 8, 49, 37),
    );
    expect(parseHttpDate('garbage'), isNull);
  });

  test('a device three hours ahead gets a negative offset', () {
    final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
    final skew = ClockSkew(systemNowMs: () => local)
      ..observe(DateTime.utc(2026, 10, 4, 12));
    expect(skew.corrected, isTrue);
    // The brief expects `offset.inMinutes` to be -180. The Date header
    // truncates to the second, so `observe` centres the estimate by adding
    // 500 ms and the offset is -3h plus 500 ms, which `inMinutes` truncates
    // to -179. The exact value is asserted instead.
    expect(skew.offset, const Duration(hours: -3, milliseconds: 500));
    expect(skew.offset.inMinutes, -179);
    expect(
      skew.nowMs(),
      closeTo(DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch, 1000),
    );
  });

  test('a small skew is ignored', () {
    final local = DateTime.utc(2026, 10, 4, 12, 0, 5).millisecondsSinceEpoch;
    final skew = ClockSkew(systemNowMs: () => local)
      ..observe(DateTime.utc(2026, 10, 4, 12));
    expect(skew.corrected, isFalse);
    expect(skew.offset, Duration.zero);
  });

  test('observeHlc uses the server clock value', () {
    final local = DateTime.utc(2026, 10, 4, 9).millisecondsSinceEpoch;
    final server = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
    final skew = ClockSkew(systemNowMs: () => local)
      ..observeHlc(HLC(BigInt.from(server) * BigInt.from(1000000), 0, 'srv'));
    expect(skew.offset.inMinutes, 180);
  });

  test('attach makes the HLC clock use corrected time', () {
    final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
    final skew = ClockSkew(systemNowMs: () => local)
      ..observe(DateTime.utc(2026, 10, 4, 12));
    final clock = HybridClock('dev', nowMs: () => local);
    skew.attach(clock);
    final ms = (clock.now().ts ~/ BigInt.from(1000000)).toInt();
    expect(
      ms,
      closeTo(DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch, 1000),
    );
  });

  group('parseHttpDate', () {
    test('reads another IMF-fixdate', () {
      expect(
        parseHttpDate('Tue, 06 Oct 2026 00:00:00 GMT'),
        DateTime.utc(2026, 10, 6),
      );
    });

    test('trims surrounding whitespace', () {
      expect(
        parseHttpDate('  Sun, 06 Nov 1994 08:49:37 GMT '),
        DateTime.utc(1994, 11, 6, 8, 49, 37),
      );
    });

    test('rejects an unknown month, the obsolete formats and a zone', () {
      expect(parseHttpDate('Sun, 06 Foo 1994 08:49:37 GMT'), isNull);
      expect(parseHttpDate('Sunday, 06-Nov-94 08:49:37 GMT'), isNull);
      expect(parseHttpDate('Sun Nov  6 08:49:37 1994'), isNull);
      expect(parseHttpDate('Sun, 06 Nov 1994 08:49:37 +0100'), isNull);
      expect(parseHttpDate(''), isNull);
    });
  });

  group('ClockSkew', () {
    test('a device behind the server gets a positive offset', () {
      final local = DateTime.utc(2026, 10, 4, 9).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observe(DateTime.utc(2026, 10, 4, 12));
      expect(skew.offset, const Duration(hours: 3, milliseconds: 500));
      expect(skew.nowMs(), local + skew.offset.inMilliseconds);
    });

    test('nowNs is nowMs in nanoseconds', () {
      final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observe(DateTime.utc(2026, 10, 4, 12));
      expect(skew.nowNs, BigInt.from(skew.nowMs()) * BigInt.from(1000000));
    });

    test('with nothing observed the clock reads the device time', () {
      final skew = ClockSkew(systemNowMs: () => 1234);
      expect(skew.corrected, isFalse);
      expect(skew.nowMs(), 1234);
      expect(skew.offset, Duration.zero);
    });

    test('an offset just under the threshold is ignored', () {
      // Date header at +29.5 s once centred: 29.5 s < 30 s.
      final local = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observe(DateTime.utc(2026, 10, 4, 12, 0, 29));
      expect(skew.corrected, isFalse);
    });

    test('an offset at the threshold is applied', () {
      final local = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observe(DateTime.utc(2026, 10, 4, 12, 0, 30));
      expect(skew.corrected, isTrue);
    });

    test('the threshold is configurable', () {
      final local = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
      final skew = ClockSkew(
        threshold: const Duration(minutes: 5),
        systemNowMs: () => local,
      )..observe(DateTime.utc(2026, 10, 4, 12, 4));
      expect(skew.corrected, isFalse);
    });

    test('jitter in later readings does not move a corrected offset', () {
      final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observe(DateTime.utc(2026, 10, 4, 12));
      final first = skew.offset;
      // Seven seconds off the first reading: under half the threshold.
      skew.observe(DateTime.utc(2026, 10, 4, 12, 0, 7));
      expect(skew.offset, first);
    });

    test(
      'a reading that moved by more than half the threshold replaces it',
      () {
        final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
        final skew = ClockSkew(systemNowMs: () => local)
          ..observe(DateTime.utc(2026, 10, 4, 12));
        skew.observe(DateTime.utc(2026, 10, 4, 12, 0, 20));
        expect(
          skew.offset,
          const Duration(hours: -3, seconds: 20, milliseconds: 500),
        );
      },
    );

    test('a later small reading clears the correction', () {
      var local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observe(DateTime.utc(2026, 10, 4, 12));
      expect(skew.corrected, isTrue);
      // The user fixed the device clock.
      local = DateTime.utc(2026, 10, 4, 12, 5).millisecondsSinceEpoch;
      skew.observe(DateTime.utc(2026, 10, 4, 12, 5));
      expect(skew.corrected, isFalse);
      expect(skew.nowMs(), local);
    });

    test('observeHlc ignores the zero clock', () {
      final skew = ClockSkew(systemNowMs: () => 5000000)..observeHlc(HLC.zero);
      expect(skew.corrected, isFalse);
    });

    test('observeHlc applies no centring', () {
      final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
      final server = DateTime.utc(2026, 10, 4, 12).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local)
        ..observeHlc(HLC(BigInt.from(server) * BigInt.from(1000000), 3, 's'));
      expect(skew.offset, const Duration(hours: -3));
    });

    test('attach follows a correction that arrives later', () {
      final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local);
      final clock = HybridClock('dev', nowMs: () => local);
      skew.attach(clock);
      // The clock has issued nothing yet, and the first reading arrives only
      // now: the attached clock reads through the skew, not a snapshot of it.
      skew.observe(DateTime.utc(2026, 10, 4, 12));
      final ms = (clock.now().ts ~/ BigInt.from(1000000)).toInt();
      expect(ms, skew.nowMs());
      expect(ms, lessThan(local));
    });

    test('an issued HLC never goes back when a correction arrives', () {
      final local = DateTime.utc(2026, 10, 4, 15).millisecondsSinceEpoch;
      final skew = ClockSkew(systemNowMs: () => local);
      final clock = HybridClock('dev', nowMs: () => local);
      skew.attach(clock);
      final issued = clock.now();
      skew.observe(DateTime.utc(2026, 10, 4, 12));
      // The clock stays monotonic. Recovering from a wrong stamp is the sync
      // engine's job (it rebases the clock and re-stamps).
      expect(clock.now().isAfter(issued), isTrue);
    });

    test('the default clock is the system clock', () {
      final before = DateTime.now().millisecondsSinceEpoch;
      final now = ClockSkew().nowMs();
      final after = DateTime.now().millisecondsSinceEpoch;
      expect(now, inInclusiveRange(before, after));
    });
  });
}
