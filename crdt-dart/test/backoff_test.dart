// Port of crdt-js src/__tests__/backoff.test.ts, case for case.
import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

Duration ms(int n) => Duration(milliseconds: n);

void main() {
  group('Backoff', () {
    test('grows geometrically without jitter', () {
      final b = Backoff(
        initialDelay: ms(100),
        factor: 2,
        maxDelay: ms(10000),
        jitter: false,
      );
      expect(b.next(), ms(100));
      expect(b.next(), ms(200));
      expect(b.next(), ms(400));
      expect(b.next(), ms(800));
    });

    test('clamps at maxDelay', () {
      final b = Backoff(
        initialDelay: ms(1000),
        factor: 10,
        maxDelay: ms(5000),
        jitter: false,
      );
      b.next();
      b.next();
      expect(b.next(), ms(5000));
      expect(b.next(), ms(5000));
    });

    test('full jitter keeps the delay within [0, ceiling]', () {
      // random() == 1 yields the ceiling; random() == 0 yields zero.
      final hi = Backoff(
        initialDelay: ms(100),
        factor: 2,
        jitter: true,
        random: () => 1,
      );
      final lo = Backoff(
        initialDelay: ms(100),
        factor: 2,
        jitter: true,
        random: () => 0,
      );
      expect(hi.next(), ms(100));
      expect(lo.next(), Duration.zero);
    });

    test('reset returns to the initial delay', () {
      final b = Backoff(initialDelay: ms(100), factor: 2, jitter: false);
      b.next();
      b.next();
      b.next();
      expect(b.attempt, 3);
      b.reset();
      expect(b.attempt, 0);
      expect(b.next(), ms(100));
    });
  });

  group('Backoff defaults', () {
    test('start at one second, double, and stop at thirty', () {
      final b = Backoff(jitter: false);
      expect(
        [for (var i = 0; i < 7; i++) b.next().inSeconds],
        [1, 2, 4, 8, 16, 30, 30],
      );
    });

    test('jitter scales the ceiling by the random draw', () {
      final b = Backoff(initialDelay: ms(1000), random: () => 0.5);
      expect(b.next(), ms(500));
      expect(b.next(), ms(1000));
    });

    test('a long outage keeps returning the ceiling', () {
      final b = Backoff(jitter: false);
      Duration last = Duration.zero;
      for (var i = 0; i < 5000; i++) {
        last = b.next();
      }
      expect(last, const Duration(seconds: 30));
      expect(b.attempt, 5000);
    });

    test(
      'a random source outside [0, 1) is clamped to the ceiling and zero',
      () {
        final over = Backoff(initialDelay: ms(100), random: () => 2.5);
        final under = Backoff(initialDelay: ms(100), random: () => -1);
        expect(over.next(), ms(100));
        expect(under.next(), Duration.zero);
      },
    );

    test('a zero initial delay stays zero', () {
      final b = Backoff(initialDelay: Duration.zero, jitter: false);
      for (var i = 0; i < 2000; i++) {
        expect(b.next(), Duration.zero);
      }
    });
  });
}
