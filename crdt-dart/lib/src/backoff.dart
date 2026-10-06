/// Exponential backoff with full jitter. Port of crdt-js `backoff.ts`.
///
/// Full jitter (delay = random(0, ceiling)) rather than the raw geometric
/// delay: a fixed schedule makes every client that dropped during an outage
/// retry at the same instant, which is what knocks a recovering server back
/// over.
library;

import 'dart:math' as math;

/// A growing delay schedule.
final class Backoff {
  /// Creates a schedule that starts at [initialDelay], multiplies by [factor]
  /// each step and stops growing at [maxDelay]. With [jitter] each delay is a
  /// random point between zero and that ceiling; [random] is the source of
  /// randomness, for tests.
  Backoff({
    this.initialDelay = const Duration(seconds: 1),
    this.maxDelay = const Duration(seconds: 30),
    this.factor = 2,
    this.jitter = true,
    double Function()? random,
  }) : _random = random ?? math.Random().nextDouble;

  /// First delay.
  final Duration initialDelay;

  /// Ceiling.
  final Duration maxDelay;

  /// Growth multiplier.
  final double factor;

  /// Whether to apply full jitter.
  final bool jitter;

  final double Function() _random;
  int _attempts = 0;

  /// Number of delays issued since the last [reset].
  int get attempt => _attempts;

  /// The next delay, advancing the schedule.
  Duration next() {
    final grown = initialDelay.inMicroseconds * math.pow(factor, _attempts);
    // A zero initial delay times an overflowed power is NaN; read it as zero.
    final ceilingUs = grown.isNaN
        ? 0.0
        : math.min(maxDelay.inMicroseconds.toDouble(), grown);
    _attempts++;
    if (!jitter) return Duration(microseconds: ceilingUs.floor());
    final r = _random().clamp(0.0, 1.0);
    return Duration(microseconds: (r * ceilingUs).floor());
  }

  /// Returns to the initial delay. Call after a successful connection.
  void reset() => _attempts = 0;
}
