/// Device clock correction from server time.
///
/// New in the Dart port. A phone whose clock runs hours off stamps changes
/// the server refuses as too far from its own time (see `DriftRejection`).
/// This estimates the gap from what the server says and shifts the HLC clock
/// by it.
library;

import 'hlc.dart';

const _months = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
];
final _imfFixdate = RegExp(
  r'^\w{3}, (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$',
);

/// Parses an HTTP `Date` header (IMF-fixdate, RFC 7231). Null when the value
/// is in another format.
DateTime? parseHttpDate(String value) {
  final m = _imfFixdate.firstMatch(value.trim());
  if (m == null) return null;
  final month = _months.indexOf(m.group(2)!);
  if (month < 0) return null;
  return DateTime.utc(
    int.parse(m.group(3)!),
    month + 1,
    int.parse(m.group(1)!),
    int.parse(m.group(4)!),
    int.parse(m.group(5)!),
    int.parse(m.group(6)!),
  );
}

final _nsPerMs = BigInt.from(1000000);

/// Estimates how far this device's wall clock is from the server's and
/// corrects for it once the gap is large enough to matter.
///
/// Two readings feed it: the HTTP `Date` header ([observe]) and the push
/// response's `latest_hlc` ([observeHlc]), which is the server's
/// `clock.Now()`. On the web the `Date` header is readable only when the
/// server lists it in `Access-Control-Expose-Headers`; `latest_hlc` still
/// works there.
final class ClockSkew {
  /// Creates an estimator. Offsets smaller than [threshold] are ignored.
  /// [systemNowMs] replaces the wall clock, for tests.
  ClockSkew({
    this.threshold = const Duration(seconds: 30),
    int Function()? systemNowMs,
  }) : _systemNowMs =
           systemNowMs ?? (() => DateTime.now().millisecondsSinceEpoch);

  /// Smallest offset that is applied. Network latency and the one-second
  /// resolution of the `Date` header are noise at a smaller scale.
  final Duration threshold;
  final int Function() _systemNowMs;
  int _offsetMs = 0;

  /// Records an HTTP `Date` header.
  ///
  /// The header has one-second resolution and truncates, so the server's real
  /// time lies in the second after it; 500 ms is added to centre the
  /// estimate.
  void observe(DateTime serverTime) =>
      _record(serverTime.millisecondsSinceEpoch + 500 - _systemNowMs());

  /// Records a server clock value, the push response's `latest_hlc`. The zero
  /// value (a server with nothing to report) is ignored.
  void observeHlc(HLC serverClock) {
    if (serverClock.isZero) return;
    _record((serverClock.ts ~/ _nsPerMs).toInt() - _systemNowMs());
  }

  void _record(int measuredMs) {
    final t = threshold.inMilliseconds;
    if (measuredMs.abs() < t) {
      _offsetMs = 0;
    } else if (_offsetMs == 0 || (measuredMs - _offsetMs).abs() > t ~/ 2) {
      // Once corrected, a new reading replaces the offset only when it
      // differs by more than half the threshold, so jitter in the readings
      // does not move the clock.
      _offsetMs = measuredMs;
    }
  }

  /// The applied correction: add it to the device clock to get server time.
  Duration get offset => Duration(milliseconds: _offsetMs);

  /// Whether a correction is applied.
  bool get corrected => _offsetMs != 0;

  /// Corrected wall time in milliseconds since the Unix epoch.
  int nowMs() => _systemNowMs() + _offsetMs;

  /// Corrected wall time in nanoseconds since the Unix epoch.
  BigInt get nowNs => BigInt.from(nowMs()) * _nsPerMs;

  /// Makes [clock] read corrected time from now on, including a correction
  /// that arrives later.
  void attach(HybridClock clock) => clock.nowMs = nowMs;
}
