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
const _weekdays = {'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'};
final _imfFixdate = RegExp(
  r'^(\w{3}), (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$',
);

/// Parses an HTTP `Date` header (IMF-fixdate, RFC 7231). Null when the value
/// is in another format or names a date or time that does not exist (31 Feb,
/// hour 24, minute 60). The weekday name must be one of the seven; it is not
/// checked against the date.
///
/// RFC 7231 allows second 60 for a leap second; it reads as the first second
/// of the next minute, as POSIX time has no leap second.
DateTime? parseHttpDate(String value) {
  final m = _imfFixdate.firstMatch(value.trim());
  if (m == null) return null;
  if (!_weekdays.contains(m.group(1)!)) return null;
  final month = _months.indexOf(m.group(3)!);
  if (month < 0) return null;
  final year = int.parse(m.group(4)!);
  final day = int.parse(m.group(2)!);
  final hour = int.parse(m.group(5)!);
  final minute = int.parse(m.group(6)!);
  final second = int.parse(m.group(7)!);
  if (hour > 23 || minute > 59 || second > 60) return null;
  final parsed = DateTime.utc(
    year,
    month + 1,
    day,
    hour,
    minute,
    second == 60 ? 59 : second,
  );
  // `DateTime.utc` rolls an impossible date over (31 Feb is 3 Mar), so a
  // field that moved means the input was not a real date.
  if (parsed.year != year || parsed.month != month + 1 || parsed.day != day) {
    return null;
  }
  return second == 60 ? parsed.add(const Duration(seconds: 1)) : parsed;
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
///
/// A correction is applied at a gap of [threshold] or more (30 s by default)
/// and cleared only when a reading falls below five sixths of it (25 s), so a
/// device sitting near the threshold does not flip between corrected and not.
/// A reading further than [maxOffset] (24 hours by default) from the device
/// clock is not believed: it is ignored and the offset stays as it was.
final class ClockSkew {
  /// Creates an estimator. A gap smaller than [threshold] is not corrected
  /// and one larger than [maxOffset] is ignored. [systemNowMs] replaces the
  /// wall clock, for tests.
  ClockSkew({
    this.threshold = const Duration(seconds: 30),
    this.maxOffset = const Duration(hours: 24),
    int Function()? systemNowMs,
  }) : _systemNowMs =
           systemNowMs ?? (() => DateTime.now().millisecondsSinceEpoch);

  /// Smallest gap that is applied. Network latency and the one-second
  /// resolution of the `Date` header are noise at a smaller scale.
  final Duration threshold;

  /// Largest gap that is believed. A server value beyond it (a bug, or a
  /// hostile response) would stamp every later write years out, and the
  /// clock never steps back.
  final Duration maxOffset;
  final int Function() _systemNowMs;
  int _offsetMs = 0;

  /// Records an HTTP `Date` header.
  ///
  /// The header has one-second resolution and truncates, so the server's real
  /// time lies in the second after it; 500 ms is added to centre the
  /// estimate.
  void observe(DateTime serverTime) =>
      _record(serverTime.millisecondsSinceEpoch + 500 - _systemNowMs());

  /// Records a server clock value: the push response's `latest_hlc`, which
  /// is the server's own `clock.Now()`. Do not pass a pull response's
  /// `latest_hlc`, which is the highest change HLC and says nothing about
  /// the server's time. The zero value (a server with nothing to report) is
  /// ignored.
  void observeHlc(HLC serverClock) {
    if (serverClock.isZero) return;
    // Subtract in BigInt and range-check before narrowing, so a value that
    // does not fit an int cannot wrap on the VM or lose precision on the web.
    final gap = serverClock.ts ~/ _nsPerMs - BigInt.from(_systemNowMs());
    if (gap.abs() > BigInt.from(maxOffset.inMilliseconds)) return;
    _record(gap.toInt());
  }

  void _record(int measuredMs) {
    if (measuredMs.abs() > maxOffset.inMilliseconds) return;
    final t = threshold.inMilliseconds;
    if (_offsetMs == 0) {
      if (measuredMs.abs() >= t) _offsetMs = measuredMs;
    } else if (measuredMs.abs() < t * 5 ~/ 6) {
      _offsetMs = 0;
    } else if ((measuredMs - _offsetMs).abs() > t ~/ 2) {
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
