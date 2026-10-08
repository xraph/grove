/// A wait that [CancellableWait.cancel] can cut short. Not part of the public
/// API.
library;

import 'dart:async';

/// One wait at a time between reconnect attempts.
///
/// Waits with a real [Timer] unless a `sleep` replacement is given (tests).
/// [cancel] ends the current wait at once, so a stopped stream or transport
/// does not leave a timer behind.
final class CancellableWait {
  /// Creates a waiter that uses [sleep] in place of a real timer when given.
  CancellableWait([this._sleep]);

  final Future<void> Function(Duration)? _sleep;
  void Function()? _cancel;

  /// Completes after [delay], or as soon as [cancel] runs.
  Future<void> wait(Duration delay) {
    final wake = Completer<void>();
    void complete() {
      if (!wake.isCompleted) wake.complete();
    }

    Timer? timer;
    final sleeper = _sleep;
    if (sleeper == null) {
      timer = Timer(delay, complete);
    } else {
      sleeper(delay)
          .then<void>((_) => complete(), onError: (Object _) => complete());
    }
    void cancel() {
      timer?.cancel();
      complete();
    }

    _cancel = cancel;
    return wake.future.whenComplete(() {
      if (_cancel == cancel) _cancel = null;
    });
  }

  /// Ends the current wait, if there is one.
  void cancel() => _cancel?.call();
}
