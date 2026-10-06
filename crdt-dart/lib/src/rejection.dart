/// Why the server refused a push, read from its error response.
///
/// New in the Dart port: crdt-js treats every push failure alike. The sync
/// engine needs to know which change to blame and whether the rest of the
/// batch was kept.
///
/// The messages come from the Go server (`crdt/validation.go`,
/// `crdt/server.go`). This describes grove v1.7.0. The pending branch
/// `fix/crdt-sync-defects` changes two behaviours, and the classifier reads
/// correctly under both because it matches message text, never position:
///
///  * v1.7.0 merges the changes before a hook rejection and then fails the
///    push; the fix fails the whole push with nothing merged.
///  * v1.7.0 rejects a change whose HLC is more than the drift bound away
///    from server time in either direction; the fix rejects only future-dated
///    ones.
///
/// The error messages are the same on both. What differs is the engine's
/// recovery: under v1.7.0 a hook rejection leaves earlier changes merged,
/// so the engine bisects. Bisection works under both.
library;

/// Why the server refused a push. See [classifyPushError].
sealed class PushRejection {
  /// Creates a rejection with the server's text.
  const PushRejection(this.reason);

  /// The server's text for this rejection.
  final String reason;

  /// The `PendingRejection.kind` this maps to.
  String get kind;
}

/// Validation failed for the change at [index]; nothing was merged.
final class ValidationRejection extends PushRejection {
  /// Creates the rejection.
  const ValidationRejection(this.index, super.reason);

  /// Index of the offending change in the pushed batch.
  final int index;

  @override
  String get kind => 'validation';
}

/// The change at [index] has an HLC too far from server time; nothing was
/// merged.
///
/// Go parity: `ValidationConfig.ValidateChangeRecord`. grove v1.7.0 measures
/// the distance in both directions; `fix/crdt-sync-defects` rejects only
/// future-dated changes. The message is the same.
final class DriftRejection extends PushRejection {
  /// Creates the rejection.
  const DriftRejection(this.index, super.reason);

  /// Index of the offending change in the pushed batch.
  final int index;

  @override
  String get kind => 'drift';
}

/// The batch exceeded the server's change limit; nothing was merged.
final class BatchTooLargeRejection extends PushRejection {
  /// Creates the rejection.
  const BatchTooLargeRejection(this.limit, super.reason);

  /// The server's maximum changes per push.
  final int limit;

  @override
  String get kind => 'validation';
}

/// A server sync hook refused one change.
///
/// Go parity: `SyncController.HandlePush`. In grove v1.7.0 the changes before
/// the refused one in the batch were merged, and which change was refused is
/// unknown until the batch is bisected. `fix/crdt-sync-defects` fails the
/// whole push with nothing merged. Both are handled by bisecting.
final class HookRejection extends PushRejection {
  /// Creates the rejection.
  const HookRejection(super.reason);

  @override
  String get kind => 'hook';
}

/// Any other failure with a response.
final class UnclassifiedRejection extends PushRejection {
  /// Creates the rejection.
  const UnclassifiedRejection(this.status, super.reason);

  /// HTTP status, or 0 for a WebSocket error frame.
  final int status;

  @override
  String get kind => status >= 400 && status < 500 ? 'bad_request' : 'server';
}

/// The message text of an error body, if any.
///
/// Reads `{"error": ...}` (the grove extension, the conformance server and
/// the WebSocket error payload), `{"message": ...}` (forge `HTTPError`,
/// foundry) or a raw string body.
String? serverMessage(Object? body) => switch (body) {
  final String s => s,
  {'error': final String e} => e,
  {'message': final String m} => m,
  _ => null,
};

// The digit runs are capped at 15, which fits an int exactly on the VM and on
// the web, so a number outside what a server could send never parses to
// something platform-dependent.
final _maxChanges = RegExp(
  r'crdt: push exceeds max changes \(\d+ > (\d{1,15})\)',
);
final _changeIndex = RegExp(r'crdt: change\[(\d{1,15})\]: (.*)$', dotAll: true);
const _hookPrefix = 'crdt: inbound change hook: ';

/// Classifies a failed push from its HTTP [status] (0 for a WebSocket error
/// frame) and decoded [body].
///
/// Rules, in order:
///
///  1. `crdt: push exceeds max changes (n > m)` is a
///     [BatchTooLargeRejection] with limit m.
///  2. `crdt: change[i]: <inner>` is a [DriftRejection] when `<inner>`
///     contains `HLC timestamp drift too large`, else a [ValidationRejection].
///  3. A message containing `crdt: inbound change hook: ` is a
///     [HookRejection] carrying the text after it.
///  4. Anything else is an [UnclassifiedRejection].
///
/// Where more than one pattern appears in a message, the one that starts
/// first wins. A hook's own reason is free text and may quote another
/// pattern (a hook that wraps a validation error); the server's prefix always
/// comes first, so the hook rule still wins, and the engine never blames the
/// wrong index.
PushRejection classifyPushError(int status, Object? body) {
  final msg = serverMessage(body) ?? '';
  final max = _maxChanges.firstMatch(msg);
  final idx = _changeIndex.firstMatch(msg);
  final hook = msg.indexOf(_hookPrefix);
  final hookFirst =
      hook >= 0 &&
      (max == null || hook < max.start) &&
      (idx == null || hook < idx.start);
  if (hookFirst) return HookRejection(msg.substring(hook + _hookPrefix.length));
  final limit = max == null ? null : int.tryParse(max.group(1)!);
  if (limit != null) return BatchTooLargeRejection(limit, msg);
  final index = idx == null ? null : int.tryParse(idx.group(1)!);
  if (index != null) {
    final inner = idx!.group(2)!;
    return inner.contains('HLC timestamp drift too large')
        ? DriftRejection(index, inner)
        : ValidationRejection(index, inner);
  }
  return UnclassifiedRejection(status, msg);
}
