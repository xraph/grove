/// Reading credentials the way every HTTP caller must. Not part of the public
/// API.
///
/// `HttpTransport` and the room client both read the auth provider before each
/// request and both turn what it throws into the same errors, so the rule
/// lives here once.
library;

import 'auth.dart';
import 'errors.dart';

/// The headers [auth] supplies now, or none when it is null.
///
/// An exception from the provider is rethrown through [asAuthError], with its
/// stack trace. A Dart `Error` passes through untouched.
Future<Map<String, String>> readAuthHeaders(CrdtAuthProvider? auth) async {
  try {
    return await auth?.getHeaders() ?? const {};
  } on Exception catch (error, stack) {
    Error.throwWithStackTrace(asAuthError(error, 'reading credentials'), stack);
  }
}

/// A cancellation passes through as thrown. Any other exception from the
/// credentials or the refresh callback is wrapped in an [AuthError], which no
/// retry layer retries (a `NetworkError` from an identity provider must not
/// look like a transport failure to `withRetry`).
Exception asAuthError(Exception error, String doing) =>
    error is CrdtError && error.code == CrdtErrorCode.cancelled
    ? error
    : AuthError('CRDT auth failed while $doing: $error', cause: error);
