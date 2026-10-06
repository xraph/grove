/// Auth providers. Port of crdt-js `auth.ts`.
library;

import 'dart:async';

/// Supplies the headers a request carries, such as `Authorization`.
///
/// [getHeaders] runs before every request, so a provider can refresh an
/// expired token there. It may answer synchronously or with a future.
///
/// Never put a secret in the message of an exception [getHeaders] throws. The
/// transports wrap a provider's exception in an `AuthError` whose text carries
/// the exception's own text, and that text can reach logs and error reports.
/// A provider that fails because its account was switched away throws a
/// `CrdtError` with `CrdtErrorCode.cancelled`, which is passed on as it is.
abstract interface class CrdtAuthProvider {
  /// The headers to add to the next request.
  FutureOr<Map<String, String>> getHeaders();
}

/// An auth provider that returns a fixed set of headers.
///
/// ```dart
/// final auth = StaticAuthProvider({'Authorization': 'Bearer my-token'});
/// ```
final class StaticAuthProvider implements CrdtAuthProvider {
  /// Creates a provider for [headers]. The map is copied.
  StaticAuthProvider(Map<String, String> headers) : _headers = Map.of(headers);

  final Map<String, String> _headers;

  /// A fresh copy of the headers on every call, so a caller that edits the
  /// result cannot change the provider.
  @override
  Map<String, String> getHeaders() => Map.of(_headers);
}
