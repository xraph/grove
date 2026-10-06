/// Room management over the server's room HTTP endpoints. Port of crdt-js
/// `room.ts`.
///
/// The paths, request bodies and status codes follow Go's room handlers
/// (`crdt.RoomHTTPHandler` and the Forge extension's room routes), which are
/// the authority where crdt-js differs.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:http/http.dart' as http;

import 'auth.dart';
import 'auth_read.dart';
import 'errors.dart';
import 'presence_types.dart';
import 'redact.dart';
import 'types.dart';
import 'wire_helpers.dart';

/// Room operations against a CRDT sync server.
///
/// [baseUrl] is the same URL the sync client uses. Every call reads the auth
/// provider again, so a token refreshed between calls is used. An error from
/// the provider is never retried and is not turned into a response: it is
/// thrown as an [AuthError], except a [CrdtError] with
/// [CrdtErrorCode.cancelled] (the account the provider belongs to was
/// switched away), which reaches the caller as thrown. Nothing here retries a
/// request.
///
/// A failed response throws [TransportError] with the status, the body and
/// the response headers; no response at all throws [NetworkError]. No error
/// text shows a query value or a credential: URLs in it are rebuilt without
/// their query values, and the values of the auth and static headers are
/// removed.
///
/// ```dart
/// final rooms = RoomClient(baseUrl: Uri.parse('https://api.example.com/sync'));
/// final room = await rooms.joinDocumentRoom(
///   'documents',
///   'doc-1',
///   'node-1',
///   ParticipantData(name: 'Alice', color: randomColor()),
/// );
/// await rooms.updateCursor(
///   room.room.id,
///   'node-1',
///   const CursorPosition(line: 10, column: 5),
/// );
/// ```
final class RoomClient {
  /// Creates a client for the server at [baseUrl]. Trailing slashes on its
  /// path are dropped.
  ///
  /// Room paths sit under [roomsPath]. [client] defaults to a new
  /// `http.Client`, which [close] shuts. [headers] go on every request;
  /// headers from [auth] win over them. crdt-js sends only [headers]; the
  /// provider is new here.
  RoomClient({
    required Uri baseUrl,
    this.roomsPath = '/rooms',
    http.Client? client,
    Map<String, String> headers = const {},
    this._auth,
  }) : baseUrl = baseUrl.replace(
         path: baseUrl.path.replaceFirst(RegExp(r'/+$'), ''),
       ),
       _client = client ?? http.Client(),
       _ownsClient = client == null,
       _headers = Map.of(headers);

  /// The server's base URL, without trailing slashes on its path.
  final Uri baseUrl;

  /// The path of the room endpoints, under [baseUrl].
  final String roomsPath;

  final http.Client _client;
  final bool _ownsClient;
  final Map<String, String> _headers;
  final CrdtAuthProvider? _auth;

  /// Closes the HTTP client if this client created it.
  void close() {
    if (_ownsClient) _client.close();
  }

  /// Lists the rooms, only those of [type] when it is given and not empty.
  Future<List<RoomInfo>> listRooms({String? type}) => _request(
    'GET',
    roomsPath,
    query: type == null || type.isEmpty ? null : {'type': type},
    // Go answers `null` for a type with no rooms.
    decode: (j) => wireList(j, RoomInfo.fromJson),
  );

  /// Returns the room [roomId] with its live participants, or `null` when the
  /// server has no such room (a 404).
  ///
  /// Go parity: only a 404 means the room is absent. crdt-js answers `null`
  /// for any failure, which would hide a rejected credential or a cancelled
  /// account switch, so every other failure is thrown.
  Future<RoomInfo?> getRoom(String roomId) async {
    try {
      return await _request(
        'GET',
        _roomPath(roomId),
        decode: (j) => RoomInfo.fromJson(_required(j)),
      );
    } on TransportError catch (error) {
      if (error.statusCode == 404) return null;
      rethrow;
    }
  }

  /// Creates a room. The server returns the existing room when [id] is taken.
  ///
  /// [type], [metadata], [maxParticipants] and [createdBy] are sent only when
  /// set. A [maxParticipants] of zero or less means no limit.
  Future<Room> createRoom(
    String id, {
    String type = '',
    Object? metadata,
    int maxParticipants = 0,
    String createdBy = '',
  }) => _request(
    'POST',
    roomsPath,
    body: {
      'id': id,
      if (type.isNotEmpty) 'type': type,
      'metadata': ?metadata,
      if (maxParticipants > 0) 'max_participants': maxParticipants,
      if (createdBy.isNotEmpty) 'created_by': createdBy,
    },
    decode: (j) => Room.fromJson(_required(j)),
  );

  /// Joins [roomId] as [nodeId] with the optional participant [data].
  ///
  /// The server creates the room when it does not exist. A full room is a 409
  /// [TransportError].
  Future<RoomInfo> joinRoom(
    String roomId,
    String nodeId, [
    ParticipantData? data,
  ]) => _request(
    'POST',
    '${_roomPath(roomId)}/join',
    body: {'node_id': nodeId, if (data != null) 'data': data.toJson()},
    decode: (j) => RoomInfo.fromJson(_required(j)),
  );

  /// Leaves [roomId]. The server destroys a room that becomes empty.
  Future<void> leaveRoom(String roomId, String nodeId) => _request(
    'POST',
    '${_roomPath(roomId)}/leave',
    body: {'node_id': nodeId},
    decode: _ignore,
  );

  /// Sets the cursor of [nodeId] in [roomId]. The server merges it into the
  /// participant's presence data.
  Future<void> updateCursor(
    String roomId,
    String nodeId,
    CursorPosition cursor,
  ) => _request(
    'POST',
    '${_roomPath(roomId)}/cursor',
    body: {'node_id': nodeId, 'cursor': cursor.toJson()},
    decode: _ignore,
  );

  /// Sets whether [nodeId] is typing in [roomId].
  Future<void> updateTyping(String roomId, String nodeId, bool isTyping) =>
      _request(
        'POST',
        '${_roomPath(roomId)}/typing',
        body: {'node_id': nodeId, 'is_typing': isTyping},
        decode: _ignore,
      );

  /// Replaces the metadata of [roomId]. A room the server does not know is a
  /// 404 [TransportError].
  ///
  /// Go parity: `crdt.RoomHTTPHandler` serves `PUT /rooms/{id}/metadata` and
  /// reads the whole body as the metadata. crdt-js sends `POST` with
  /// `{"metadata": ...}`, which Go answers with a 405.
  Future<void> updateMetadata(String roomId, Object? metadata) => _request(
    'PUT',
    '${_roomPath(roomId)}/metadata',
    body: metadata,
    hasBody: true,
    decode: _ignore,
  );

  /// The presence states of everyone in [roomId].
  Future<List<PresenceState>> getParticipants(String roomId) => _request(
    'GET',
    '${_roomPath(roomId)}/participants',
    decode: (j) => wireList(j, PresenceState.fromJson),
  );

  /// Creates the document room for [table] and [pk] if needed, then joins it.
  ///
  /// A failure to create is ignored, as in crdt-js: the room probably exists,
  /// and Go's join creates a missing room anyway. A cancellation, a failure
  /// of the auth provider and a Dart `Error` are not ignored.
  Future<RoomInfo> joinDocumentRoom(
    String table,
    String pk,
    String nodeId, [
    ParticipantData? data,
  ]) async {
    final roomId = documentRoomId(table, pk);
    try {
      await createRoom(
        roomId,
        type: 'document',
        metadata: <String, Object?>{'table': table, 'pk': pk},
      );
    } on AuthError {
      rethrow;
    } on CrdtError catch (error) {
      if (error.code == CrdtErrorCode.cancelled) rethrow;
      // The room likely exists. Join goes on.
    }
    return joinRoom(roomId, nodeId, data);
  }

  /// Leaves the document room for [table] and [pk].
  Future<void> leaveDocumentRoom(String table, String pk, String nodeId) =>
      leaveRoom(documentRoomId(table, pk), nodeId);

  String _roomPath(String roomId) =>
      '$roomsPath/${Uri.encodeComponent(roomId)}';

  static void _ignore(Object? _) {}

  /// The decoded body, which a call that returns one must have.
  static Object _required(Object? json) =>
      json ?? (throw const FormatException('the response body is empty'));

  Uri _url(String path, Map<String, String>? query) {
    final slash = path.startsWith('/') ? path : '/$path';
    return baseUrl.replace(
      path: '${baseUrl.path}$slash',
      queryParameters: query,
    );
  }

  Future<T> _request<T>(
    String method,
    String path, {
    Object? body,
    bool hasBody = false,
    Map<String, String>? query,
    required T Function(Object? json) decode,
  }) async {
    final url = _url(path, query);
    final sends = hasBody || body != null;
    final encoded = sends ? encodeWire(body) : null;
    // Read for this request: nothing from an earlier one is reused.
    final authHeaders = await readAuthHeaders(_auth);
    final headers = {
      'accept': 'application/json',
      if (sends) 'content-type': 'application/json',
      ..._headers,
      ...authHeaders,
    };
    final secrets = [
      for (final value in [..._headers.values, ...authHeaders.values]) ...[
        value,
        // A server may echo a token without its scheme (`Bearer abc`).
        if (value.contains(' ')) value.substring(value.indexOf(' ') + 1),
      ],
    ];
    String clean(String text) => redactText(text, secrets: secrets);

    final http.Response response;
    try {
      final request = http.Request(method, url)..headers.addAll(headers);
      if (encoded != null) request.body = encoded;
      response = await http.Response.fromStream(await _client.send(request));
    } on Exception catch (error) {
      // A cancellation ends the request; it is not a network failure.
      if (error is CrdtError && error.code == CrdtErrorCode.cancelled) {
        rethrow;
      }
      final text = error is CrdtError ? error.message : '$error';
      throw NetworkError(
        'Room API $method $path failed: ${clean(text)}',
        cause: _Redacted(error.runtimeType, clean(text)),
      );
    }

    final text = utf8.decode(response.bodyBytes, allowMalformed: true);
    final status = response.statusCode;
    if (status < 200 || status >= 300) {
      final shown = clean(text);
      final reason = response.reasonPhrase;
      throw TransportError(
        'Room API error: $status${reason == null || reason.isEmpty ? '' : ' $reason'}'
        '${shown.isEmpty ? '' : ': $shown'}',
        statusCode: status,
        body: _decodeBody(shown),
        headers: response.headers,
      );
    }
    try {
      final empty = status == 204 || text.trim().isEmpty;
      return decode(empty ? null : jsonDecode(text));
    } on FormatException catch (error) {
      throw TransportError(
        'Room API $path returned an unreadable body: ${clean(error.message)}',
        statusCode: status,
        body: clean(text),
        headers: response.headers,
      );
    }
  }

  /// The error body as JSON when it parses, as text when it does not, and
  /// null when it is empty.
  static Object? _decodeBody(String text) {
    if (text.trim().isEmpty) return null;
    try {
      return jsonDecode(text);
    } on FormatException {
      return text;
    }
  }
}

/// A stand-in for an error that may have held a secret: its type and its
/// redacted text. Stored as the `cause` of a [NetworkError] in place of the
/// raw error.
final class _Redacted {
  const _Redacted(this.type, this.text);

  final Type type;
  final String text;

  @override
  String toString() => '$type: $text';
}

/// The standard room id for a document: `table:pk`. Matches Go's
/// `DocumentRoomID`.
String documentRoomId(String table, String pk) => '$table:$pk';

/// Predefined collaboration colours.
const _collabColors = [
  '#e57373', // red
  '#81c784', // green
  '#64b5f6', // blue
  '#ffb74d', // orange
  '#ba68c8', // purple
  '#4dd0e1', // cyan
  '#fff176', // yellow
  '#f06292', // pink
  '#a1887f', // brown
  '#90a4ae', // blue-grey
  '#aed581', // light green
  '#7986cb', // indigo
  '#4db6ac', // teal
  '#ff8a65', // deep orange
  '#9575cd', // deep purple
  '#dce775', // lime
];

/// A random collaboration colour from a fixed palette, as a hex string.
/// [random] is for tests.
String randomColor([Random? random]) =>
    _collabColors[(random ?? Random()).nextInt(_collabColors.length)];
