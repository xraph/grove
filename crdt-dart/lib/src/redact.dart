/// Removing secrets from text that may reach a log or an error message. Not
/// part of the public API.
///
/// The WebSocket transport and the room client both build error text from
/// URLs and server answers, and both go through here, so there is one rule for
/// what a redacted message looks like.
library;

/// Matches a URL in free text: a scheme, `://`, then anything up to a space or
/// a quote.
final _urlPattern = RegExp(r'''[A-Za-z][A-Za-z0-9+.\-]*://[^\s'"<>]+''');

/// [url] without credentials or fragment, rebuilt with every query value
/// replaced by `REDACTED`.
String redactUrl(Uri url) {
  final port = url.hasPort ? ':${url.port}' : '';
  final keys = url.hasQuery && url.query.isNotEmpty
      ? url.queryParametersAll.keys
      : const <String>[];
  final query = keys.isEmpty
      ? ''
      : '?${[for (final k in keys) '${Uri.encodeQueryComponent(k)}=REDACTED'].join('&')}';
  return '${url.scheme}://${url.host}$port${url.path}$query';
}

/// [text] with every secret removed.
///
/// First by parsing: each URL in the text is rebuilt with every query value
/// replaced by `REDACTED` and its credentials dropped, so a URL the caller has
/// never seen is covered. Then the known [secrets] are removed wherever they
/// still appear, case-insensitively, raw and form-encoded, longest first so
/// that a secret that contains another is removed whole. Secrets shorter than
/// three characters are not removed from free text.
String redactText(String text, {Iterable<String> secrets = const []}) {
  var out = text.replaceAllMapped(_urlPattern, (m) {
    final raw = m[0]!;
    final core = raw.replaceFirst(RegExp(r'[.,;:)\]}]+$'), '');
    final uri = Uri.tryParse(core);
    final redacted = uri == null ? '<url REDACTED>' : redactUrl(uri);
    return '$redacted${raw.substring(core.length)}';
  });
  final known = {...secrets}.where((s) => s.length >= 3);
  final forms = <String>{
    for (final s in known) ...{
      s,
      Uri.encodeQueryComponent(s),
      Uri.encodeComponent(s),
    },
  }.toList()..sort((a, b) => b.length.compareTo(a.length));
  for (final form in forms) {
    out = out.replaceAll(
      RegExp(RegExp.escape(form), caseSensitive: false),
      'REDACTED',
    );
  }
  return out;
}
