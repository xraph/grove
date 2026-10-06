import 'dart:js_interop';

@JS('process.cwd')
external JSString _cwd();

@JS('require')
external JSFunction? get _require;

@JS('process.mainModule.require')
external JSFunction? get _mainRequire;

@JS()
@staticInterop
class _Fs {}

extension on _Fs {
  external JSString readFileSync(JSString path, JSString encoding);
}

/// Reads a test fixture by path relative to the package root, through node's
/// `fs`, because `dart:io` is unavailable on the node platform.
String readFixture(String path) {
  final req = _require ?? _mainRequire;
  if (req == null) throw StateError('no node require available');
  final fs = req.callAsFunction(null, 'fs'.toJS)! as _Fs;
  return fs.readFileSync('${_cwd().toDart}/$path'.toJS, 'utf8'.toJS).toDart;
}
