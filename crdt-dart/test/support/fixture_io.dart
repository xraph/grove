import 'dart:io';

/// Reads a test fixture by path relative to the package root.
String readFixture(String path) => File(path).readAsStringSync();
