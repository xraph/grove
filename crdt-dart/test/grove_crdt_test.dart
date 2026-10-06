import 'package:grove_crdt/grove_crdt.dart';
import 'package:test/test.dart';

void main() {
  test('exposes the x-forge-sync protocol name', () {
    expect(groveCrdtProtocol, 'grove-crdt');
  });
}
