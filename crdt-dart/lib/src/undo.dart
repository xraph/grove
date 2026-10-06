import 'package:meta/meta.dart';

import 'types.dart';

/// One undo or redo entry: a change and the state it replaced. Port of crdt-js
/// `UndoEntry`.
@immutable
final class UndoEntry {
  /// Creates an entry.
  const UndoEntry({
    required this.change,
    required this.previousState,
    required this.timestamp,
    this.previousDocument,
  });

  /// The change that was applied.
  final ChangeRecord change;

  /// The field state before the change was applied, or null when the field did
  /// not exist.
  final FieldState? previousState;

  /// The whole document before a `deleteDocument`, or null for any other
  /// change. Dart only: undoing an unpushed delete restores the document
  /// exactly, where the field snapshot alone could not.
  final DocumentState? previousDocument;

  /// When the change was recorded, in milliseconds since the Unix epoch.
  final int timestamp;
}

/// Undo and redo stacks for CRDT field mutations. Port of crdt-js
/// `UndoManager`.
///
/// Before a local change, snapshot the field state and [record] the change with
/// it. [undo] pops the last entry and the caller restores its state. [redo]
/// pops an undone entry back. A new [record] clears the redo stack, which is
/// the usual undo semantics.
final class UndoManager {
  /// Creates a manager that keeps at most [maxHistory] undo entries.
  ///
  /// Throws a [RangeError] when [maxHistory] is negative.
  UndoManager({this.maxHistory = 100}) {
    RangeError.checkNotNegative(maxHistory, 'maxHistory');
  }

  /// The most undo entries kept. The oldest go first.
  final int maxHistory;

  final List<UndoEntry> _undo = [];
  final List<UndoEntry> _redo = [];

  /// Records [change] and the state it replaced. Clears the redo stack.
  ///
  /// Pass [previousDocument] for a `deleteDocument`.
  void record(ChangeRecord change, FieldState? previousState, {DocumentState? previousDocument}) {
    _undo.add(UndoEntry(
      change: change,
      previousState: previousState,
      previousDocument: previousDocument,
      timestamp: DateTime.now().millisecondsSinceEpoch,
    ));
    _trim(_undo);
    _redo.clear();
  }

  void _trim(List<UndoEntry> stack) {
    if (stack.length > maxHistory) stack.removeRange(0, stack.length - maxHistory);
  }

  /// Whether [undo] has an entry.
  bool get canUndo => _undo.isNotEmpty;

  /// Whether [redo] has an entry.
  bool get canRedo => _redo.isNotEmpty;

  /// How many entries [undo] can pop.
  int get undoCount => _undo.length;

  /// How many entries [redo] can pop.
  int get redoCount => _redo.length;

  /// Pops the last entry from the undo stack, pushes it on the redo stack and
  /// returns it so the caller can restore its state. Returns null when there
  /// is nothing to undo.
  UndoEntry? undo() {
    if (_undo.isEmpty) return null;
    final entry = _undo.removeLast();
    _redo.add(entry);
    return entry;
  }

  /// Pops the last entry from the redo stack, pushes it on the undo stack and
  /// returns it so the caller can re-apply the change. Returns null when there
  /// is nothing to redo.
  UndoEntry? redo() {
    if (_redo.isEmpty) return null;
    final entry = _redo.removeLast();
    _undo.add(entry);
    _trim(_undo);
    return entry;
  }

  /// Pops the last entry from the undo stack WITHOUT putting it on the redo
  /// stack, and returns it. Returns null when there is nothing to undo.
  ///
  /// For a store that cannot offer the entry for redo: the change it holds was
  /// never pushed, so undoing it discards it. Use [pushRedo] afterwards if a
  /// different entry should be redoable.
  UndoEntry? popUndo() => _undo.isEmpty ? null : _undo.removeLast();

  /// Puts [entry] on the redo stack without touching the undo stack or
  /// clearing redo.
  ///
  /// For a store whose undo works through a compensating change. It pops the
  /// entry with [popUndo], applies the compensation, and pushes the entry that
  /// redoes it. Using [undo] instead would leave the popped entry on the redo
  /// stack too, and one undo would then take two redos.
  void pushRedo(UndoEntry entry) {
    _redo.add(entry);
    _trim(_redo);
  }

  /// Clears both stacks.
  void clear() {
    _undo.clear();
    _redo.clear();
  }
}
