package crdt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTextPositionTracksEdits(t *testing.T) {
	text := NewTextState()
	_, err := text.Insert(TextRef{}, "A😀BC", "a", HLC{Timestamp: 1, NodeID: "a"})
	require.NoError(t, err)
	before, ok := text.PositionAt(2, TextBefore)
	require.True(t, ok)
	after, ok := text.PositionAt(2, TextAfter)
	require.True(t, ok)
	selection := TextSelection{Anchor: before, Focus: after}
	raw, err := json.Marshal(selection)
	require.NoError(t, err)
	var decoded TextSelection
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, selection, decoded)
	_, err = text.Insert(*after.Ref, "XY", "b", HLC{Timestamp: 2, NodeID: "b"})
	require.NoError(t, err)
	require.Equal(t, "A😀XYBC", text.Value())
	index, ok := text.ResolvePosition(before)
	require.True(t, ok)
	require.Equal(t, 4, index)
	index, ok = text.ResolvePosition(after)
	require.True(t, ok)
	require.Equal(t, 2, index)
	ref, ok := text.RefAt(1)
	require.True(t, ok)
	_, err = text.Delete(ref, 3)
	require.NoError(t, err)
	require.Equal(t, "ABC", text.Value())
	for _, position := range []TextPosition{before, after} {
		index, ok = text.ResolvePosition(position)
		require.True(t, ok)
		require.Equal(t, 1, index)
	}
}

func TestTextPositionBoundariesAndUnknownEdits(t *testing.T) {
	text := NewTextState()
	start, ok := text.PositionAt(0, TextAfter)
	require.True(t, ok)
	end, ok := text.PositionAt(0, TextBefore)
	require.True(t, ok)
	_, err := text.Insert(TextRef{}, "hello", "a", HLC{Timestamp: 1, NodeID: "a"})
	require.NoError(t, err)
	index, ok := text.ResolvePosition(start)
	require.True(t, ok)
	require.Zero(t, index)
	index, ok = text.ResolvePosition(end)
	require.True(t, ok)
	require.Equal(t, 5, index)
	for _, offset := range []int{-1, 6} {
		_, ok = text.PositionAt(offset, TextBefore)
		require.False(t, ok)
	}
	_, ok = text.PositionAt(0, "invalid")
	require.False(t, ok)
	_, ok = text.ResolvePosition(TextPosition{Side: "invalid"})
	require.False(t, ok)
	_, ok = text.ResolvePosition(TextPosition{Side: TextAfter, Ref: &TextRef{Origin: HLC{Timestamp: 99, NodeID: "missing"}}})
	require.False(t, ok)
}
