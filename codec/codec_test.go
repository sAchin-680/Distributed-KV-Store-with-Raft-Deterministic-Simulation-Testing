package codec_test

import (
	"testing"

	"github.com/sAchin-680/raftkv/codec"
	"github.com/sAchin-680/raftkv/raft"
)

// Conversion code is where a forgotten field becomes silent data loss.
//
// It compiles fine, the tests that do not look at that field pass, and the value
// simply arrives as zero on the other side — a command with no payload, a
// configuration with no learners, a vote for node 0. Round-tripping every field
// with a distinctive non-zero value is the cheapest guard against that.

func TestEntryRoundTripsEveryField(t *testing.T) {
	for _, want := range []raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Command: []byte("set x=1")},
		{Index: 99, Term: 42, Type: raft.EntryNoOp},
		{Index: 7, Term: 3, Type: raft.EntryConfChange, Command: []byte("membership")},
		{}, // the zero entry must survive too
	} {
		raw, err := codec.MarshalEntry(want)
		if err != nil {
			t.Fatalf("MarshalEntry(%s): %v", want, err)
		}
		got, err := codec.UnmarshalEntry(raw)
		if err != nil {
			t.Fatalf("UnmarshalEntry: %v", err)
		}

		if got.Index != want.Index {
			t.Errorf("index %d, want %d", got.Index, want.Index)
		}
		if got.Term != want.Term {
			t.Errorf("term %d, want %d", got.Term, want.Term)
		}
		if got.Type != want.Type {
			t.Errorf("type %s, want %s", got.Type, want.Type)
		}
		if string(got.Command) != string(want.Command) {
			t.Errorf("command %q, want %q", got.Command, want.Command)
		}
	}
}

func TestHardStateRoundTripsEveryField(t *testing.T) {
	for _, want := range []raft.HardState{
		{Term: 7, Vote: 3, Commit: 1024},
		{Term: 1, Vote: 0, Commit: 0},
		{},
		{Term: 1 << 40, Vote: 1 << 20, Commit: 1 << 50}, // large values still fit
	} {
		got, err := codec.UnmarshalHardState(codec.MarshalHardState(want))
		if err != nil {
			t.Fatalf("UnmarshalHardState(%s): %v", want, err)
		}
		if got != want {
			t.Errorf("round trip: got %s, want %s", got, want)
		}
	}
}

func TestTruncatedHardStateIsAnError(t *testing.T) {
	raw := codec.MarshalHardState(raft.HardState{Term: 300, Vote: 300, Commit: 300})
	if _, err := codec.UnmarshalHardState(raw[:1]); err == nil {
		t.Error("a truncated hard state should be an error, not a silently zeroed one — " +
			"a node that reads its vote as 0 will vote again in the same term")
	}
}

func TestConfigurationRoundTripsEveryField(t *testing.T) {
	want := raft.Configuration{
		Voters:    []raft.NodeID{2, 3, 4},
		OldVoters: []raft.NodeID{1, 2, 3},
		Learners:  []raft.NodeID{9},
	}

	got := codec.ConfigurationFromProto(codec.ConfigurationToProto(want))
	if !got.Equal(want) {
		t.Errorf("round trip: got %s, want %s", got, want)
	}
	if !got.IsJoint() {
		t.Error("the joint half was lost; a membership change would silently " +
			"become a single-step change, which is the failure joint consensus exists to prevent")
	}
}

// Whatever arrives on the wire must come out normalized, because the core
// derives ordered decisions from these slices and unsorted input would make its
// behaviour depend on what a peer happened to send.
func TestConfigurationIsNormalizedOnDecode(t *testing.T) {
	scrambled := raft.Configuration{Voters: []raft.NodeID{3, 1, 2, 1}}
	got := codec.ConfigurationFromProto(codec.ConfigurationToProto(scrambled))

	if len(got.Voters) != 3 {
		t.Fatalf("voters = %v, want three after deduplication", got.Voters)
	}
	for i := 1; i < len(got.Voters); i++ {
		if got.Voters[i] <= got.Voters[i-1] {
			t.Fatalf("voters = %v, want them sorted", got.Voters)
		}
	}
}

func TestSnapshotRoundTripsEveryField(t *testing.T) {
	want := raft.Snapshot{
		LastIncludedIndex: 4096,
		LastIncludedTerm:  12,
		Config:            raft.NewConfiguration([]raft.NodeID{1, 2, 3}, 7),
		Data:              []byte("serialized state machine"),
	}

	raw, err := codec.MarshalSnapshot(want)
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	got, err := codec.UnmarshalSnapshot(raw)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}

	if got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm {
		t.Errorf("boundary: got %d@%d, want %d@%d",
			got.LastIncludedIndex, got.LastIncludedTerm,
			want.LastIncludedIndex, want.LastIncludedTerm)
	}
	if string(got.Data) != string(want.Data) {
		t.Errorf("data %q, want %q", got.Data, want.Data)
	}
	// The one that would be easy to drop and hard to notice.
	if !got.Config.Equal(want.Config) {
		t.Errorf("configuration %s, want %s — without it a node restoring from "+
			"this snapshot does not know who its peers are", got.Config, want.Config)
	}
}

func TestConfChangeRoundTrips(t *testing.T) {
	for _, want := range []raft.ConfChange{
		{Kind: raft.ConfChangeEnterJoint, Config: raft.Configuration{
			Voters: []raft.NodeID{2, 3, 4}, OldVoters: []raft.NodeID{1, 2, 3},
		}},
		{Kind: raft.ConfChangeLeaveJoint, Config: raft.NewConfiguration([]raft.NodeID{2, 3, 4})},
	} {
		raw, err := codec.MarshalConfChange(want)
		if err != nil {
			t.Fatalf("MarshalConfChange: %v", err)
		}
		got, err := codec.UnmarshalConfChange(raw)
		if err != nil {
			t.Fatalf("UnmarshalConfChange: %v", err)
		}
		if got.Kind != want.Kind {
			t.Errorf("kind %s, want %s", got.Kind, want.Kind)
		}
		if !got.Config.Equal(want.Config) {
			t.Errorf("config %s, want %s", got.Config, want.Config)
		}
	}
}

// A log written by a newer version may contain entry types this build does not
// know. Refusing to read it would turn a rolling upgrade into an outage, so an
// unknown type decodes as a normal entry rather than failing.
func TestUnknownEntryTypeDecodesAsNormal(t *testing.T) {
	raw, err := codec.MarshalEntry(raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryType(99)})
	if err != nil {
		t.Fatalf("MarshalEntry: %v", err)
	}
	got, err := codec.UnmarshalEntry(raw)
	if err != nil {
		t.Fatalf("UnmarshalEntry: %v", err)
	}
	if got.Type != raft.EntryNormal {
		t.Errorf("type = %s, want it to degrade to normal", got.Type)
	}
}
