// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Tests that lock in the leader-shuffles-too contract.
//
// These tests are deliberately written BEFORE the implementation change.
// In PR-1 (this file) we ship the harness + the failing assertions so
// the contract is auditable. PR-2 flips the leader-bypass in
// entriesToPersist() and these tests become green.
//
// Naming: TestLeaderShuffles_* so the suite can be selected with
//   go test -run 'TestLeaderShuffles_'

package etcdserver

import (
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// ---- helpers -----------------------------------------------------------

// confChangeEntry returns a raftpb.Entry tagged as a ConfChange so we can
// verify ConfChange entries are kept regardless of persist-set membership.
func confChangeEntry(index uint64) raftpb.Entry {
	return raftpb.Entry{
		Term:  1,
		Index: index,
		Type:  raftpb.EntryConfChange,
		Data:  []byte("conf-change"),
	}
}

// hasIndex returns true if ents contains an entry at the given index.
func hasIndex(ents []raftpb.Entry, idx uint64) bool {
	for _, e := range ents {
		if e.Index == idx {
			return true
		}
	}
	return false
}

// indicesOf returns the index list of the entries (for diagnostic output).
func indicesOf(ents []raftpb.Entry) []uint64 {
	out := make([]uint64, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Index)
	}
	return out
}

// ---- T1: leader's entries SHOULD be filtered per persist-set -----------

// TestLeaderShuffles_LeaderEntriesAreFiltered drives a synthetic raftNode
// where the local node is the leader, and asserts entriesToPersist
// returns *exactly* the indices whose persist-set (per the scheme)
// contains localID. Expectations are derived from the scheme itself
// rather than hand-coded so this test is robust to the rotation's
// off-by-one conventions (offset, sort order, etc).
//
// Pre-PR-2 this test fails because the leader-bypass returns the full
// `ents` slice. After PR-2 it should report identical inclusion as
// `Scheme.ShouldPersist`.
func TestLeaderShuffles_LeaderEntriesAreFiltered(t *testing.T) {
	rn := newTestRaftNode(t, /*self=*/ 2, []uint64{1, 2, 3},
		50*time.Millisecond, time.Minute)

	all := entries(1, 12)
	got := rn.entriesToPersist(all, /*islead=*/ true)

	// Build the expected set from the scheme directly.
	var wantKeep, wantSkip []uint64
	for _, e := range all {
		if rn.metronomeScheme.ShouldPersist(2, e.Index) {
			wantKeep = append(wantKeep, e.Index)
		} else {
			wantSkip = append(wantSkip, e.Index)
		}
	}
	if len(wantKeep) == 0 || len(wantSkip) == 0 {
		t.Fatalf("test fixture: expected the rotation to produce BOTH kept and "+
			"skipped indices across [1..12]; got kept=%v skip=%v — adjust the "+
			"span or scheme so the test is meaningful", wantKeep, wantSkip)
	}

	for _, idx := range wantKeep {
		if !hasIndex(got, idx) {
			t.Errorf("leader: expected index %d (in persist-set of localID=2) to be "+
				"PERSISTED, but it was skipped. got=%v "+
				"(entriesToPersist still has the leader-bypass — see PR-2)",
				idx, indicesOf(got))
		}
	}
	for _, idx := range wantSkip {
		if hasIndex(got, idx) {
			t.Errorf("leader: expected index %d (NOT in persist-set of localID=2) to be "+
				"SKIPPED, but it was persisted. got=%v "+
				"(entriesToPersist still has the leader-bypass — see PR-2)",
				idx, indicesOf(got))
		}
	}
	if len(got) != len(wantKeep) {
		t.Errorf("leader: expected exactly %d entries kept; got %d (%v)",
			len(wantKeep), len(got), indicesOf(got))
	}
}

// ---- T2: same logic, different localID position ----------------------

// TestLeaderShuffles_LeaderRotationByID verifies the rotation produces
// the expected K/N persist count regardless of which voter ID is the
// leader. We sweep localID over {1,2,3} so any off-by-one in the
// rotation surfaces.
func TestLeaderShuffles_LeaderRotationByID(t *testing.T) {
	ids := []uint64{1, 2, 3}
	const N, K = 3, 2
	const span uint64 = 12 // multiple of N so each pid sees a clean K/N
	for _, self := range ids {
		t.Run("self="+itoa(self), func(t *testing.T) {
			rn := newTestRaftNode(t, self, ids,
				50*time.Millisecond, time.Minute)
			got := rn.entriesToPersist(entries(1, span), /*islead=*/ true)
			// Each pid is in exactly K/N of the indices. For span=12,
			// expect 12*K/N = 8 kept.
			expected := int(span * K / N)
			if len(got) != expected {
				t.Errorf("self=%d (leader): expected %d kept of %d (K/N=%d/%d); got %d kept (%v). "+
					"Leader-bypass not yet removed — see PR-2.",
					self, expected, span, K, N, len(got), indicesOf(got))
			}
			// Each kept entry must actually be in the persist-set for this self.
			for _, e := range got {
				if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
					continue
				}
				if !rn.metronomeScheme.ShouldPersist(self, e.Index) {
					t.Errorf("self=%d: entry %d was kept but is NOT in this node's persist-set",
						self, e.Index)
				}
			}
		})
	}
}

// ---- T3: ConfChange entries are always kept (even for leader-skipped indices) ----

// TestLeaderShuffles_ConfChangeAlwaysKept inserts ConfChange entries at
// indices that would be SKIPPED by the leader's persist-set rotation,
// and verifies the filter still keeps them. This invariant must survive
// the bypass removal.
func TestLeaderShuffles_ConfChangeAlwaysKept(t *testing.T) {
	rn := newTestRaftNode(t, /*self=*/ 2, []uint64{1, 2, 3},
		50*time.Millisecond, time.Minute)

	// index 3 and 6 are NOT in localID=2's persist-set (per T1 rotation).
	// Put ConfChange entries at exactly those indices.
	all := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte("x")},
		{Term: 1, Index: 2, Data: []byte("x")},
		confChangeEntry(3), // would be SKIPPED if it were a normal entry
		{Term: 1, Index: 4, Data: []byte("x")},
		{Term: 1, Index: 5, Data: []byte("x")},
		confChangeEntry(6), // would be SKIPPED if it were a normal entry
	}

	got := rn.entriesToPersist(all, /*islead=*/ true)

	if !hasIndex(got, 3) {
		t.Errorf("ConfChange at index 3 was filtered out; ConfChange entries MUST always persist. "+
			"got=%v", indicesOf(got))
	}
	if !hasIndex(got, 6) {
		t.Errorf("ConfChange at index 6 was filtered out; ConfChange entries MUST always persist. "+
			"got=%v", indicesOf(got))
	}
}

// ---- T4: empty ents slice is a no-op ----------------------------------

// TestLeaderShuffles_EmptyEntriesNoop verifies that a Ready with no
// entries doesn't crash the filter and returns an empty slice
// (HardState-only Readies happen frequently and must be cheap).
func TestLeaderShuffles_EmptyEntriesNoop(t *testing.T) {
	rn := newTestRaftNode(t, /*self=*/ 2, []uint64{1, 2, 3},
		50*time.Millisecond, time.Minute)

	got := rn.entriesToPersist(nil, /*islead=*/ true)
	if len(got) != 0 {
		t.Errorf("expected empty result for nil ents; got %v", indicesOf(got))
	}

	got = rn.entriesToPersist([]raftpb.Entry{}, /*islead=*/ false)
	if len(got) != 0 {
		t.Errorf("expected empty result for empty ents; got %v", indicesOf(got))
	}
}

// ---- T5: follower behavior unchanged (regression guard) ---------------

// TestLeaderShuffles_FollowerUnchanged asserts that follower filtering is
// already correct in the canonical commit, so the leader-bypass removal
// must not regress it.
func TestLeaderShuffles_FollowerUnchanged(t *testing.T) {
	rn := newTestRaftNode(t, /*self=*/ 2, []uint64{1, 2, 3},
		50*time.Millisecond, time.Minute)
	got := rn.entriesToPersist(entries(1, 6), /*islead=*/ false)
	if len(got) != 4 {
		t.Errorf("follower already filters correctly today — got %d of 6 (%v)",
			len(got), indicesOf(got))
	}
}

// ---- tiny stdlib-free int-to-string for table-driven subtests --------

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	return string(buf[i:])
}
