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

package etcdserver

import (
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/etcdserver/metronome"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// newTestRaftNode builds a minimal raftNode with metronome enabled for
// work-stealing state-machine tests. It does NOT start raft.
func newTestRaftNode(t *testing.T, nodeID uint64, clusterIDs []uint64, wsTimeout, wsDuration time.Duration) *raftNode {
	t.Helper()
	scheme, err := metronome.NewScheme(clusterIDs, 0 /* default f+1 */)
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	rs := raft.NewMemoryStorage()
	// The oracle holds the live scheme (read by currentScheme()) and this
	// node's durable watermark. In production it's built in bootstrap before
	// StartNode; tests that build raftNode directly must seed it too, else the
	// filter sees scheme==nil and degenerates to "keep everything".
	oracle := &metronomeOracle{}
	oracle.scheme.Store(scheme)
	rn := &raftNode{
		lg: zap.NewNop(),
		raftNodeConfig: raftNodeConfig{
			lg:              zap.NewNop(),
			raftStorage:     rs,
			localID:         nodeID,
			metronomeScheme: scheme,
			metronome:       oracle,
			wsTimeout:       wsTimeout,
			wsDuration:      wsDuration,
		},
	}
	return rn
}

// seedMemoryStorage pushes contiguous dummy entries [from..to] into
// raftStorage so collectInMemoryEntries has something to return.
func seedMemoryStorage(t *testing.T, rn *raftNode, from, to uint64) {
	t.Helper()
	ents := make([]raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		ents = append(ents, raftpb.Entry{Term: 1, Index: i, Data: []byte("x")})
	}
	if err := rn.raftStorage.Append(ents); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// entries builds a slice of raftpb.Entry with contiguous indices.
func entries(from, to uint64) []raftpb.Entry {
	out := make([]raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, raftpb.Entry{Term: 1, Index: i, Data: []byte("x")})
	}
	return out
}

// ----- Basic state-machine tests -------------------------------------

func TestRecordCommitProgress_TracksAdvance(t *testing.T) {
	rn := newTestRaftNode(t, /*self=*/ 2, []uint64{1, 2, 3}, 50*time.Millisecond, time.Minute)

	// First observed commit advance sets the baseline timestamp.
	rn.recordCommitProgress(3)
	if rn.wsLastCommit != 3 || rn.wsLastAdvance.IsZero() {
		t.Fatalf("expected commit=3 + baseline set, got commit=%d zero=%v", rn.wsLastCommit, rn.wsLastAdvance.IsZero())
	}
	first := rn.wsLastAdvance

	time.Sleep(2 * time.Millisecond)

	// A further advance refreshes the stall timer.
	rn.recordCommitProgress(6)
	if rn.wsLastCommit != 6 || !rn.wsLastAdvance.After(first) {
		t.Fatalf("expected commit=6 + refreshed timer, got commit=%d", rn.wsLastCommit)
	}

	// No advance (commit unchanged) must NOT move the timer — that is what
	// lets a genuine stall accumulate toward the timeout.
	prev := rn.wsLastAdvance
	rn.recordCommitProgress(6)
	if !rn.wsLastAdvance.Equal(prev) {
		t.Fatalf("timer must not move without a commit advance")
	}
}

// countSchemeSkips mirrors maybeTriggerWorkSteal's lazy reconstruction:
// the indices in (afterCommit, last] this node is NOT assigned to persist.
func countSchemeSkips(rn *raftNode, afterCommit, last uint64) int {
	s := rn.currentScheme()
	n := 0
	for idx := afterCommit + 1; idx <= last; idx++ {
		if !s.ShouldPersist(rn.localID, idx) {
			n++
		}
	}
	return n
}

// ----- Trigger tests -------------------------------------------------

func TestMaybeTriggerWorkSteal_FiresOnStall(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3},
		/*wsTimeout=*/ 5*time.Millisecond,
		/*wsDuration=*/ 100*time.Millisecond)
	seedMemoryStorage(t, rn, 1, 10)

	// Cluster advanced at least once before (wsLastCommit set), then commit
	// stalled past the timeout with entries pending (last=10 > committed=2).
	rn.wsLastCommit = 2
	rn.wsLastAdvance = time.Now().Add(-50 * time.Millisecond) // stale
	rn.storage = &noopStorage{}

	// The flush set is reconstructed from the scheme, not a buffer.
	want := countSchemeSkips(rn, 2, 10)
	if want == 0 {
		t.Fatal("test setup: expected some scheme-skipped entries to flush")
	}

	beforeTriggered := counterValue(metronomeWorkStealsTriggered)
	beforeEntries := counterValue(metronomeWorkStealEntries)

	rn.maybeTriggerWorkSteal()

	if !rn.inWorkStealMode() {
		t.Fatalf("expected work-steal mode to be active after trigger")
	}
	if got := counterValue(metronomeWorkStealsTriggered); got != beforeTriggered+1 {
		t.Fatalf("triggered counter: before=%v after=%v", beforeTriggered, got)
	}
	if got := counterValue(metronomeWorkStealEntries); got != beforeEntries+float64(want) {
		t.Fatalf("entries counter: want +%d (scheme-skipped in (2,10]), before=%v after=%v", want, beforeEntries, got)
	}
}

// Regression guard: during the initial startup window (before any
// real commit has been observed), the timer can appear "stale"
// because leader election and initial replication take hundreds of
// ms, yet no commit has advanced yet. We must NOT fire WS in that
// window — otherwise every metronome follower immediately enters
// "log everything" mode on startup and the scheme's byte savings
// collapse to zero.
func TestMaybeTriggerWorkSteal_NoFireBeforeFirstCommit(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Millisecond, time.Minute)
	seedMemoryStorage(t, rn, 1, 5) // entries pending (last=5)
	// wsLastCommit == 0 simulates "haven't seen a commit advance yet."
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second) // would otherwise trigger
	rn.storage = &noopStorage{}
	before := counterValue(metronomeWorkStealsTriggered)
	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("WS must NOT fire before first commit observed (startup guard)")
	}
	if counterValue(metronomeWorkStealsTriggered) != before {
		t.Fatalf("trigger counter should not have moved")
	}
}

func TestMaybeTriggerWorkSteal_NoFireWhenCommitCaughtUp(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Millisecond, time.Minute)
	seedMemoryStorage(t, rn, 1, 5)
	rn.wsLastCommit = 5 // commit == last: nothing pending blocks commit
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second)
	rn.storage = &noopStorage{}
	before := counterValue(metronomeWorkStealsTriggered)
	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("should not fire when commit has caught up (no pending entries)")
	}
	if counterValue(metronomeWorkStealsTriggered) != before {
		t.Fatalf("should not have incremented trigger counter")
	}
}

func TestMaybeTriggerWorkSteal_NoFireWithinTimeout(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Second, time.Minute)
	seedMemoryStorage(t, rn, 1, 5)
	rn.wsLastCommit = 1            // pending 2..5
	rn.wsLastAdvance = time.Now()  // fresh — within timeout
	rn.storage = &noopStorage{}
	before := counterValue(metronomeWorkStealsTriggered)
	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("should not enter WS mode before timeout elapsed")
	}
	if counterValue(metronomeWorkStealsTriggered) != before {
		t.Fatalf("trigger counter should not have moved")
	}
}

func TestMaybeTriggerWorkSteal_ExitsAfterDuration(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3},
		1*time.Millisecond,
		10*time.Millisecond)
	seedMemoryStorage(t, rn, 1, 5)
	rn.wsLastCommit = 1 // prior commit observed; pending 2..5
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second)
	rn.storage = &noopStorage{}

	rn.maybeTriggerWorkSteal()
	if !rn.inWorkStealMode() {
		t.Fatalf("expected WS active")
	}

	// Wait past the WS window.
	time.Sleep(30 * time.Millisecond)

	// Simulate the stall clearing: commit advanced to the log tail, so there
	// is nothing left blocking commit. The trigger fires on a persistent
	// COMMIT stall, so once commit catches up it must exit the window and
	// revert to the shuffling scheme (not re-enter persist-everything).
	rn.wsLastCommit = 5
	rn.wsLastAdvance = time.Now()

	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("expected WS window to have elapsed and not re-fire once commit caught up")
	}
}

// ----- Filter integration test ---------------------------------------

func TestFilterMetronomeEntries_PassthroughInWSMode(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Millisecond, time.Second)
	// Force WS mode (helper keeps the atomic mirror in sync).
	rn.setWSActiveUntil(time.Now().Add(5 * time.Second))

	all := entries(10, 15)
	got := rn.filterMetronomeEntries(all)
	if len(got) != len(all) {
		t.Fatalf("in WS mode, filter should pass through all entries; got %d of %d", len(got), len(all))
	}
}

// ----- Lazy flush reconstruction -------------------------------------

// On a stall, the entries flushed are reconstructed from the scheme over
// the pending range (committed, last] — exactly this node's skipped
// indices — with no per-Ready buffer maintained.
func TestWorkSteal_FlushReconstructedFromScheme(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 5*time.Millisecond, time.Minute)
	seedMemoryStorage(t, rn, 1, 12)
	rn.wsLastCommit = 4 // pending 5..12
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second)
	rn.storage = &noopStorage{}

	want := countSchemeSkips(rn, 4, 12)
	beforeEntries := counterValue(metronomeWorkStealEntries)
	rn.maybeTriggerWorkSteal()
	if got := counterValue(metronomeWorkStealEntries); got != beforeEntries+float64(want) {
		t.Fatalf("flushed entries: want +%d (scheme-skipped in (4,12]), before=%v after=%v", want, beforeEntries, got)
	}
}

// ----- helpers -------------------------------------------------------

// noopStorage is a stub serverstorage.Storage for tests. Save accepts
// anything and returns nil.
type noopStorage struct{}

func (*noopStorage) Save(_ raftpb.HardState, _ []raftpb.Entry) error { return nil }
func (*noopStorage) SaveSnap(_ raftpb.Snapshot) error                { return nil }
func (*noopStorage) Close() error                                    { return nil }
func (*noopStorage) Release(_ raftpb.Snapshot) error                 { return nil }
func (*noopStorage) Sync() error                                     { return nil }
func (*noopStorage) MinimalEtcdVersion() *semver.Version             { return nil }

// counterValue returns the current value of a prometheus Counter.
func counterValue(c prometheus.Counter) float64 {
	return testutil.ToFloat64(c)
}
