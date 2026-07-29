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

package raft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// mockMetronome is a fully controllable MetronomeOracle for deterministic
// commit-rule tests (no timers, no network, no request timeout).
type mockMetronome struct {
	persist  func(id, index uint64) bool
	quorum   int
	selfDur  uint64
	logEvery bool
}

func (m *mockMetronome) ShouldPersist(id, index uint64) bool { return m.persist(id, index) }
func (m *mockMetronome) Quorum() int                         { return m.quorum }
func (m *mockMetronome) SelfDurable() uint64                 { return m.selfDur }
func (m *mockMetronome) SelfLogEverything() bool             { return m.logEvery }
func (m *mockMetronome) StallTicks() int                     { return 100 }
func (m *mockMetronome) WindowTicks() int                    { return 6000 }

// newMetronomeLeader returns node 1 as leader of {1,2,3} with the log holding
// entries [1..n] all at term 1 (index 1 is the become-leader no-op), committed
// at 0, and the given oracle wired in.
func newMetronomeLeader(t *testing.T, m *mockMetronome, n uint64) *raft {
	t.Helper()
	cfg := newTestConfig(1, 5, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.Metronome = m
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader() // appends the no-op at index 1, term 1
	for i := uint64(1); i < n; i++ {
		if !r.appendEntry(pb.Entry{Data: []byte("x")}) {
			t.Fatalf("appendEntry %d rejected", i+1)
		}
	}
	if got := r.raftLog.lastIndex(); got != n {
		t.Fatalf("setup: lastIndex=%d want %d", got, n)
	}
	if r.raftLog.committed != 0 {
		t.Fatalf("setup: committed=%d want 0", r.raftLog.committed)
	}
	return r
}

// setDurable sets a voter's reported durable watermark (term 1, matching the
// log) used by the per-entry rule.
func setDurable(r *raft, id, durable uint64) {
	pr := r.trk.Progress[id]
	pr.Durable = durable
	pr.DurableTerm = 1
	pr.Match = durable // keep Match consistent for the standard-commit fallback
}

// TestMetronomeCommit_Safety is the core safety test: the per-entry rule must
// NEVER advance commit past an index that does not yet have T durable copies.
func TestMetronomeCommit_Safety(t *testing.T) {
	// N=3, T=2. Persist-set: index 1 -> {1,2}, index 2 -> {2,3}, index 3 -> {1,3}.
	// Node 3 is "down": its durable watermark is frozen at 1.
	m := &mockMetronome{
		quorum: 2,
		persist: func(id, index uint64) bool {
			switch index {
			case 1:
				return id == 1 || id == 2
			case 2:
				return id == 2 || id == 3
			case 3:
				return id == 1 || id == 3
			}
			return false
		},
	}
	r := newMetronomeLeader(t, m, 3)

	// Leader (id 1) durable through 3; node 2 durable through 3; node 3 stuck at 1.
	m.selfDur = 3
	setDurable(r, 2, 3)
	setDurable(r, 3, 1)

	r.maybeCommit()

	// index 1 (P={1,2}): leader durable + node2 durable = 2 -> durable.
	// index 2 (P={2,3}): node2 durable(1), node3 stuck(0), leader NOT in P,
	//   no steal -> 1 < T -> NOT durable. Commit must stop at 1.
	if r.raftLog.committed != 1 {
		t.Fatalf("SAFETY VIOLATION: committed=%d, want 1 (index 2 has only 1 durable copy)", r.raftLog.committed)
	}
}

// TestMetronomeCommit_RecoveryViaPersistEverything: an index whose persist-set
// includes a down node wedges the per-entry rule; once the leader enters
// persist-everything mode (work-steal recovery) it falls back to the standard
// rule and commit unblocks. This is the node-down scenario the integration
// tests exercise — recovery is leader-driven, with no steal-report dependence.
func TestMetronomeCommit_RecoveryViaPersistEverything(t *testing.T) {
	m := &mockMetronome{
		quorum: 2,
		persist: func(id, index uint64) bool {
			switch index {
			case 1:
				return id == 1 || id == 2
			case 2:
				return id == 2 || id == 3 // leader (1) outside; node 3 down -> wedged
			case 3:
				return id == 1 || id == 2
			}
			return false
		},
	}
	r := newMetronomeLeader(t, m, 3)
	m.selfDur = 3
	r.trk.Progress[1].Match = 3 // leader holds everything in memory
	setDurable(r, 2, 3)
	setDurable(r, 3, 1) // node 3 down

	// Steady-state per-entry rule: index 2 has only 1 durable copy -> wedged at 1.
	r.maybeCommit()
	if r.raftLog.committed != 1 {
		t.Fatalf("pre-recovery: committed=%d want 1", r.raftLog.committed)
	}

	// Leader enters persist-everything (work-steal) -> standard commit unblocks.
	m.logEvery = true
	r.maybeCommit()
	if r.raftLog.committed != 3 {
		t.Fatalf("post-recovery: committed=%d want 3 (persist-everything fallback should unblock)", r.raftLog.committed)
	}
}

// TestMetronomeCommit_LogEverythingFallback: when this node is in persist-
// everything mode, commit falls back to the standard median-of-Match rule.
func TestMetronomeCommit_LogEverythingFallback(t *testing.T) {
	m := &mockMetronome{
		quorum:   2,
		logEvery: true, // recovery mode -> standard commit
		persist:  func(id, index uint64) bool { return false /* irrelevant in fallback */ },
	}
	r := newMetronomeLeader(t, m, 3)

	// Standard commit = median of Match. Leader=3, node2=3, node3=1(down).
	// sorted [1,3,3], quorum position -> 3. Commit should reach 3.
	r.trk.Progress[1].Match = 3
	r.trk.Progress[2].Match = 3
	r.trk.Progress[3].Match = 1
	r.maybeCommit()
	if r.raftLog.committed != 3 {
		t.Fatalf("fallback: committed=%d want 3 (standard median commit)", r.raftLog.committed)
	}
}
