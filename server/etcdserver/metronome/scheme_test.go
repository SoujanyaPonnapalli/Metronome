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

package metronome

import (
	"testing"
)

func TestDefaultQuorumSize(t *testing.T) {
	cases := []struct{ n, want int }{
		{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {7, 4}, {9, 5},
	}
	for _, c := range cases {
		if got := DefaultQuorumSize(c.n); got != c.want {
			t.Errorf("DefaultQuorumSize(%d) = %d; want %d", c.n, got, c.want)
		}
	}
}

func TestNewSchemeValidation(t *testing.T) {
	// rejects empty members
	if _, err := NewScheme(nil, 0); err == nil {
		t.Fatal("expected error on empty nodeIDs")
	}
	// rejects K < f+1
	if _, err := NewScheme([]uint64{1, 2, 3, 4, 5}, 2); err == nil {
		t.Fatal("expected error on K=2 for N=5 (min is 3)")
	}
	// rejects K > N
	if _, err := NewScheme([]uint64{1, 2, 3}, 4); err == nil {
		t.Fatal("expected error on K=4 for N=3")
	}
	// rejects duplicates
	if _, err := NewScheme([]uint64{1, 2, 2}, 0); err == nil {
		t.Fatal("expected error on duplicate nodeIDs")
	}
	// accepts default K
	s, err := NewScheme([]uint64{3, 1, 2, 5, 4}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.QuorumSize() != 3 {
		t.Errorf("default QuorumSize for N=5 should be 3, got %d", s.QuorumSize())
	}
	// accepts K = N (behaves like standard Raft)
	if _, err := NewScheme([]uint64{1, 2, 3}, 3); err != nil {
		t.Fatalf("K=N should be accepted, got %v", err)
	}
}

// binom returns C(n, k).
func binom(n, k int) int {
	if k < 0 || k > n {
		return 0
	}
	if k > n-k {
		k = n - k
	}
	res := 1
	for i := 0; i < k; i++ {
		res = res * (n - i) / (i + 1)
	}
	return res
}

// TestShouldPersistCoverage verifies the distance-maximized schedule is
// load-balanced: over one full period (C(N,K) entries) every node persists
// an equal share C(N-1,K-1), and every persist-set is exactly K nodes.
func TestShouldPersistCoverage(t *testing.T) {
	nodes := []uint64{10, 20, 30, 40, 50}
	s, err := NewScheme(nodes, 3) // K=f+1=3, N=5, period C(5,3)=10
	if err != nil {
		t.Fatal(err)
	}
	period := uint64(s.Period())
	counts := map[uint64]int{}
	for idx := uint64(0); idx < period; idx++ {
		ps := s.PersistSet(idx)
		if len(ps) != s.QuorumSize() {
			t.Fatalf("persist-set at %d has %d members, want K=%d", idx, len(ps), s.QuorumSize())
		}
		for _, id := range ps {
			counts[id]++
		}
	}
	want := binom(s.NumNodes()-1, s.QuorumSize()-1) // each node's share per period
	for _, id := range nodes {
		if counts[id] != want {
			t.Errorf("node %d: persisted %d/period, want balanced %d", id, counts[id], want)
		}
	}
}

// TestPersistSetSpread verifies the two things that matter for the
// distance-maximized ordering: (1) the durability invariant — every
// persist-set is exactly K distinct nodes (a majority, so every entry has
// K>=f+1 persisters); and (2) the spread — the mean overlap between
// consecutive persist-sets is BELOW the naive index%N rotation's K-1, i.e.
// the ordering actually keeps consecutive entries off the same nodes (the
// regression we are fixing).
func TestPersistSetSpread(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	s, _ := NewScheme(nodes, 3) // K=3, N=5
	period := s.Period()

	for a := 0; a < period; a++ {
		ps := s.PersistSet(uint64(a))
		if len(ps) != s.QuorumSize() {
			t.Fatalf("persist-set %d size %d, want K=%d", a, len(ps), s.QuorumSize())
		}
		seen := map[uint64]bool{}
		for _, id := range ps {
			if seen[id] {
				t.Fatalf("duplicate node in persist-set %d", a)
			}
			seen[id] = true
		}
	}

	total, pairs := 0, 0
	for a := 0; a < period; a++ {
		b := (a + 1) % period
		setA := map[uint64]bool{}
		for _, id := range s.PersistSet(uint64(a)) {
			setA[id] = true
		}
		ov := 0
		for _, id := range s.PersistSet(uint64(b)) {
			if setA[id] {
				ov++
			}
		}
		total += ov
		pairs++
	}
	mean := float64(total) / float64(pairs)
	if mean >= float64(s.QuorumSize()-1) {
		t.Errorf("mean consecutive overlap %.2f is not below naive K-1=%d; distance ordering ineffective", mean, s.QuorumSize()-1)
	}
}

func TestShouldPersistDeterminism(t *testing.T) {
	// Sorted order ensures two nodes independently construct the same
	// scheme from the same membership.
	s1, _ := NewScheme([]uint64{3, 1, 2, 5, 4}, 3)
	s2, _ := NewScheme([]uint64{5, 4, 3, 2, 1}, 3)
	for idx := uint64(0); idx < 100; idx++ {
		for _, id := range []uint64{1, 2, 3, 4, 5} {
			if s1.ShouldPersist(id, idx) != s2.ShouldPersist(id, idx) {
				t.Fatalf("non-deterministic at idx=%d node=%d", idx, id)
			}
		}
	}
}

// TestShouldPersistKEqualsN: when K = N, every node persists every
// entry — same as standard Raft.
func TestShouldPersistKEqualsN(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	s, _ := NewScheme(nodes, 3)
	for idx := uint64(0); idx < 10; idx++ {
		for _, id := range nodes {
			if !s.ShouldPersist(id, idx) {
				t.Errorf("K=N: node %d should persist idx %d", id, idx)
			}
		}
	}
}
