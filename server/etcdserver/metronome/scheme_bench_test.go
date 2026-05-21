// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metronome

import "testing"

// shouldPersistLoop is the previous O(K) implementation, kept here as a
// baseline for benchmarking against the new O(1) ShouldPersist that uses
// the precomputed positionOf map.
func (s *Scheme) shouldPersistLoop(nodeID uint64, index uint64) bool {
	n := len(s.nodeIDs)
	start := int(index % uint64(n))
	for i := 0; i < s.quorumSize; i++ {
		if s.nodeIDs[(start+i)%n] == nodeID {
			return true
		}
	}
	return false
}

// raftpb-free entry stub for benchmarking the filter loop without
// pulling raft into the metronome package.
type benchEntry struct {
	Index   uint64
	IsConfC bool
}

// filterReordered is the new ordering: ShouldPersist first, then the
// (rare) ConfChange-fallthrough. Mirrors the production filter loop in
// raft.go.
func filterReordered(s *Scheme, localID uint64, ents []benchEntry, out []benchEntry) []benchEntry {
	for i := range ents {
		e := &ents[i]
		if s.ShouldPersist(localID, e.Index) || e.IsConfC {
			out = append(out, *e)
		}
	}
	return out
}

// filterOriginal is the pre-reorder loop: ConfChange checks first, then
// ShouldPersist. The common path pays 2 wasted type comparisons per
// kept normal entry.
func filterOriginal(s *Scheme, localID uint64, ents []benchEntry, out []benchEntry) []benchEntry {
	for i := range ents {
		e := &ents[i]
		if e.IsConfC || s.ShouldPersist(localID, e.Index) {
			out = append(out, *e)
		}
	}
	return out
}

// BenchmarkFilter measures the full per-Ready filter cost (the function
// that runs once per Ready in the etcdserver hot path) for the new
// ordering vs the previous ordering. Each iteration filters a 50-entry
// Ready, which approximates our v=4096 c=200 steady-state batch size.
func BenchmarkFilter(b *testing.B) {
	for _, cfg := range []struct {
		name   string
		ids    []uint64
		quorum int
		me     uint64
	}{
		{"N=3_K=2", []uint64{10, 20, 30}, 2, 20},
		{"N=5_K=3", []uint64{10, 20, 30, 40, 50}, 3, 30},
		{"N=7_K=4", []uint64{10, 20, 30, 40, 50, 60, 70}, 4, 40},
	} {
		s, err := NewScheme(cfg.ids, cfg.quorum)
		if err != nil {
			b.Fatal(err)
		}
		const batchSize = 50
		ents := make([]benchEntry, batchSize)
		for i := range ents {
			ents[i] = benchEntry{Index: uint64(1000 + i)}
		}
		out := make([]benchEntry, 0, batchSize)
		b.Run(cfg.name+"_reordered", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out = filterReordered(s, cfg.me, ents, out[:0])
			}
		})
		b.Run(cfg.name+"_original", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out = filterOriginal(s, cfg.me, ents, out[:0])
			}
		})
	}
}

// BenchmarkShouldPersist measures the hot-path filter cost. Run with:
//   go test -bench=BenchmarkShouldPersist -benchmem ./etcdserver/metronome
//
// The /opt subtests use the production O(1) impl; /loop subtests use the
// previous O(K) implementation. Same workload, same test inputs.
func BenchmarkShouldPersist(b *testing.B) {
	for _, c := range []struct {
		name      string
		ids       []uint64
		quorum    int
		queryNode uint64
	}{
		{"N=3_K=2", []uint64{10, 20, 30}, 2, 20},
		{"N=5_K=3", []uint64{10, 20, 30, 40, 50}, 3, 30},
		{"N=7_K=4", []uint64{10, 20, 30, 40, 50, 60, 70}, 4, 40},
	} {
		s, err := NewScheme(c.ids, c.quorum)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(c.name+"_opt", func(b *testing.B) {
			b.ReportAllocs()
			var sink bool
			for i := 0; i < b.N; i++ {
				sink = s.ShouldPersist(c.queryNode, uint64(i))
			}
			_ = sink
		})
		b.Run(c.name+"_loop", func(b *testing.B) {
			b.ReportAllocs()
			var sink bool
			for i := 0; i < b.N; i++ {
				sink = s.shouldPersistLoop(c.queryNode, uint64(i))
			}
			_ = sink
		})
	}
}
