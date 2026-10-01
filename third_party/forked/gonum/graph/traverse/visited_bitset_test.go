/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package traverse

import (
	"math/bits"
	"math/rand"
	"testing"
)

func TestVisitedBitset(t *testing.T) {
	testcases := []struct {
		name string
		ids  []int
	}{
		{name: "empty"},
		{name: "single", ids: []int{0}},
		// Stays inline.
		{name: "below promotion threshold", ids: []int{7, 3, 64, 65, 1000000}},
		// Crosses over into the bitset.
		{name: "above promotion threshold", ids: seq(visitedBitsetInlineIDs * 4)},
		{name: "descending", ids: reverse(seq(visitedBitsetInlineIDs * 4))},
		{name: "sparse and interleaved", ids: []int{0, 1000000, 63, 64, 999999, 1, 4096, 2, 500000, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}},
		{name: "negative", ids: append(seq(visitedBitsetInlineIDs*2), -1, -64, -65)},
		// IDs outside the bitset range land in the overflow set, both when
		// promoting and after.
		{name: "beyond bitset range", ids: append([]int{visitedBitsetMaxID, -1}, append(seq(visitedBitsetInlineIDs*2), visitedBitsetMaxID-1, 1<<40)...)},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var s visitedBitset
			for i, id := range tc.ids {
				if s.has(id) {
					t.Fatalf("id %d reported visited before insert", id)
				}
				s.insert(id)
				for _, inserted := range tc.ids[:i+1] {
					if !s.has(inserted) {
						t.Fatalf("id %d reported unvisited after inserting %d", inserted, id)
					}
				}
			}
			if id := maxID(tc.ids) + 1; s.has(id) {
				t.Errorf("id %d reported visited but was never inserted", id)
			}

			s.clear()
			for _, id := range tc.ids {
				if s.has(id) {
					t.Fatalf("id %d still reported visited after clear", id)
				}
			}
		})
	}
}

func TestVisitedBitsetDuplicateInsert(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "below promotion threshold", size: 1},
		{name: "at promotion threshold", size: visitedBitsetInlineIDs},
		{name: "above promotion threshold", size: visitedBitsetInlineIDs + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s visitedBitset
			ids := seq(tc.size)
			for pass := 0; pass < 2; pass++ {
				for i := 0; i <= visitedBitsetInlineIDs+1; i++ {
					for _, id := range ids {
						s.insert(id)
					}
				}
				// A cleared bitset stays allocated, so only the first pass can
				// still be inline.
				if s.bits == nil {
					if s.n != tc.size {
						t.Fatalf("pass %d: duplicate inserts left %d inline entries, want %d", pass, s.n, tc.size)
					}
				} else if got := popcount(s.bits); got != tc.size {
					t.Fatalf("pass %d: bitset has %d bits set, want %d", pass, got, tc.size)
				}
				if pass == 0 && (s.bits != nil) != (tc.size > visitedBitsetInlineIDs) {
					t.Fatalf("pass %d: bitset allocated = %t with %d distinct IDs", pass, s.bits != nil, tc.size)
				}
				for _, id := range ids {
					if !s.has(id) {
						t.Fatalf("pass %d: id %d missing after duplicate inserts", pass, id)
					}
				}
				s.clear()
			}
		})
	}
}

// TestVisitedBitsetReuse checks that a cleared set keeps its bitset and
// handles IDs both inside and beyond its current size.
func TestVisitedBitsetReuse(t *testing.T) {
	var s visitedBitset
	for _, id := range seq(visitedBitsetInlineIDs + 1) {
		s.insert(id)
	}
	words := len(s.bits)
	s.clear()
	if len(s.bits) != words {
		t.Fatalf("clear changed bitset length from %d to %d words", words, len(s.bits))
	}
	if got := popcount(s.bits); got != 0 {
		t.Fatalf("bitset has %d bits set after clear", got)
	}
	for _, id := range []int{1, words * 64, words*64*3 + 5} {
		if s.has(id) {
			t.Fatalf("id %d reported visited before insert", id)
		}
		s.insert(id)
		if !s.has(id) {
			t.Fatalf("id %d reported unvisited after insert", id)
		}
	}
}

// TestVisitedBitsetMatchesReference checks the bitset against a plain map
// for a random ID mix, including IDs routed to the overflow set.
func TestVisitedBitsetMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	var s visitedBitset
	want := map[int]bool{}
	for i := 0; i < 10000; i++ {
		id := r.Intn(1 << 20)
		if i%10 == 0 {
			id = visitedBitsetMaxID + r.Intn(1<<20)
		}
		if got := s.has(id); got != want[id] {
			t.Fatalf("has(%d) = %t, want %t", id, got, want[id])
		}
		s.insert(id)
		want[id] = true
	}
}

func popcount(words []uint64) int {
	n := 0
	for _, w := range words {
		n += bits.OnesCount64(w)
	}
	return n
}
