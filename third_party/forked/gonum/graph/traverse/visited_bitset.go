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

// visitedBitset records the IDs of the nodes a traversal has already seen,
// using one bit per ID.
//
// Simple graphs hand out small, dense IDs, so a bitset sized to the largest
// visited ID is far smaller than a map and needs no hashing. Short walks stay
// in an inline array so they allocate nothing beyond the traverser itself.
// IDs a bitset cannot reasonably hold (negative, or at least
// visitedBitsetMaxID) go to a visitedSet, which ordinary graphs never reach.
//
// The zero value is ready to use.
type visitedBitset struct {
	// ids[:n] holds the visited IDs until bits takes over.
	ids [visitedBitsetInlineIDs]int
	n   int

	// bits takes over once ids is full, and from then on holds every visited
	// ID in [0, visitedBitsetMaxID). It is kept, zeroed, across clear so a
	// reused traverser does not reallocate it.
	bits []uint64
	// overflow holds IDs outside the bitset range once bits has taken over.
	overflow visitedSet
}

// visitedBitsetInlineIDs is the number of IDs kept inline before switching
// to the bitset. Most authorization traversals never reach it.
const visitedBitsetInlineIDs = 16

// visitedBitsetMaxID caps the bitset at 8 MiB so a single huge ID from a
// sparse graph cannot force a huge allocation.
const visitedBitsetMaxID = 1 << 26

func (s *visitedBitset) has(id int) bool {
	if s.bits == nil {
		for _, visited := range s.ids[:s.n] {
			if visited == id {
				return true
			}
		}
		return false
	}
	if uint(id) < visitedBitsetMaxID {
		w := id / 64
		return w < len(s.bits) && s.bits[w]&(1<<(uint(id)%64)) != 0
	}
	return s.overflow.has(id)
}

func (s *visitedBitset) insert(id int) {
	if s.bits == nil {
		if s.has(id) {
			return
		}
		if s.n < len(s.ids) {
			s.ids[s.n] = id
			s.n++
			return
		}
		s.promote()
	}
	s.insertBit(id)
}

// promote moves the inline IDs into the bitset, sizing it for the largest of
// them up front.
func (s *visitedBitset) promote() {
	largest := 0
	for _, id := range s.ids[:s.n] {
		if uint(id) < visitedBitsetMaxID {
			largest = max(largest, id)
		}
	}
	s.bits = make([]uint64, largest/64+1)
	for _, id := range s.ids[:s.n] {
		s.insertBit(id)
	}
	s.n = 0
}

func (s *visitedBitset) insertBit(id int) {
	if uint(id) >= visitedBitsetMaxID {
		s.overflow.insert(id)
		return
	}
	w := id / 64
	if w >= len(s.bits) {
		// Double so a walk climbing through IDs regrows only logarithmically.
		grown := make([]uint64, max(w+1, 2*len(s.bits)))
		copy(grown, s.bits)
		s.bits = grown
	}
	s.bits[w] |= 1 << (uint(id) % 64)
}

func (s *visitedBitset) clear() {
	s.n = 0
	clear(s.bits)
	s.overflow.clear()
}
