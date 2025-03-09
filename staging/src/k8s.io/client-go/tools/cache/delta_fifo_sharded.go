/*
Copyright 2025 The Kubernetes Authors.

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

package cache

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync/atomic"

	"k8s.io/klog/v2"
)

// ConsistentHashRouter implements consistent hashing for shard routing
type ConsistentHashRouter struct {
	virtualNodes int            // number of virtual nodes per physical node
	hashRing     []uint32       // sorted hash ring
	shardMap     map[uint32]int // maps hash values to shard indices
	shardNum     int            // number of physical shards
}

func NewConsistentHashRouter(shardNum, virtualNodes int) *ConsistentHashRouter {
	r := &ConsistentHashRouter{
		virtualNodes: virtualNodes,
		shardNum:     shardNum,
		shardMap:     make(map[uint32]int),
	}
	r.buildHashRing()
	return r
}

func (r *ConsistentHashRouter) buildHashRing() {
	// Create virtual nodes for each physical shard
	for i := 0; i < r.shardNum; i++ {
		for v := 0; v < r.virtualNodes; v++ {
			hash := r.hashKey(fmt.Sprintf("shard-%d-virtual-%d", i, v))
			r.hashRing = append(r.hashRing, hash)
			r.shardMap[hash] = i
		}
	}
	// Sort hash ring
	sortUint32Slice(r.hashRing)
}

func (r *ConsistentHashRouter) hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (r *ConsistentHashRouter) GetShard(key string) int {
	if len(r.hashRing) == 0 {
		return 0
	}
	hash := r.hashKey(key)
	idx := sort.Search(len(r.hashRing), func(i int) bool {
		return r.hashRing[i] >= hash
	})
	if idx == len(r.hashRing) {
		idx = 0
	}
	return r.shardMap[r.hashRing[idx]]
}

// Helper function to sort uint32 slice
func sortUint32Slice(s []uint32) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

// ShardedDeltaFIFOOptions is the configuration parameters for ShardedDeltaFIFO
type ShardedDeltaFIFOOptions struct {
	// Number of shards to create
	ShardNumber int
	// Number of virtual nodes per shard for consistent hashing
	VirtualNodes int
	// KeyFunction is used to figure out what key an object should have
	KeyFunction KeyFunc
	// KnownObjects is expected to return a list of keys that the consumer of
	// this queue "knows about"
	KnownObjects KeyListerGetter
	// EmitDeltaTypeReplaced indicates that the queue consumer
	// understands the Replaced DeltaType. Before the `Replaced` event type was
	// added, calls to Replace() were handled the same as Sync(). For
	// backwards-compatibility purposes, this is false by default.
	// When true, `Replaced` events will be sent for items passed to a Replace() call.
	// When false, `Sync` events will be sent instead.
	EmitDeltaTypeReplaced bool
	// If set, will be called for objects before enqueueing them. Please
	// see the comment on TransformFunc for details.
	Transformer TransformFunc
}

// ShardedDeltaFIFO is a producer-consumer queue that implements a multiset of
// Delta objects. It divides the workload into multiple shards to improve performance
// in high-scale scenarios.
type ShardedDeltaFIFO struct {
	// shards contains all the DeltaFIFO shards
	shards []*DeltaFIFO
	// router is the consistent hash router
	router *ConsistentHashRouter
	// keyFunc is used to make the key used for queued item insertion and retrieval
	keyFunc KeyFunc
	// closed indicates if the queue is closed
	closed atomic.Bool
}

// NewShardedDeltaFIFO returns a new ShardedDeltaFIFO
func NewShardedDeltaFIFO(opts ShardedDeltaFIFOOptions) (*ShardedDeltaFIFO, error) {
	if opts.ShardNumber <= 0 {
		return nil, fmt.Errorf("shard number must be positive")
	}
	if opts.VirtualNodes <= 0 {
		opts.VirtualNodes = 10 // default to 10 virtual nodes per shard
	}
	if opts.KeyFunction == nil {
		opts.KeyFunction = MetaNamespaceKeyFunc
	}

	router := NewConsistentHashRouter(opts.ShardNumber, opts.VirtualNodes)
	shards := make([]*DeltaFIFO, opts.ShardNumber)
	for i := 0; i < opts.ShardNumber; i++ {
		shards[i] = NewDeltaFIFO(opts.KeyFunction, opts.KnownObjects)
	}

	// Initialize closed flag
	result := &ShardedDeltaFIFO{
		shards:  shards,
		router:  router,
		keyFunc: opts.KeyFunction,
	}
	result.closed.Store(false)

	return result, nil
}

// getShard returns the shard for the given key
func (f *ShardedDeltaFIFO) getShard(key string) *DeltaFIFO {
	shardIndex := f.router.GetShard(key)
	klog.Infof("GetShard: %v for key: %v", shardIndex, key)
	return f.shards[shardIndex]
}

// Add inserts an item into the appropriate shard, and puts it in the queue.
func (f *ShardedDeltaFIFO) Add(obj interface{}) error {
	key, err := f.keyFunc(obj)
	if err != nil {
		return err
	}
	return f.getShard(key).Add(obj)
}

// Update is just like Add, but makes an Updated Delta in the appropriate shard.
func (f *ShardedDeltaFIFO) Update(obj interface{}) error {
	key, err := f.keyFunc(obj)
	if err != nil {
		return err
	}
	return f.getShard(key).Update(obj)
}

// Delete is just like Add, but makes a Deleted Delta in the appropriate shard.
func (f *ShardedDeltaFIFO) Delete(obj interface{}) error {
	key, err := f.keyFunc(obj)
	if err != nil {
		return err
	}
	return f.getShard(key).Delete(obj)
}

// Replace will delete the contents of all shards, using the given list as the new content.
func (f *ShardedDeltaFIFO) Replace(list []interface{}, resourceVersion string) error {
	// Group items by shard
	shardItems := make([][]interface{}, len(f.shards))
	for _, item := range list {
		key, err := f.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		shardIndex := f.router.GetShard(key)
		shardItems[shardIndex] = append(shardItems[shardIndex], item)
	}

	// Replace each shard
	for i, items := range shardItems {
		if err := f.shards[i].Replace(items, resourceVersion); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all shards
func (f *ShardedDeltaFIFO) Close() {
	if !f.closed.CompareAndSwap(false, true) {
		return
	}
	for _, shard := range f.shards {
		shard.Close()
	}
}

// IsClosed checks if the queue is closed
func (f *ShardedDeltaFIFO) IsClosed() bool {
	return f.closed.Load()
}

// HasSynced returns true if all shards have synced
func (f *ShardedDeltaFIFO) HasSynced() bool {
	for _, shard := range f.shards {
		klog.Infof("HasSynced shard: %v", shard.HasSynced())
		if !shard.HasSynced() {
			return false
		}
	}
	return true
}

// Resync calls Resync on all shards
func (f *ShardedDeltaFIFO) Resync() error {
	for _, shard := range f.shards {
		if err := shard.Resync(); err != nil {
			return err
		}
	}
	return nil
}

// GetIndexer returns the indexer
func (f *ShardedDeltaFIFO) GetIndexer() Indexer {
	// DeltaFIFO doesn't support GetIndexer, so we return nil
	return nil
}

// Pop blocks until it has something to process.
func (f *ShardedDeltaFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	if f.IsClosed() {
		return nil, ErrFIFOClosed
	}

	// Create channel for errors
	errChan := make(chan error, len(f.shards))
	done := make(chan struct{})
	defer close(done)

	// Start concurrent consumption of all shards
	for i := range f.shards {
		go func(shard *DeltaFIFO) {
			for {
				select {
				case <-done:
					return
				default:
					_, err := shard.Pop(process)
					if err != nil {
						if err == ErrFIFOClosed {
							select {
							case <-done:
								return
							default:
								errChan <- err
							}
							return
						}
						continue
					}
				}
			}
		}(f.shards[i])
	}

	// Wait for error if any
	err := <-errChan
	if err == ErrFIFOClosed {
		f.Close()
	}
	return nil, err
}

// AddIfNotPresent is not supported by DeltaFIFO
func (f *ShardedDeltaFIFO) AddIfNotPresent(obj interface{}) error {
	// Since DeltaFIFO doesn't support AddIfNotPresent, we use Add instead
	return f.Add(obj)
}

// List returns a list of all items in all shards.
func (f *ShardedDeltaFIFO) List() []interface{} {
	var result []interface{}
	for _, shard := range f.shards {
		for _, deltas := range shard.items {
			if newest := deltas.Newest(); newest != nil {
				result = append(result, newest.Object)
			}
		}
	}
	return result
}

// ListKeys returns a list of all keys in all shards.
func (f *ShardedDeltaFIFO) ListKeys() []string {
	var result []string
	for _, shard := range f.shards {
		for key := range shard.items {
			result = append(result, key)
		}
	}
	return result
}

// Get returns the requested item from the appropriate shard.
func (f *ShardedDeltaFIFO) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := f.keyFunc(obj)
	if err != nil {
		return nil, false, err
	}
	// Get the item from the appropriate shard
	shard := f.getShard(key)
	deltas, exists := shard.items[key]
	if !exists {
		return nil, false, nil
	}
	return copyDeltas(deltas), true, nil
}

// GetByKey returns the requested item by key from the appropriate shard.
func (f *ShardedDeltaFIFO) GetByKey(key string) (item interface{}, exists bool, err error) {
	// Get the item from the appropriate shard
	shard := f.getShard(key)
	deltas, exists := shard.items[key]
	if !exists {
		return nil, false, nil
	}
	return copyDeltas(deltas), true, nil
}

// Len returns the sum of lengths of all shards.
func (f *ShardedDeltaFIFO) Len() int {
	length := 0
	for _, shard := range f.shards {
		// Get the length of each shard's queue
		length += len(shard.items)
	}
	return length
}
