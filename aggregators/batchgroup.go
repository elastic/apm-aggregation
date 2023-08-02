// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

const dbCommitThresholdBytes = 10 << 20

// batchGroup creates a cache of pebble.Batch to allow for multiple batches to be
// used concurrently. The batchGroup scales up based on demand.
//
// TODO @lahsivjar: Allow scaledown of the group.
type batchGroup struct {
	mu           sync.Mutex
	db           *pebble.DB
	cache        *pq
	maxSize      int
	writeOptions *pebble.WriteOptions
}

func newBatchGroup(maxSize int, db *pebble.DB, writeOptions *pebble.WriteOptions) *batchGroup {
	return &batchGroup{
		db:           db,
		cache:        newPQ(maxSize),
		maxSize:      maxSize,
		writeOptions: writeOptions,
	}
}

// commitAll commits all the currently available batch groups. The caller must ensure
// that all batches are returned when commitAll is called and no new batches are
// requested before this call finishes.
func (bg *batchGroup) commitAll() error {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	var errs []error
	bg.cache.ForEachWithPop(func(b *pebble.Batch) {
		if err := b.Commit(bg.writeOptions); err != nil {
			errs = append(errs, fmt.Errorf("failed to commit pebble batch: %w", err))
		}
		if err := b.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close pebble batch: %w", err))
		}
	})
	if len(errs) > 0 {
		return fmt.Errorf("failed to commit batch group: %w", errors.Join(errs...))
	}
	return nil
}

func (bg *batchGroup) getBatch() *pebble.Batch {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	if bg.cache.Len() > 0 && bg.cache.Peek().Len() < dbCommitThresholdBytes {
		return bg.cache.Pop()
	}

	// TODO: Limit the max number of batch that can be created.
	return bg.db.NewBatch()
}

func (bg *batchGroup) releaseBatch(b *pebble.Batch) error {
	if b == nil {
		return nil
	}

	bg.mu.Lock()
	defer bg.mu.Unlock()

	bg.cache.Push(b)
	return nil
}

type pq struct {
	q queue
}

type queue []*pebble.Batch

func (q queue) Len() int {
	return len(q)
}

func (q queue) Less(i, j int) bool {
	return q[i].Len() < q[j].Len()
}

func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *queue) Push(x interface{}) {
	*q = append(*q, x.(*pebble.Batch))
}

func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

func newPQ(maxSize int) *pq {
	return &pq{
		q: make([]*pebble.Batch, 0, maxSize),
	}
}

func (pq *pq) Push(b *pebble.Batch) {
	heap.Push(&pq.q, b)
}

func (pq *pq) Peek() *pebble.Batch {
	return pq.q[0]
}

func (pq *pq) Pop() *pebble.Batch {
	raw := heap.Pop(&pq.q)
	return raw.(*pebble.Batch)
}

func (pq *pq) ForEachWithPop(f func(*pebble.Batch)) {
	for pq.Len() > 0 {
		f(pq.Pop())
	}
}

func (pq *pq) Len() int {
	return pq.q.Len()
}
