// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"golang.org/x/sync/semaphore"
)

const maxBatchSizeBytes = 10 * 1024 * 1024 // 10MB

// batchGroup creates a fixed set of pebble.Batches to allow for
// multiple batches to be used concurrently.
type batchGroup struct {
	db           *pebble.DB
	writeOptions *pebble.WriteOptions

	// mu synchronises access to the batch lists.
	mu sync.Mutex
	// because we maintain a fixed number of batches, withBatch
	// must wait for a batch when they are all in use. available
	// tracks how many batches are currently available for use.
	available *semaphore.Weighted
	active    *batchList
	empty     *batchList
	full      *batchList
}

func newBatchGroup(size int64, db *pebble.DB, writeOptions *pebble.WriteOptions) *batchGroup {
	g := &batchGroup{
		db:           db,
		writeOptions: writeOptions,
		available:    semaphore.NewWeighted(size),
	}

	var last *batchList
	for i := int64(0); i < size; i++ {
		g.empty = &batchList{
			next: last,
			b:    db.NewBatch(),
		}
		last = g.empty
	}
	return g
}

// takeActive removes and returns all non-empty batches from the group.
func (g *batchGroup) takeActive() *batchList {
	g.mu.Lock()
	defer g.mu.Unlock()

	var n int64
	var head, tail *batchList
	if g.full != nil {
		n = 1
		head, tail = g.full, g.full
		for tail.next != nil {
			n++
			tail = tail.next
		}
		g.full = nil
	}
	if g.active != nil {
		if head == nil {
			n++
			head, tail = g.active, g.active
		} else {
			tail.next = g.active
		}
		for tail.next != nil {
			n++
			tail = tail.next
		}
		g.active = nil
	}
	g.available.Acquire(context.Background(), n)
	return head
}

// returnEmpty returns empty batches to the group.
func (g *batchGroup) returnEmpty(head *batchList) {
	if head == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	var n int64 = 1
	tail := head
	for tail.next != nil {
		tail = tail.next
		n++
	}
	tail.next = g.empty
	g.empty = head
	g.available.Release(n)
}

func (g *batchGroup) withBatch(ctx context.Context, f func(*pebble.Batch) error) error {
	var commitBatch bool
	var batch *batchList

	if err := g.available.Acquire(ctx, 1); err != nil {
		return err
	}
	defer g.available.Release(1)

	// If there's an active batch (non-empty, but also not full), use that.
	// Otherwise use an empty batch if there is one, and failing that take
	// a full batch and commit it before use. In general we prefer to write
	// fewer batches, and defer committing until harvest time where possible.
	g.mu.Lock()
	if g.active != nil {
		batch = g.active
		g.active = batch.next
		batch.next = nil
	} else if g.empty != nil {
		batch = g.empty
		g.empty = batch.next
		batch.next = nil
	} else if g.full != nil {
		batch = g.full
		g.full = batch.next
		batch.next = nil
		commitBatch = true
	}
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		if batch.b.Len() < maxBatchSizeBytes {
			batch.next = g.active
			g.active = batch
		} else {
			batch.next = g.full
			g.full = batch
		}
		g.mu.Unlock()
	}()
	if commitBatch {
		if err := batch.b.Commit(g.writeOptions); err != nil {
			return fmt.Errorf("failed to commit batch: %w", err)
		}
		batch.b.Reset()
	}
	return f(batch.b)
}

type batchList struct {
	next *batchList
	b    *pebble.Batch
}
