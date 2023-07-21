// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

const dbCommitThresholdBytes = 10 * 1024 * 1024 // commit every 10MB

// batchGroup creates a cache of pebble.Batch to allow for multiple batches to be
// used concurrently. The batchGroup scales up based on demand.
//
// TODO @lahsivjar: Allow scaledown of the group.
type batchGroup struct {
	db        *pebble.DB
	cache     chan *pebble.Batch
	maxSize   int
	created   atomic.Int32
	creatorMu sync.Mutex // allow only 1 new batch to be created at a time
}

func newBatchGroup(maxSize int, db *pebble.DB) *batchGroup {
	return &batchGroup{
		db:      db,
		cache:   make(chan *pebble.Batch, maxSize),
		maxSize: maxSize,
	}
}

// commitAll commits all the currently available batch groups. The caller must ensure
// that all batches are returned when commitAll is called and no new batches are
// requested before this call finishes.
func (bg *batchGroup) commitAll() error {
	var errs []error
	for {
		select {
		case b := <-bg.cache:
			if err := b.Commit(pebble.Sync); err != nil {
				errs = append(errs, fmt.Errorf("failed to commit pebble batch: %w", err))
			}
			if err := b.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close pebble batch: %w", err))
			}
			// We will not create a new replacement batch
			bg.created.Add(-1)
		default:
			remaining := bg.created.Load()
			if remaining > 0 {
				errs = append(errs, fmt.Errorf("%d batches were not returned to the group before committing", remaining))
			}
			if len(errs) > 0 {
				return fmt.Errorf("failed to commit batch group: %w", errors.Join(errs...))
			}
			return nil
		}
	}
}

func (bg *batchGroup) getBatch() (*pebble.Batch, error) {
	select {
	case b := <-bg.cache:
		return b, nil
	default:
	}

	bg.creatorMu.Lock()
	defer bg.creatorMu.Unlock()
	// attempt get with lock
	select {
	case b := <-bg.cache:
		return b, nil
	default:
	}
	// if no more batches can be created then wait for a batch to be released.
	if bg.created.Load() == int32(bg.maxSize) {
		return <-bg.cache, nil
	}
	b := bg.db.NewBatch()
	bg.created.Add(1)
	return b, nil
}

func (bg *batchGroup) releaseBatch(b *pebble.Batch) error {
	if b == nil {
		return nil
	}
	// TODO @lahsivjar: Handle batch released for previous processing time
	// maybe wrap pebble.Batch into a custom struct with processing time
	if b.Len() >= dbCommitThresholdBytes {
		if err := b.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("failed to commit pebble batch: %w", err)
		}
		if err := b.Close(); err != nil {
			return fmt.Errorf("failed to close pebble batch: %w", err)
		}
		b = bg.db.NewBatch()
	}
	// This should never block because we will always expect a given batch
	// to come back and the group will hold its position.
	bg.cache <- b
	return nil
}
