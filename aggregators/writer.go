// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package aggregators holds the logic for doing the actual aggregation.
package aggregators

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-data/model/modelpb"
)

// ErrWriterClosed is returned by Writer methods after the writer is closed.
var ErrWriterClosed = errors.New("writer is closed")

// Writer provides methods for writing metrics to Pebble.
type Writer struct {
	aggregator *Aggregator

	mu    sync.Mutex
	batch *pebble.Batch
}

// Close commits and closes the writer's batch,
// and removes the writer from the aggregator.
//
// Writers must not be used again after Close is called.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.batch == nil {
		return nil
	}
	batch := w.batch
	w.batch = nil
	w.aggregator.writers.Delete(w)
	return errors.Join(
		batch.Commit(w.aggregator.writeOptions),
		batch.Close(),
	)
}

// Commit replaces the writer's batch, and commits any buffered writes.
func (w *Writer) Commit() error {
	batch, ok := w.takeBatch()
	if !ok {
		return nil
	}
	return errors.Join(batch.Commit(w.aggregator.writeOptions), batch.Close())
}

// takeBatch locks the writer, and if its batch is non-empty, replaces
// the batch with a new one and returns the old one. If the batch is
// empty, a nil batch is returned.
func (w *Writer) takeBatch() (*pebble.Batch, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	batch := w.batch
	if batch == nil || batch.Empty() {
		return nil, false
	}
	w.batch = w.aggregator.db.NewBatch()
	return batch, true
}

// WriteEventMetrics writes metrics for one or more APM events.
//
// This function will return an error if the writer has been closed.
func (w *Writer) WriteEventMetrics(ctx context.Context, id [16]byte, events ...*modelpb.APMEvent) error {
	w.aggregator.mu.RLock()
	defer w.aggregator.mu.RUnlock()

	var totalBytesIn int64
	aggregateFunc := func(k CombinedMetricsKey, m *aggregationpb.CombinedMetrics) error {
		bytesIn, err := w.writeCombinedMetrics(ctx, k, m)
		totalBytesIn += int64(bytesIn)
		if err != nil {
			return fmt.Errorf("failed to write event metrics: %w", err)
		}
		return nil
	}

	cmk := CombinedMetricsKey{ID: id}
	var errs []error
	for _, ivl := range w.aggregator.cfg.AggregationIntervals {
		cmk.ProcessingTime = w.aggregator.processingTime.Truncate(ivl)
		cmk.Interval = ivl
		for _, event := range events {
			err := EventToCombinedMetrics(event, cmk, w.aggregator.cfg.Partitioner, aggregateFunc)
			if err != nil {
				errs = append(errs, err)
			}
		}
		w.aggregator.cachedEvents.add(ivl, id, float64(len(events)))
	}

	cmIDAttrs := w.aggregator.cfg.CombinedMetricsIDToKVs(id)
	cmIDAttrSet := attribute.NewSet(cmIDAttrs...)
	w.aggregator.metrics.RequestsTotal.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
	w.aggregator.metrics.BytesIngested.Add(ctx, totalBytesIn, metric.WithAttributeSet(cmIDAttrSet))
	if len(errs) > 0 {
		w.aggregator.metrics.RequestsFailed.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
		return fmt.Errorf("failed batch aggregation:\n%w", errors.Join(errs...))
	}
	return nil
}

// WriteCombinedMetrics writes combined metrics.
//
// This function will return an error if the writer has been closed.
func (w *Writer) WriteCombinedMetrics(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm *aggregationpb.CombinedMetrics,
) error {
	w.aggregator.mu.RLock()
	defer w.aggregator.mu.RUnlock()

	bytesIn, err := w.writeCombinedMetrics(ctx, cmk, cm)
	w.aggregator.cachedEvents.add(cmk.Interval, cmk.ID, cm.EventsTotal)

	cmIDAttrs := w.aggregator.cfg.CombinedMetricsIDToKVs(cmk.ID)
	cmIDAttrSet := attribute.NewSet(cmIDAttrs...)
	w.aggregator.metrics.RequestsTotal.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
	w.aggregator.metrics.BytesIngested.Add(ctx, int64(bytesIn), metric.WithAttributeSet(cmIDAttrSet))
	if err != nil {
		w.aggregator.metrics.RequestsFailed.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
	}
	return err
}

// writeCombinedMetrics writes combined metrics for a given key and returns
// number of bytes ingested along with the error, if any.
func (w *Writer) writeCombinedMetrics(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm *aggregationpb.CombinedMetrics,
) (_ int, resultErr error) {

	// We conditionally commit and close the batch if it is large enough after writing.
	// We do this after releasing the lock to avoid holding up other writers or readers.
	var commitBatch *pebble.Batch
	defer func() {
		if commitBatch == nil {
			return
		}
		resultErr = errors.Join(
			resultErr,
			commitBatch.Commit(w.aggregator.writeOptions),
			commitBatch.Close(),
		)
	}()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.batch == nil {
		return 0, ErrWriterClosed
	}

	valueSize := cm.SizeVT()
	op := w.batch.MergeDeferred(cmk.SizeBinary(), valueSize)
	if err := cmk.MarshalBinaryToSizedBuffer(op.Key); err != nil {
		return 0, fmt.Errorf("failed to marshal combined metrics key: %w", err)
	}
	if _, err := cm.MarshalToSizedBufferVT(op.Value); err != nil {
		return 0, fmt.Errorf("failed to marshal combined metrics: %w", err)
	}
	if err := op.Finish(); err != nil {
		return 0, fmt.Errorf("failed to finalize merge operation: %w", err)
	}
	if w.batch.Len() >= dbCommitThresholdBytes {
		commitBatch = w.batch
		w.batch = w.aggregator.db.NewBatch()
	}
	return valueSize, nil
}
