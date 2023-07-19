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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/elastic/apm-aggregation/aggregators/internal/telemetry"
	"github.com/elastic/apm-data/model/modelpb"
)

const (
	dbCommitThresholdBytes = 10 * 1024 * 1024 // commit every 10MB
	aggregationIvlKey      = "aggregation_interval"
)

var (
	// ErrAggregatorStopped means that aggregator was stopped when the
	// method was called and thus cannot be processed further.
	ErrAggregatorStopped = errors.New("aggregator is stopping or stopped")
	// ErrAggregatorAlreadyRunning means that aggregator Run method is
	// called while the aggregator is already running.
	ErrAggregatorAlreadyRunning = errors.New("aggregator is already running")
)

// Aggregator represents a LSM based aggregator instance to generate
// aggregated metrics. The metrics aggregated by the aggregator are
// harvested based on the aggregation interval and processed by the
// defined processor. The aggregated metrics are timestamped based
// on when the aggregator is created and the harvest loop. All the
// events collected between call to New and Run are collected in the
// same processing time bucket and thereafter the processing time
// bucket is advanced in factors of aggregation interval.
type Aggregator struct {
	db  *pebble.DB
	cfg Config

	mu             sync.Mutex
	processingTime time.Time
	batch          *pebble.Batch
	cachedStats    map[time.Duration]map[[16]byte]stats

	stopping   chan struct{}
	runStarted atomic.Bool
	runStopped chan struct{}

	metrics *telemetry.Metrics
}

// stats is used to cache request based stats accepted by the
// pipeline and publish them once harvest is performed. This
// fixes the time shift introduced by the aggregation interval.
// Mostly useful for metrics which needs to be correlated with
// harvest time metrics.
type stats struct {
	eventsTotal float64
}

func (s *stats) merge(from stats) {
	s.eventsTotal += from.eventsTotal
}

// New returns a new aggregator instance.
func New(opts ...Option) (*Aggregator, error) {
	cfg, err := NewConfig(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregation config: %w", err)
	}

	pb, err := pebble.Open(cfg.DataDir, &pebble.Options{
		Merger: &pebble.Merger{
			Name: "combined_metrics_merger",
			Merge: func(_, value []byte) (pebble.ValueMerger, error) {
				merger := combinedMetricsMerger{
					limits: cfg.Limits,
				}
				if err := merger.metrics.UnmarshalBinary(value); err != nil {
					return nil, err
				}
				return &merger, nil
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble db: %w", err)
	}

	metrics, err := telemetry.NewMetrics(
		func() *pebble.Metrics { return pb.Metrics() },
		telemetry.WithMeter(cfg.Meter),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics: %w", err)
	}

	return &Aggregator{
		db:             pb,
		cfg:            cfg,
		processingTime: time.Now().Truncate(cfg.AggregationIntervals[0]),
		cachedStats:    newCachedStats(cfg.AggregationIntervals),
		stopping:       make(chan struct{}),
		runStopped:     make(chan struct{}),
		metrics:        metrics,
	}, nil
}

func newCachedStats(ivls []time.Duration) map[time.Duration]map[[16]byte]stats {
	m := make(map[time.Duration]map[[16]byte]stats, len(ivls))
	for _, ivl := range ivls {
		m[ivl] = make(map[[16]byte]stats)
	}
	return m
}

// AggregateBatch aggregates all events in the batch. This function will return
// an error if the aggregator's Run loop has errored or has been explicitly stopped.
// However, it doesn't require aggregator to be running to perform aggregation.
func (a *Aggregator) AggregateBatch(
	ctx context.Context,
	id string,
	b *modelpb.Batch,
) error {
	cmIDAttrs := a.cfg.CombinedMetricsIDToKVs(id)

	a.mu.Lock()
	defer a.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.stopping:
		return ErrAggregatorStopped
	default:
	}

	cmkID, err := EncodeToCombinedMetricsKeyID(id)
	if err != nil {
		return fmt.Errorf("invalid ID received: %w", err)
	}

	var errs []error
	var totalBytesIn int64
	cmk := CombinedMetricsKey{ID: cmkID}
	for _, ivl := range a.cfg.AggregationIntervals {
		cmk.ProcessingTime = a.processingTime.Truncate(ivl)
		cmk.Interval = ivl
		for _, e := range *b {
			bytesIn, err := a.aggregateAPMEvent(ctx, cmk, e)
			if err != nil {
				errs = append(errs, err)
			}
			totalBytesIn += int64(bytesIn)
		}
		cmStats := a.cachedStats[ivl][cmkID]
		cmStats.eventsTotal += float64(len(*b))
		a.cachedStats[ivl][cmkID] = cmStats
	}

	cmIDAttrSet := attribute.NewSet(cmIDAttrs...)
	a.metrics.RequestsTotal.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
	a.metrics.BytesIngested.Add(ctx, totalBytesIn, metric.WithAttributeSet(cmIDAttrSet))
	if len(errs) > 0 {
		a.metrics.RequestsFailed.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
		return fmt.Errorf("failed batch aggregation:\n%w", errors.Join(errs...))
	}
	return nil
}

// AggregateCombinedMetrics aggregates partial metrics into a bigger aggregate.
// This function will return an error if the aggregator's Run loop has errored
// or has been explicitly stopped. However, it doesn't require aggregator to be
// running to perform aggregation.
func (a *Aggregator) AggregateCombinedMetrics(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm CombinedMetrics,
) error {
	cmkID := DecodeFromCombinedMetricsKeyID(cmk.ID)
	cmIDAttrs := a.cfg.CombinedMetricsIDToKVs(cmkID)
	traceAttrs := append(append([]attribute.KeyValue{}, cmIDAttrs...),
		attribute.String(aggregationIvlKey, formatDuration(cmk.Interval)),
		attribute.String("processing_time", cmk.ProcessingTime.String()))
	ctx, span := a.cfg.Tracer.Start(ctx, "AggregateCombinedMetrics", trace.WithAttributes(traceAttrs...))
	defer span.End()

	a.mu.Lock()
	defer a.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.stopping:
		return ErrAggregatorStopped
	default:
	}

	bytesIn, err := a.aggregate(ctx, cmk, cm)

	if _, ok := a.cachedStats[cmk.Interval]; !ok {
		// Protection for stats collected from a different instance
		// of aggregator as aggregators can be chained.
		a.cachedStats[cmk.Interval] = make(map[[16]byte]stats)
	}
	cmStats := a.cachedStats[cmk.Interval][cmk.ID]
	cmStats.eventsTotal += cm.eventsTotal
	a.cachedStats[cmk.Interval][cmk.ID] = cmStats

	span.SetAttributes(attribute.Int("bytes_ingested", bytesIn))
	cmIDAttrSet := attribute.NewSet(cmIDAttrs...)
	a.metrics.RequestsTotal.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
	a.metrics.BytesIngested.Add(ctx, int64(bytesIn), metric.WithAttributeSet(cmIDAttrSet))
	if err != nil {
		a.metrics.RequestsFailed.Add(ctx, 1, metric.WithAttributeSet(cmIDAttrSet))
	}
	return err
}

// Run harvests the aggregated results periodically. For an aggregator,
// Run must be called at-most once.
// - Running more than once will return ErrAggregatorAlreadyRunning.
// - Running after aggregator is stopped will return ErrAggregatorStopped.
func (a *Aggregator) Run(ctx context.Context) error {
	if !a.runStarted.CompareAndSwap(false, true) {
		return ErrAggregatorAlreadyRunning
	}
	defer close(a.runStopped)

	to := a.processingTime.Add(a.cfg.AggregationIntervals[0])
	timer := time.NewTimer(time.Until(to.Add(a.cfg.HarvestDelay)))
	harvestStats := newCachedStats(a.cfg.AggregationIntervals)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.stopping:
			return ErrAggregatorStopped
		case <-timer.C:
		}

		a.mu.Lock()
		batch := a.batch
		a.batch = nil
		a.processingTime = to
		for ivl, statsm := range a.cachedStats {
			if _, ok := harvestStats[ivl]; !ok {
				// Protection for stats collected from a different instance
				// of aggregator as aggregators can be chained.
				harvestStats[ivl] = make(map[[16]byte]stats)
			}
			for cmID, cmStats := range statsm {
				hstats := harvestStats[ivl][cmID]
				hstats.merge(cmStats)
				harvestStats[ivl][cmID] = hstats
				delete(statsm, cmID)
			}
		}
		a.mu.Unlock()

		if err := a.commitAndHarvest(ctx, batch, to, harvestStats); err != nil {
			a.cfg.Logger.Warn("failed to commit and harvest metrics", zap.Error(err))
		}
		to = to.Add(a.cfg.AggregationIntervals[0])
		timer.Reset(time.Until(to.Add(a.cfg.HarvestDelay)))
	}
}

// Stop stops the aggregator. Aggregations performed after calling Stop
// will return an error. Stop can be called multiple times but concurrent
// calls to stop will block.
func (a *Aggregator) Stop(ctx context.Context) error {
	ctx, span := a.cfg.Tracer.Start(ctx, "Aggregator.Stop")
	defer span.End()

	a.mu.Lock()
	select {
	case <-a.stopping:
	default:
		close(a.stopping)
	}
	a.mu.Unlock()
	if a.runStarted.Load() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for run to complete: %w", ctx.Err())
		case <-a.runStopped:
		}
	}

	a.cfg.Logger.Info("stopping aggregator")
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.db != nil {
		a.cfg.Logger.Info("running final aggregation")
		if a.batch != nil {
			if err := a.batch.Commit(pebble.Sync); err != nil {
				span.RecordError(err)
				return fmt.Errorf("failed to commit batch: %w", err)
			}
			if err := a.batch.Close(); err != nil {
				span.RecordError(err)
				return fmt.Errorf("failed to close batch: %w", err)
			}
			a.batch = nil
		}
		var errs []error
		for _, ivl := range a.cfg.AggregationIntervals {
			// At any particular time there will be 1 harvest candidate for
			// each aggregation interval. We will align the end time and
			// process each of these.
			//
			// TODO (lahsivjar): It is possible to harvest the same
			// time multiple times, not an issue but can be optimized.
			to := a.processingTime.Truncate(ivl).Add(ivl)
			if err := a.harvest(ctx, to, a.cachedStats); err != nil {
				span.RecordError(err)
				errs = append(errs, fmt.Errorf(
					"failed to harvest metrics for interval %s: %w", formatDuration(ivl), err),
				)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("failed while running final harvest: %w", errors.Join(errs...))
		}
		if err := a.db.Close(); err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to close pebble: %w", err)
		}
		// All future operations are invalid after db is closed
		a.db = nil
	}
	if err := a.metrics.CleanUp(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to cleanup instrumentation: %w", err)
	}
	return nil
}

func (a *Aggregator) aggregateAPMEvent(
	ctx context.Context,
	cmk CombinedMetricsKey,
	e *modelpb.APMEvent,
) (int, error) {
	kvs, err := EventToCombinedMetrics(e, cmk, a.cfg.Partitioner)
	if err != nil {
		return 0, fmt.Errorf("failed to convert event to combined metrics: %w", err)
	}
	var totalBytesIn int
	for cmk, cm := range kvs {
		bytesIn, err := a.aggregate(ctx, cmk, *cm)
		totalBytesIn += bytesIn
		if err != nil {
			return totalBytesIn, fmt.Errorf("failed to aggregate combined metrics: %w", err)
		}
	}
	return totalBytesIn, nil
}

// aggregate aggregates combined metrics for a given key and returns
// number of bytes ingested along with the error, if any.
func (a *Aggregator) aggregate(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm CombinedMetrics,
) (int, error) {
	cmproto := cm.ToProto()
	defer cmproto.ReturnToVTPool()

	if a.batch == nil {
		// Batch is backed by a sync pool. After each commit we will release the batch
		// back to the pool by calling Batch#Close and subsequently acquire a new batch.
		a.batch = a.db.NewBatch()
	}

	op := a.batch.MergeDeferred(cmk.SizeBinary(), cmproto.SizeVT())
	if err := cmk.MarshalBinaryToSizedBuffer(op.Key); err != nil {
		return 0, fmt.Errorf("failed to marshal combined metrics key: %w", err)
	}
	if _, err := cmproto.MarshalToSizedBufferVT(op.Value); err != nil {
		return 0, fmt.Errorf("failed to marshal combined metrics: %w", err)
	}
	if err := op.Finish(); err != nil {
		return 0, fmt.Errorf("failed to finalize merge operation: %w", err)
	}

	bytesIn := cmproto.SizeVT()
	if a.batch.Len() >= dbCommitThresholdBytes {
		if err := a.batch.Commit(pebble.Sync); err != nil {
			return bytesIn, fmt.Errorf("failed to commit pebble batch: %w", err)
		}
		if err := a.batch.Close(); err != nil {
			return bytesIn, fmt.Errorf("failed to close pebble batch: %w", err)
		}
		a.batch = nil
	}
	return bytesIn, nil
}

func (a *Aggregator) commitAndHarvest(
	ctx context.Context,
	batch *pebble.Batch,
	to time.Time,
	harvestStats map[time.Duration]map[[16]byte]stats,
) error {
	ctx, span := a.cfg.Tracer.Start(ctx, "commitAndHarvest")
	defer span.End()

	var errs []error
	if batch != nil {
		if err := batch.Commit(pebble.Sync); err != nil {
			span.RecordError(err)
			errs = append(errs, fmt.Errorf("failed to commit batch before harvest: %w", err))
		}
		if err := batch.Close(); err != nil {
			span.RecordError(err)
			errs = append(errs, fmt.Errorf("failed to close batch before harvest: %w", err))
		}
	}
	if err := a.harvest(ctx, to, harvestStats); err != nil {
		span.RecordError(err)
		errs = append(errs, fmt.Errorf("failed to harvest aggregated metrics: %w", err))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// harvest collects the mature metrics for all aggregation intervals and
// deletes the entries in db once the metrics are fully harvested. Harvest
// takes an end time denoting the exclusive upper bound for harvesting.
func (a *Aggregator) harvest(
	ctx context.Context,
	end time.Time,
	harvestStats map[time.Duration]map[[16]byte]stats,
) error {
	snap := a.db.NewSnapshot()
	defer snap.Close()

	var errs []error
	for _, ivl := range a.cfg.AggregationIntervals {
		// Check if the given aggregation interval needs to be harvested now
		if end.Truncate(ivl).Equal(end) {
			start := end.Add(-ivl)
			cmCount, err := a.harvestForInterval(
				ctx, snap, start, end, ivl, harvestStats[ivl],
			)
			if err != nil {
				errs = append(errs, fmt.Errorf(
					"failed to harvest aggregated metrics for interval %s: %w",
					ivl, err,
				))
			}
			a.cfg.Logger.Debug(
				"Finished harvesting aggregated metrics",
				zap.Int("combined_metrics_successfully_harvested", cmCount),
				zap.Duration("aggregation_interval_ns", ivl),
				zap.Time("harvested_till(exclusive)", end),
				zap.Error(err),
			)
		}
	}
	return errors.Join(errs...)
}

// harvestForInterval harvests aggregated metrics for a given interval.
// Returns the number of combined metrics successfully harvested and an
// error. It is possible to have non nil error and greater than 0
// combined metrics if some of the combined metrics failed harvest.
func (a *Aggregator) harvestForInterval(
	ctx context.Context,
	snap *pebble.Snapshot,
	start, end time.Time,
	ivl time.Duration,
	cmStats map[[16]byte]stats,
) (int, error) {
	from := CombinedMetricsKey{
		Interval:       ivl,
		ProcessingTime: start,
	}
	to := CombinedMetricsKey{
		Interval:       ivl,
		ProcessingTime: end,
	}
	lb := make([]byte, from.SizeBinary())
	ub := make([]byte, to.SizeBinary())
	from.MarshalBinaryToSizedBuffer(lb)
	to.MarshalBinaryToSizedBuffer(ub)

	// caching and publishing events total metrics at this point helps reduce
	// the time gap between total and processed metrics to a max of the lowest
	// aggregation interval. This gap can be introduced if L1 aggregators are
	// stopped when the L2 aggregator is waiting for harvest delay leading to
	// premature harvest as part of the graceful shutdown process.
	ivlAttr := attribute.String(aggregationIvlKey, formatDuration(ivl))
	for cmID, stats := range cmStats {
		cmIDStr := DecodeFromCombinedMetricsKeyID(cmID)
		attrs := append(a.cfg.CombinedMetricsIDToKVs(cmIDStr), ivlAttr)
		a.metrics.EventsTotal.Add(
			ctx, stats.eventsTotal,
			metric.WithAttributeSet(
				attribute.NewSet(attrs...),
			),
		)
		delete(cmStats, cmID)
	}

	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: lb,
		UpperBound: ub,
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	defer iter.Close()

	var errs []error
	var cmCount int
	for iter.First(); iter.Valid(); iter.Next() {
		var cmk CombinedMetricsKey
		if err := cmk.UnmarshalBinary(iter.Key()); err != nil {
			errs = append(errs, fmt.Errorf("failed to unmarshal key: %w", err))
			continue
		}
		harvestStats, err := a.processHarvest(ctx, cmk, iter.Value(), ivl)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cmCount++

		cmIDStr := DecodeFromCombinedMetricsKeyID(cmk.ID)
		attrs := append(a.cfg.CombinedMetricsIDToKVs(cmIDStr), ivlAttr)
		attrSet := metric.WithAttributeSet(attribute.NewSet(attrs...))
		// processingDelay is normalized by subtracting aggregation interval and
		// harvest delay, both of which are expected delays. Normalization helps
		// us to use the lower (higher resolution) range of the histogram for the
		// important values. The normalized processingDelay can be negative as a
		// result of premature harvest triggered by a stop of the aggregator. The
		// negative value is accepted as a good value and recorded in the lower
		// histogram buckets.
		processingDelay := time.Since(cmk.ProcessingTime).Seconds() -
			(ivl.Seconds() + a.cfg.HarvestDelay.Seconds())
		// queuedDelay is not explicitly normalized because we want to record the
		// full delay. For a healthy deployment, the queued delay would be
		// implicitly normalized due to the usage of youngest event timestamp.
		// Negative values are possible at edges due to delays in running the
		// harvest loop or time sync issues between agents and server.
		queuedDelay := time.Since(harvestStats.youngestEventTimestamp).Seconds()
		a.metrics.MinQueuedDelay.Record(ctx, queuedDelay, attrSet)
		a.metrics.ProcessingDelay.Record(ctx, processingDelay, attrSet)
		a.metrics.EventsProcessed.Add(ctx, harvestStats.eventsTotal, attrSet)
	}
	err := a.db.DeleteRange(lb, ub, pebble.Sync)
	if len(errs) > 0 {
		err = errors.Join(err, fmt.Errorf(
			"failed to process %d out of %d metrics:\n%w",
			len(errs), cmCount, errors.Join(errs...),
		))
	}
	return cmCount, err
}

type harvestStats struct {
	eventsTotal            float64
	youngestEventTimestamp time.Time
}

func (a *Aggregator) processHarvest(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cmb []byte,
	aggIvl time.Duration,
) (harvestStats, error) {
	var (
		cm CombinedMetrics
		hs harvestStats
	)
	if err := cm.UnmarshalBinary(cmb); err != nil {
		return hs, fmt.Errorf("failed to unmarshal metrics: %w", err)
	}
	if err := a.cfg.Processor(ctx, cmk, cm, aggIvl); err != nil {
		return hs, fmt.Errorf("failed to process combined metrics ID %s: %w", cmk.ID, err)
	}
	hs.eventsTotal = cm.eventsTotal
	hs.youngestEventTimestamp = cm.youngestEventTimestamp
	return hs, nil
}
