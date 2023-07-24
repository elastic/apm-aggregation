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
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/elastic/apm-aggregation/aggregators/internal/telemetry"
)

const (
	dbCommitThresholdBytes = 10 * 1024 * 1024 // commit every 10MB
	aggregationIvlKey      = "aggregation_interval"
)

var (
	// ErrAggregatorClosed means that aggregator was closed when the
	// method was called and thus cannot be processed further.
	ErrAggregatorClosed = errors.New("aggregator is closed")
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
	db           *pebble.DB
	writeOptions *pebble.WriteOptions
	cfg          Config

	mu             sync.RWMutex
	writers        sync.Map
	processingTime time.Time
	cachedEvents   cachedEventsMap

	closed            chan struct{}
	harvestingStopped chan struct{}

	metrics *telemetry.Metrics
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
		writeOptions:   pebble.Sync,
		cfg:            cfg,
		processingTime: time.Now().Truncate(cfg.AggregationIntervals[0]),
		closed:         make(chan struct{}),
		metrics:        metrics,
	}, nil
}

// Close commits and closes any open Writers, performs a final harvest,
// and closes the underlying database.
//
// No further writes may be performed after Close is called,
// and no further harvests will be performed once Close returns.
func (a *Aggregator) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	select {
	case <-a.closed:
	default:
		close(a.closed)
	}

	var errs []error
	a.writers.Range(func(_, value any) bool {
		writer := value.(*Writer)
		if err := writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close writer: %w", err))
		}
		return true
	})
	if a.harvestingStopped != nil {
		select {
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("context cancelled while waiting for harvesting to stop: %w", ctx.Err()))
		case <-a.harvestingStopped:
		}
	}
	if a.db != nil {
		if err := a.finalHarvest(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to perform final harvest: %w", err))
		}
		if err := a.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close pebble: %w", err))
		}
		a.db = nil
	}
	if err := a.metrics.CleanUp(); err != nil {
		errs = append(errs, fmt.Errorf("failed to cleanup instrumentation: %w", err))
	}
	return errors.Join(errs...)
}

// NewWriter returns a new Writer, for adding metric values to be aggregated.
//
// The returned Writer will be associated with the aggregator, and will be
// automatically closed (with any buffered writes committed) when the aggregator
// is closed.
//
// NewWriter will return an error if the aggregator has already been stopped.
func (a *Aggregator) NewWriter() (*Writer, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	select {
	case <-a.closed:
		return nil, ErrAggregatorClosed
	default:
	}
	w := &Writer{aggregator: a, batch: a.db.NewBatch()}
	a.writers.Store(w, w)
	return w, nil
}

// StartHarvesting starts periodically harvesting aggregated metrics.
//
// StartHarvester may be called at most once, and will return an error if it
// is called a second time, or if the aggregator has already been closed.
func (a *Aggregator) StartHarvesting() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	select {
	case <-a.closed:
		return ErrAggregatorClosed
	default:
	}
	if a.harvestingStopped != nil {
		return errors.New("harvesting already started")
	}
	a.harvestingStopped = make(chan struct{})
	go a.harvestLoop()
	return nil
}

func (a *Aggregator) harvestLoop() {
	defer close(a.harvestingStopped)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	to := a.processingTime.Add(a.cfg.AggregationIntervals[0])
	timer := time.NewTimer(time.Until(to.Add(a.cfg.HarvestDelay)))
	defer timer.Stop()
	for {
		select {
		case <-a.closed:
			return
		case <-timer.C:
		}

		var commitBatches []*pebble.Batch
		a.mu.Lock()
		a.writers.Range(func(_, value any) bool {
			writer := value.(*Writer)
			if batch, ok := writer.takeBatch(); ok {
				commitBatches = append(commitBatches, batch)
			}
			return true
		})
		a.processingTime = to
		cachedEventsStats := a.cachedEvents.loadAndDelete(to)
		a.mu.Unlock()

		if err := a.commitAndHarvest(ctx, commitBatches, to, cachedEventsStats); err != nil {
			a.cfg.Logger.Warn("failed to commit and harvest metrics", zap.Error(err))
		}
		to = to.Add(a.cfg.AggregationIntervals[0])
		timer.Reset(time.Until(to.Add(a.cfg.HarvestDelay)))
	}
}

// finalHarvest is called by Aggregator.Close, even if StartHarvesting is never called.
func (a *Aggregator) finalHarvest(ctx context.Context) error {
	var errs []error
	for _, ivl := range a.cfg.AggregationIntervals {
		// At any particular time there will be 1 harvest candidate for
		// each aggregation interval. We will align the end time and
		// process each of these.
		//
		// TODO (lahsivjar): It is possible to harvest the same
		// time multiple times, not an issue but can be optimized.
		to := a.processingTime.Truncate(ivl).Add(ivl)
		if err := a.harvest(ctx, to, a.cachedEvents.loadAndDelete(to)); err != nil {
			errs = append(errs, fmt.Errorf(
				"failed to harvest metrics for interval %s: %w", formatDuration(ivl), err),
			)
		}
	}
	return errors.Join(errs...)
}

func (a *Aggregator) commitAndHarvest(
	ctx context.Context,
	batches []*pebble.Batch,
	to time.Time,
	cachedEventsStats map[time.Duration]map[[16]byte]float64,
) error {
	ctx, span := a.cfg.Tracer.Start(ctx, "commitAndHarvest")
	defer span.End()

	var errs []error
	for _, batch := range batches {
		if err := batch.Commit(a.writeOptions); err != nil {
			span.RecordError(err)
			errs = append(errs, fmt.Errorf("failed to commit batch before harvest: %w", err))
		}
		if err := batch.Close(); err != nil {
			span.RecordError(err)
			errs = append(errs, fmt.Errorf("failed to close batch before harvest: %w", err))
		}
	}
	if err := a.harvest(ctx, to, cachedEventsStats); err != nil {
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
	cachedEventsStats map[time.Duration]map[[16]byte]float64,
) error {
	snap := a.db.NewSnapshot()
	defer snap.Close()

	var errs []error
	for _, ivl := range a.cfg.AggregationIntervals {
		// Check if the given aggregation interval needs to be harvested now
		if end.Truncate(ivl).Equal(end) {
			start := end.Add(-ivl)
			cmCount, err := a.harvestForInterval(
				ctx, snap, start, end, ivl, cachedEventsStats[ivl],
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
	cachedEventsStats map[[16]byte]float64,
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
	for cmID, eventsTotal := range cachedEventsStats {
		attrs := append(a.cfg.CombinedMetricsIDToKVs(cmID), ivlAttr)
		a.metrics.EventsTotal.Add(ctx, eventsTotal, metric.WithAttributes(attrs...))
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

		attrs := append(a.cfg.CombinedMetricsIDToKVs(cmk.ID), ivlAttr)
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
	err := a.db.DeleteRange(lb, ub, a.writeOptions)
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
