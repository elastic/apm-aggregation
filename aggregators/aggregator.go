// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package aggregators holds the logic for doing the actual aggregation.
package aggregators

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
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

// Processor defines handling of the aggregated metrics post harvest.
type Processor func(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm CombinedMetrics,
	aggregationIvl time.Duration,
) error

// Aggregator represents a LSM based aggregator instance to generate
// aggregated metrics. The metrics aggregated by the aggregator are
// harvested based on the aggregation interval and processed by the
// defined processor. The aggregated metrics are timestamped based
// on when the aggregator is created and the harvest loop. All the
// events collected between call to New and Run are collected in the
// same processing time bucket and thereafter the processing time
// bucket is advanced in factors of aggregation interval.
type Aggregator struct {
	db        *pebble.DB
	limits    Limits
	processor Processor

	aggregationIntervals []time.Duration
	harvestDelay         time.Duration

	mu             sync.Mutex
	processingTime time.Time
	batch          *pebble.Batch
	cachedStats    map[time.Duration]map[string]stats

	stopping   chan struct{}
	runStarted atomic.Bool
	runStopped chan struct{}

	metrics *telemetry.Metrics
	tracer  trace.Tracer
	logger  *zap.Logger

	combinedMetricsIDToKVs func(string) []attribute.KeyValue
}

// AggregatorConfig contains the required config for running the
// aggregator.
type AggregatorConfig struct {
	DataDir string
	Limits  Limits
	// Processor defines handling of the aggregated metrics post
	// harvest. Processor is called for each decoded combined metrics
	// after they are harvested.
	Processor Processor
	// AggregationIntervals defines the intervals that aggregator
	// will aggregate for. Note that the aggregation intervals
	// used for second level aggregation must be equal to the
	// aggregation intervals used for first level aggregations.
	AggregationIntervals []time.Duration
	// HarvestDelay delays the harvest by the configured duration.
	// This means that harvest for a specific processing time
	// would be performed with HarvestDelay.
	//
	// Without delay, a normal harvest schedule will harvest metrics
	// aggregated for processing time, say `t0`, at time `t1`, where
	// `t1 = t0 + aggregation_interval`. With delay of, say `d`, the
	// harvester will harvest the metrics for `t0` at `t1 + d`. In
	// addition to harvest the duration for which the metrics are
	// aggregated by the AggregateBatch API will also be affected.
	//
	// The main purpose of the delay is to handle the latency of
	// receiving the l1 aggregated metrics in l2 aggregation. Thus
	// the value must be configured for the l2 aggregator and is
	// not required for l1 aggregator. If used as such then the
	// harvest delay has no effects on the duration for which the
	// metrics are aggregated. This is because AggregateBatch API is
	// not used by the l2 aggregator.
	HarvestDelay time.Duration
	// A custom tracer which will be used by the aggregator.
	// Defaults to a tracer retrieved from the global TracerProvider.
	Tracer trace.Tracer
	// A custom meter provider which will be used by the telemetry.
	// Defaults to the global MeterProvider.
	MeterProvider metric.MeterProvider

	// Optional. A function that converts a combined metrics ID
	// to zero or more attribute.KeyValue for telemetry.
	CombinedMetricsIDToKVs func(string) []attribute.KeyValue
}

// stats is used to cache request based stats accepted by the
// pipeline and publish them once harvest is performed. This
// fixes the time shift introduced by the aggregation interval.
// Mostly useful for metrics which needs to be correlated with
// harvest time metrics.
type stats struct {
	eventsTotal int64
}

func (s *stats) merge(from stats) {
	s.eventsTotal += from.eventsTotal
}

// New returns a new aggregator instance.
func New(cfg AggregatorConfig, logger *zap.Logger) (*Aggregator, error) {
	if err := validateCfg(cfg); err != nil {
		return nil, err
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
		telemetry.WithMeterProvider(cfg.MeterProvider),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics: %w", err)
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = otel.Tracer("aggregators")
	}

	combinedMetricsIDToKVs := cfg.CombinedMetricsIDToKVs
	if combinedMetricsIDToKVs == nil {
		combinedMetricsIDToKVs = func(_ string) []attribute.KeyValue { return nil }
	}

	return &Aggregator{
		db:                     pb,
		limits:                 cfg.Limits,
		processor:              cfg.Processor,
		harvestDelay:           cfg.HarvestDelay,
		aggregationIntervals:   cfg.AggregationIntervals,
		processingTime:         time.Now().Truncate(cfg.AggregationIntervals[0]),
		cachedStats:            newCachedStats(cfg.AggregationIntervals),
		stopping:               make(chan struct{}),
		runStopped:             make(chan struct{}),
		metrics:                metrics,
		logger:                 logger,
		tracer:                 tracer,
		combinedMetricsIDToKVs: combinedMetricsIDToKVs,
	}, nil
}

func validateCfg(cfg AggregatorConfig) error {
	if cfg.DataDir == "" {
		return errors.New("data directory is required")
	}
	if cfg.Processor == nil {
		return errors.New("processor is required")
	}
	if len(cfg.AggregationIntervals) == 0 {
		return errors.New("at least one aggregation interval is required")
	}
	if !sort.SliceIsSorted(cfg.AggregationIntervals, func(i, j int) bool {
		return cfg.AggregationIntervals[i] < cfg.AggregationIntervals[j]
	}) {
		return errors.New("aggregation intervals must be in ascending order")
	}
	lowest := cfg.AggregationIntervals[0]
	highest := cfg.AggregationIntervals[len(cfg.AggregationIntervals)-1]
	for i := 1; i < len(cfg.AggregationIntervals); i++ {
		ivl := cfg.AggregationIntervals[i]
		if ivl%lowest != 0 {
			return errors.New("aggregation intervals must be a factor of lowest interval")
		}
	}
	// For encoding/decoding the processing time for combined metrics we only consider
	// seconds granularity making 1 sec the lowest possible aggregation interval. We
	// also encode interval as 2 unsigned bytes making 65535 (~18 hours) the highest
	// possible aggregation interval.
	if lowest < time.Second {
		return errors.New("aggregation interval less than one second is not supported")
	}
	if highest > 18*time.Hour {
		return errors.New("aggregation interval greater than 18 hours is not supported")
	}
	return nil
}

func newCachedStats(ivls []time.Duration) map[time.Duration]map[string]stats {
	m := make(map[time.Duration]map[string]stats, len(ivls))
	for _, ivl := range ivls {
		m[ivl] = make(map[string]stats)
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
	cmIDAttrs := a.combinedMetricsIDToKVs(id)
	ctx, span := a.tracer.Start(ctx, "AggregateBatch", trace.WithAttributes(cmIDAttrs...))
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

	var errs []error
	var totalBytesIn int64
	cmk := CombinedMetricsKey{ID: id}
	for _, ivl := range a.aggregationIntervals {
		cmk.ProcessingTime = a.processingTime.Truncate(ivl)
		cmk.Interval = ivl
		for _, e := range *b {
			bytesIn, err := a.aggregateAPMEvent(ctx, cmk, e)
			if err != nil {
				span.RecordError(err)
				errs = append(errs, err)
			}
			totalBytesIn += int64(bytesIn)
		}
		cmStats := a.cachedStats[ivl][id]
		cmStats.eventsTotal += int64(len(*b))
		a.cachedStats[ivl][id] = cmStats
	}

	span.SetAttributes(attribute.Int64("total_bytes_ingested", totalBytesIn))
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
	cmIDAttrs := a.combinedMetricsIDToKVs(cmk.ID)
	traceAttrs := append(append([]attribute.KeyValue{}, cmIDAttrs...),
		attribute.String(aggregationIvlKey, formatDuration(cmk.Interval)),
		attribute.String("processing_time", cmk.ProcessingTime.String()))
	ctx, span := a.tracer.Start(ctx, "AggregateCombinedMetrics", trace.WithAttributes(traceAttrs...))
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
		a.cachedStats[cmk.Interval] = make(map[string]stats)
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

	to := a.processingTime.Add(a.aggregationIntervals[0])
	timer := time.NewTimer(time.Until(to.Add(a.harvestDelay)))
	harvestStats := newCachedStats(a.aggregationIntervals)
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
				harvestStats[ivl] = make(map[string]stats)
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
			a.logger.Warn("failed to commit and harvest metrics", zap.Error(err))
		}
		to = to.Add(a.aggregationIntervals[0])
		timer.Reset(time.Until(to.Add(a.harvestDelay)))
	}
}

// Stop stops the aggregator. Aggregations performed after calling Stop
// will return an error. Stop can be called multiple times but concurrent
// calls to stop will block.
func (a *Aggregator) Stop(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "Aggregator.Stop")
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

	a.logger.Info("stopping aggregator")
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.db != nil {
		a.logger.Info("running final aggregation")
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
		for _, ivl := range a.aggregationIntervals {
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
	traceAttrs := append(append([]attribute.KeyValue{}, a.combinedMetricsIDToKVs(cmk.ID)...),
		attribute.String(aggregationIvlKey, formatDuration(cmk.Interval)),
		attribute.String("processing_time", cmk.ProcessingTime.String()))
	ctx, span := a.tracer.Start(ctx, "aggregateAPMEvent", trace.WithAttributes(traceAttrs...))
	defer span.End()

	cm, err := EventToCombinedMetrics(e, cmk.Interval)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to convert event to combined metrics: %w", err)
	}
	bytesIn, err := a.aggregate(ctx, cmk, cm)
	span.SetAttributes(attribute.Int("bytes_ingested", bytesIn))
	if err != nil {
		span.RecordError(err)
		return bytesIn, fmt.Errorf("failed to aggregate combined metrics: %w", err)
	}
	return bytesIn, nil
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
	harvestStats map[time.Duration]map[string]stats,
) error {
	ctx, span := a.tracer.Start(ctx, "commitAndHarvest")
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
	harvestStats map[time.Duration]map[string]stats,
) error {
	snap := a.db.NewSnapshot()
	defer snap.Close()

	var errs []error
	for _, ivl := range a.aggregationIntervals {
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
			a.logger.Debug(
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
	cmStats map[string]stats,
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
		attrs := append([]attribute.KeyValue{ivlAttr}, a.combinedMetricsIDToKVs(cmID)...)
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
		eventsProcessed, err := a.processHarvest(ctx, cmk, iter.Value(), ivl)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cmCount++
		attrs := append([]attribute.KeyValue{ivlAttr}, a.combinedMetricsIDToKVs(cmk.ID)...)
		a.metrics.EventsProcessed.Add(
			ctx, eventsProcessed,
			metric.WithAttributeSet(
				attribute.NewSet(attrs...),
			),
		)
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

func (a *Aggregator) processHarvest(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cmb []byte,
	aggIvl time.Duration,
) (int64, error) {
	var cm CombinedMetrics
	if err := cm.UnmarshalBinary(cmb); err != nil {
		return 0, fmt.Errorf("failed to unmarshal metrics: %w", err)
	}
	if err := a.processor(ctx, cmk, cm, aggIvl); err != nil {
		return 0, fmt.Errorf(
			"failed to process combined metrics ID %s: %w",
			cmk.ID, err,
		)
	}
	return cm.eventsTotal, nil
}
