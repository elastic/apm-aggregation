// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const instrumentationName = "aggregators"

// Processor defines handling of the aggregated metrics post harvest.
type Processor func(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm CombinedMetrics,
	aggregationIvl time.Duration,
) error

// Partitioner partitions the aggregation key based on the configured
// partition logic.
type Partitioner interface {
	Partition(Hasher) uint16
}

// Config contains the required config for running the aggregator.
type Config struct {
	DataDir                string
	Limits                 Limits
	Processor              Processor
	Partitioner            Partitioner
	AggregationIntervals   []time.Duration
	HarvestDelay           time.Duration
	CombinedMetricsIDToKVs func([16]byte) []attribute.KeyValue
	InMemory               bool

	Meter  metric.Meter
	Tracer trace.Tracer
	Logger *zap.Logger
}

// Option allows configuring aggregator based on functional options.
type Option func(Config) Config

// NewConfig creates a new aggregator config based on the passed options.
func NewConfig(opts ...Option) (Config, error) {
	cfg := defaultCfg()
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	return cfg, validateCfg(cfg)
}

// WithDataDir configures the data directory to be used by the database.
func WithDataDir(dataDir string) Option {
	return func(c Config) Config {
		c.DataDir = dataDir
		return c
	}
}

// WithLimits configures the limits to be used by the aggregator.
func WithLimits(limits Limits) Option {
	return func(c Config) Config {
		c.Limits = limits
		return c
	}
}

// WithProcessor configures the processor for handling of the aggregated
// metrics post harvest. Processor is called for each decoded combined
// metrics after they are harvested.
func WithProcessor(processor Processor) Option {
	return func(c Config) Config {
		c.Processor = processor
		return c
	}
}

// WithPartitioner configures a partitioner for partitioning the combined
// metrics in pebble. Partition IDs are encoded in a way that all the
// partitions of a specific combined metric are listed before any other if
// compared using the bytes comparer.
func WithPartitioner(partitioner Partitioner) Option {
	return func(c Config) Config {
		c.Partitioner = partitioner
		return c
	}
}

// WithAggregationIntervals defines the intervals that aggregator will
// aggregate for.
func WithAggregationIntervals(aggIvls []time.Duration) Option {
	return func(c Config) Config {
		c.AggregationIntervals = aggIvls
		return c
	}
}

// WithHarvestDelay delays the harvest by the configured duration.
// This means that harvest for a specific processing time would be
// performed with the given delay.
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
func WithHarvestDelay(delay time.Duration) Option {
	return func(c Config) Config {
		c.HarvestDelay = delay
		return c
	}
}

// WithMeter defines a custom meter which will be used for collecting
// telemetry. Defaults to the meter provided by global provider.
func WithMeter(meter metric.Meter) Option {
	return func(c Config) Config {
		c.Meter = meter
		return c
	}
}

// WithTracer defines a custom tracer which will be used for collecting
// traces. Defaults to the tracer provided by global provider.
func WithTracer(tracer trace.Tracer) Option {
	return func(c Config) Config {
		c.Tracer = tracer
		return c
	}
}

// WithCombinedMetricsIDToKVs defines a function that converts a combined
// metrics ID to zero or more attribute.KeyValue for telemetry.
func WithCombinedMetricsIDToKVs(f func([16]byte) []attribute.KeyValue) Option {
	return func(c Config) Config {
		c.CombinedMetricsIDToKVs = f
		return c
	}
}

// WithLogger defines a custom logger to be used by aggregator.
func WithLogger(logger *zap.Logger) Option {
	return func(c Config) Config {
		c.Logger = logger
		return c
	}
}

// WithInMemory defines whether aggregator uses in-memory file system.
func WithInMemory(enabled bool) Option {
	return func(c Config) Config {
		c.InMemory = enabled
		return c
	}
}

func defaultCfg() Config {
	return Config{
		DataDir:                "/tmp",
		Processor:              stdoutProcessor,
		Partitioner:            NewHashPartitioner(1),
		AggregationIntervals:   []time.Duration{time.Minute},
		Meter:                  otel.Meter(instrumentationName),
		Tracer:                 otel.Tracer(instrumentationName),
		CombinedMetricsIDToKVs: func(_ [16]byte) []attribute.KeyValue { return nil },
		Logger:                 zap.Must(zap.NewDevelopment()),
	}
}

func validateCfg(cfg Config) error {
	if cfg.DataDir == "" {
		return errors.New("data directory is required")
	}
	if cfg.Processor == nil {
		return errors.New("processor is required")
	}
	if cfg.Partitioner == nil {
		return errors.New("partitioner is required")
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
	// For encoding/decoding the processing time for combined metrics we only
	// consider seconds granularity making 1 sec the lowest possible
	// aggregation interval. We also encode interval as 2 unsigned bytes making
	// 65535 (~18 hours) the highest possible aggregation interval.
	if lowest < time.Second {
		return errors.New("aggregation interval less than one second is not supported")
	}
	if highest > 18*time.Hour {
		return errors.New("aggregation interval greater than 18 hours is not supported")
	}
	return nil
}

func stdoutProcessor(
	ctx context.Context,
	cmk CombinedMetricsKey,
	cm CombinedMetrics,
	aggregationIvl time.Duration,
) error {
	fmt.Printf("Recevied combined metrics with key: %+v\n", cmk)
	return nil
}
