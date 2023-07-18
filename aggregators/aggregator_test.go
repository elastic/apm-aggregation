// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"context"
	"fmt"
	"math/rand"
	"net/netip"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/cockroachdb/pebble"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/apm/module/apmotel/v2"
	"go.elastic.co/apm/v2"
	"go.elastic.co/apm/v2/apmtest"
	apmmodel "go.elastic.co/apm/v2/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-data/model/modelpb"
)

func TestNew(t *testing.T) {
	agg, err := New()
	assert.NoError(t, err)
	assert.NotNil(t, agg)
}

func TestAggregateBatch(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	gatherer, err := apmotel.NewGatherer()
	require.NoError(t, err)
	mp := metric.NewMeterProvider(metric.WithReader(gatherer))

	cmID := "testid"
	txnDuration := 100 * time.Millisecond
	uniqueEventCount := 100 // for each of txns and spans
	uniqueServices := 10
	repCount := 5
	ts := time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)
	batch := make(modelpb.Batch, 0, uniqueEventCount*repCount*2)
	// Distribute the total unique transaction count amongst the total
	// unique services uniformly.
	for i := 0; i < uniqueEventCount*repCount; i++ {
		batch = append(batch, &modelpb.APMEvent{
			Processor: modelpb.TransactionProcessor(),
			Event: &modelpb.Event{
				Outcome:  "success",
				Duration: durationpb.New(txnDuration),
				Received: timestamppb.New(ts),
			},
			Transaction: &modelpb.Transaction{
				Name:                fmt.Sprintf("foo%d", i%uniqueEventCount),
				Type:                fmt.Sprintf("txtype%d", i%uniqueEventCount),
				RepresentativeCount: 1,
				DroppedSpansStats: []*modelpb.DroppedSpanStats{
					{
						DestinationServiceResource: fmt.Sprintf("dropped_dest_resource%d", i%uniqueEventCount),
						Outcome:                    "success",
						Duration: &modelpb.AggregatedDuration{
							Count: 1,
							Sum:   durationpb.New(10 * time.Millisecond),
						},
					},
				},
			},
			Service: &modelpb.Service{Name: fmt.Sprintf("svc%d", i%uniqueServices)},
		})
		batch = append(batch, &modelpb.APMEvent{
			Processor: modelpb.SpanProcessor(),
			Event: &modelpb.Event{
				Received: timestamppb.New(ts),
			},
			Span: &modelpb.Span{
				Name:                fmt.Sprintf("bar%d", i%uniqueEventCount),
				RepresentativeCount: 1,
				DestinationService: &modelpb.DestinationService{
					Resource: "test_dest",
				},
			},
			Service: &modelpb.Service{Name: fmt.Sprintf("svc%d", i%uniqueServices)},
		})
	}

	out := make(chan CombinedMetrics, 1)
	aggIvl := time.Minute
	agg, err := New(
		WithDataDir(t.TempDir()),
		WithLimits(Limits{
			MaxSpanGroups:                         1000,
			MaxSpanGroupsPerService:               100,
			MaxTransactionGroups:                  100,
			MaxTransactionGroupsPerService:        10,
			MaxServiceTransactionGroups:           100,
			MaxServiceTransactionGroupsPerService: 10,
			MaxServices:                           10,
			MaxServiceInstanceGroupsPerService:    10,
		}),
		WithProcessor(combinedMetricsProcessor(out)),
		WithAggregationIntervals([]time.Duration{aggIvl}),
		WithHarvestDelay(time.Hour), // disable auto harvest
		WithTracer(tp.Tracer("test")),
		WithMeter(mp.Meter("test")),
		WithCombinedMetricsIDToKVs(func(id string) []attribute.KeyValue {
			return []attribute.KeyValue{attribute.String("id_key", id)}
		}),
	)
	require.NoError(t, err)

	require.NoError(t, agg.AggregateBatch(context.Background(), cmID, &batch))
	require.NoError(t, agg.Stop(context.Background()))
	var cm CombinedMetrics
	select {
	case cm = <-out:
	default:
		t.Error("failed to get aggregated metrics")
		t.FailNow()
	}

	var span tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.Name == "AggregateBatch" {
			span = s
		}
	}
	assert.NotNil(t, span)

	expectedCombinedMetrics := CombinedMetrics{
		Services:               make(map[ServiceAggregationKey]ServiceMetrics),
		eventsTotal:            float64(len(batch)),
		youngestEventTimestamp: ts,
	}
	expectedMeasurements := []apmmodel.Metrics{
		{
			Samples: map[string]apmmodel.Metric{
				"aggregator.requests.total": {Value: 1},
				"aggregator.bytes.ingested": {Value: 149750},
			},
			Labels: apmmodel.StringMap{
				apmmodel.StringMapItem{Key: "id_key", Value: cmID},
			},
		},
		{
			Samples: map[string]apmmodel.Metric{
				"aggregator.events.total":     {Value: float64(len(batch))},
				"aggregator.events.processed": {Value: float64(len(batch))},
				"events.processing-delay":     {Type: "histogram", Counts: []uint64{1}, Values: []float64{0}},
				"events.queued-delay":         {Type: "histogram", Counts: []uint64{1}, Values: []float64{0}},
			},
			Labels: apmmodel.StringMap{
				apmmodel.StringMapItem{Key: aggregationIvlKey, Value: formatDuration(aggIvl)},
				apmmodel.StringMapItem{Key: "id_key", Value: cmID},
			},
		},
	}
	sik := ServiceInstanceAggregationKey{GlobalLabelsStr: ""}
	for i := 0; i < uniqueEventCount*repCount; i++ {
		svcKey := ServiceAggregationKey{
			Timestamp:   time.Unix(0, 0).UTC(),
			ServiceName: fmt.Sprintf("svc%d", i%uniqueServices),
		}
		txKey := TransactionAggregationKey{
			TraceRoot:       true,
			TransactionName: fmt.Sprintf("foo%d", i%uniqueEventCount),
			TransactionType: fmt.Sprintf("txtype%d", i%uniqueEventCount),
			EventOutcome:    "success",
		}
		stxKey := ServiceTransactionAggregationKey{
			TransactionType: fmt.Sprintf("txtype%d", i%uniqueEventCount),
		}
		spanKey := SpanAggregationKey{
			SpanName: fmt.Sprintf("bar%d", i%uniqueEventCount),
			Resource: "test_dest",
		}
		if _, ok := expectedCombinedMetrics.Services[svcKey]; !ok {
			expectedCombinedMetrics.Services[svcKey] = newServiceMetrics()
		}
		if _, ok := expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik]; !ok {
			expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik] = newServiceInstanceMetrics()
		}
		var ok bool
		var tm TransactionMetrics
		if tm, ok = expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].TransactionGroups[txKey]; !ok {
			tm = newTransactionMetrics()
		}
		tm.Histogram.RecordDuration(txnDuration, 1)
		expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].TransactionGroups[txKey] = tm
		var stm ServiceTransactionMetrics
		if stm, ok = expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].ServiceTransactionGroups[stxKey]; !ok {
			stm = newServiceTransactionMetrics()
		}
		stm.Histogram.RecordDuration(txnDuration, 1)
		stm.SuccessCount++
		expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].ServiceTransactionGroups[stxKey] = stm
		sm := expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].SpanGroups[spanKey]
		sm.Count++
		expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].SpanGroups[spanKey] = sm

		droppedSpanStatsKey := SpanAggregationKey{
			SpanName: "",
			Resource: fmt.Sprintf("dropped_dest_resource%d", i%uniqueEventCount),
			Outcome:  "success",
		}
		dssm := expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].SpanGroups[droppedSpanStatsKey]
		dssm.Count++
		dssm.Sum += float64(10 * time.Millisecond)
		expectedCombinedMetrics.Services[svcKey].ServiceInstanceGroups[sik].SpanGroups[droppedSpanStatsKey] = dssm
	}
	assert.Empty(t, cmp.Diff(
		expectedCombinedMetrics, cm,
		cmpopts.EquateEmpty(),
		cmpopts.EquateApprox(0, 0.01),
		cmp.Comparer(func(a, b hdrhistogram.HybridCountsRep) bool {
			return a.Equal(&b)
		}),
		cmp.AllowUnexported(CombinedMetrics{}),
	))
	assert.Empty(t, cmp.Diff(
		expectedMeasurements,
		gatherMetrics(
			gatherer,
			withIgnoreMetricPrefix("pebble."),
			withZeroHistogramValues(true),
		),
		cmpopts.IgnoreUnexported(apmmodel.Time{}),
		cmpopts.EquateApprox(0, 0.01),
	))
}

func TestAggregateSpanMetrics(t *testing.T) {
	type input struct {
		serviceName         string
		agentName           string
		destination         string
		targetType          string
		targetName          string
		outcome             string
		representativeCount float64
	}

	destinationX := "destination-X"
	destinationZ := "destination-Z"
	trgTypeX := "trg-type-X"
	trgNameX := "trg-name-X"
	trgTypeZ := "trg-type-Z"
	trgNameZ := "trg-name-Z"
	defaultLabels := modelpb.Labels{
		"department_name": &modelpb.LabelValue{Global: true, Value: "apm"},
		"organization":    &modelpb.LabelValue{Global: true, Value: "observability"},
		"company":         &modelpb.LabelValue{Global: true, Value: "elastic"},
	}
	defaultNumericLabels := modelpb.NumericLabels{
		"user_id":     &modelpb.NumericLabelValue{Global: true, Value: 100},
		"cost_center": &modelpb.NumericLabelValue{Global: true, Value: 10},
	}

	for _, tt := range []struct {
		name              string
		inputs            []input
		getExpectedEvents func(time.Time, time.Duration, time.Duration, int) []*modelpb.APMEvent
	}{
		{
			name: "with destination and service targets",
			inputs: []input{
				{serviceName: "service-A", agentName: "java", destination: destinationZ, targetType: trgTypeZ, targetName: trgNameZ, outcome: "success", representativeCount: 2},
				{serviceName: "service-A", agentName: "java", destination: destinationX, targetType: trgTypeX, targetName: trgNameX, outcome: "success", representativeCount: 1},
				{serviceName: "service-B", agentName: "python", destination: destinationZ, targetType: trgTypeZ, targetName: trgNameZ, outcome: "success", representativeCount: 1},
				{serviceName: "service-A", agentName: "java", destination: destinationZ, targetType: trgTypeZ, targetName: trgNameZ, outcome: "success", representativeCount: 1},
				{serviceName: "service-A", agentName: "java", destination: destinationZ, targetType: trgTypeZ, targetName: trgNameZ, outcome: "success", representativeCount: 0},
				{serviceName: "service-A", agentName: "java", destination: destinationZ, targetType: trgTypeZ, targetName: trgNameZ, outcome: "failure", representativeCount: 1},
			},
			getExpectedEvents: func(ts time.Time, duration, ivl time.Duration, count int) []*modelpb.APMEvent {
				return []*modelpb.APMEvent{
					{
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
						},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_summary",
							Interval: formatDuration(ivl),
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "python"},
						Service: &modelpb.Service{
							Name: "service-B",
						},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_summary",
							Interval: formatDuration(ivl),
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
							Target: &modelpb.ServiceTarget{
								Type: trgTypeX,
								Name: trgNameX,
							},
						},
						Event:     &modelpb.Event{Outcome: "success"},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_destination",
							Interval: formatDuration(ivl),
							DocCount: int64(count),
						},
						Span: &modelpb.Span{
							Name: "service-A:" + destinationX,
							DestinationService: &modelpb.DestinationService{
								Resource: destinationX,
								ResponseTime: &modelpb.AggregatedDuration{
									Count: int64(count),
									Sum:   durationpb.New(time.Duration(count) * duration),
								},
							},
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
							Target: &modelpb.ServiceTarget{
								Type: trgTypeZ,
								Name: trgNameZ,
							},
						},
						Event:     &modelpb.Event{Outcome: "failure"},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_destination",
							Interval: formatDuration(ivl),
							DocCount: int64(count),
						},
						Span: &modelpb.Span{
							Name: "service-A:" + destinationZ,
							DestinationService: &modelpb.DestinationService{
								Resource: destinationZ,
								ResponseTime: &modelpb.AggregatedDuration{
									Count: int64(count),
									Sum:   durationpb.New(time.Duration(count) * duration),
								},
							},
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
							Target: &modelpb.ServiceTarget{
								Type: trgTypeZ,
								Name: trgNameZ,
							},
						},
						Event:     &modelpb.Event{Outcome: "success"},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_destination",
							Interval: formatDuration(ivl),
							DocCount: int64(3 * count),
						},
						Span: &modelpb.Span{
							Name: "service-A:" + destinationZ,
							DestinationService: &modelpb.DestinationService{
								Resource: destinationZ,
								ResponseTime: &modelpb.AggregatedDuration{
									Count: int64(3 * count),
									Sum:   durationpb.New(time.Duration(3*count) * duration),
								},
							},
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "python"},
						Service: &modelpb.Service{
							Name: "service-B",
							Target: &modelpb.ServiceTarget{
								Type: trgTypeZ,
								Name: trgNameZ,
							},
						},
						Event:     &modelpb.Event{Outcome: "success"},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_destination",
							Interval: formatDuration(ivl),
							DocCount: int64(count),
						},
						Span: &modelpb.Span{
							Name: "service-B:" + destinationZ,
							DestinationService: &modelpb.DestinationService{
								Resource: destinationZ,
								ResponseTime: &modelpb.AggregatedDuration{
									Count: int64(count),
									Sum:   durationpb.New(time.Duration(count) * duration),
								},
							},
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					},
				}
			},
		}, {
			name: "with_no_destination_and_no_service_target",
			inputs: []input{
				{serviceName: "service-A", agentName: "java", outcome: "success", representativeCount: 1},
			},
			getExpectedEvents: func(_ time.Time, _, _ time.Duration, _ int) []*modelpb.APMEvent {
				return nil
			},
		}, {
			name: "with no destination and a service target",
			inputs: []input{
				{serviceName: "service-A", agentName: "java", targetType: trgTypeZ, targetName: trgNameZ, outcome: "success", representativeCount: 1},
			},
			getExpectedEvents: func(ts time.Time, duration, ivl time.Duration, count int) []*modelpb.APMEvent {
				return []*modelpb.APMEvent{
					{
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
						},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_summary",
							Interval: formatDuration(ivl),
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
							Target: &modelpb.ServiceTarget{
								Type: trgTypeZ,
								Name: trgNameZ,
							},
						},
						Event:     &modelpb.Event{Outcome: "success"},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_destination",
							Interval: formatDuration(ivl),
							DocCount: int64(count),
						},
						Span: &modelpb.Span{
							Name: "service-A:",
							DestinationService: &modelpb.DestinationService{
								ResponseTime: &modelpb.AggregatedDuration{
									Count: int64(count),
									Sum:   durationpb.New(time.Duration(count) * duration),
								},
							},
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					},
				}
			},
		}, {
			name: "with a destination and no service target",
			inputs: []input{
				{serviceName: "service-A", agentName: "java", destination: destinationZ, outcome: "success", representativeCount: 1},
			},
			getExpectedEvents: func(ts time.Time, duration, ivl time.Duration, count int) []*modelpb.APMEvent {
				return []*modelpb.APMEvent{
					{
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
						},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_summary",
							Interval: formatDuration(ivl),
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					}, {
						Timestamp: timestamppb.New(ts.Truncate(ivl)),
						Agent:     &modelpb.Agent{Name: "java"},
						Service: &modelpb.Service{
							Name: "service-A",
						},
						Event:     &modelpb.Event{Outcome: "success"},
						Processor: modelpb.MetricsetProcessor(),
						Metricset: &modelpb.Metricset{
							Name:     "service_destination",
							Interval: formatDuration(ivl),
							DocCount: int64(count),
						},
						Span: &modelpb.Span{
							Name: "service-A:" + destinationZ,
							DestinationService: &modelpb.DestinationService{
								Resource: destinationZ,
								ResponseTime: &modelpb.AggregatedDuration{
									Count: int64(count),
									Sum:   durationpb.New(time.Duration(count) * duration),
								},
							},
						},
						Labels:        defaultLabels,
						NumericLabels: defaultNumericLabels,
					},
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var actualEvents []*modelpb.APMEvent
			aggregationIvls := []time.Duration{time.Minute, 10 * time.Minute, time.Hour}
			agg, err := New(
				WithLimits(Limits{
					MaxSpanGroups:                         1000,
					MaxSpanGroupsPerService:               100,
					MaxTransactionGroups:                  100,
					MaxTransactionGroupsPerService:        10,
					MaxServiceTransactionGroups:           100,
					MaxServiceTransactionGroupsPerService: 10,
					MaxServices:                           10,
					MaxServiceInstanceGroupsPerService:    10,
				}),
				WithAggregationIntervals(aggregationIvls),
				WithProcessor(sliceProcessor(&actualEvents)),
				WithDataDir(t.TempDir()),
			)
			require.NoError(t, err)

			count := 100
			now := time.Now()
			duration := 100 * time.Millisecond
			for _, in := range tt.inputs {
				span := makeSpan(
					now,
					in.serviceName,
					in.agentName,
					in.destination,
					in.targetType,
					in.targetName,
					in.outcome,
					duration,
					in.representativeCount,
					defaultLabels,
					defaultNumericLabels,
				)
				for i := 0; i < count; i++ {
					err := agg.AggregateBatch(context.Background(), "testid", &modelpb.Batch{span})
					require.NoError(t, err)
				}
			}
			require.NoError(t, agg.Stop(context.Background()))
			var expectedEvents []*modelpb.APMEvent
			for _, ivl := range aggregationIvls {
				expectedEvents = append(expectedEvents, tt.getExpectedEvents(now, duration, ivl, count)...)
			}
			sortKey := func(e *modelpb.APMEvent) string {
				var sb strings.Builder
				sb.WriteString(e.GetService().GetName())
				sb.WriteString(e.GetAgent().GetName())
				sb.WriteString(e.GetMetricset().GetName())
				sb.WriteString(e.GetMetricset().GetInterval())
				destSvc := e.GetSpan().GetDestinationService()
				if destSvc != nil {
					sb.WriteString(destSvc.GetResource())
				}
				target := e.GetService().GetTarget()
				if target != nil {
					sb.WriteString(target.GetName())
					sb.WriteString(target.GetType())
				}
				sb.WriteString(e.GetEvent().GetOutcome())
				return sb.String()
			}
			sort.Slice(expectedEvents, func(i, j int) bool {
				return sortKey(expectedEvents[i]) < sortKey(expectedEvents[j])
			})
			sort.Slice(actualEvents, func(i, j int) bool {
				return sortKey(actualEvents[i]) < sortKey(actualEvents[j])
			})
			assert.Empty(t, cmp.Diff(
				expectedEvents, actualEvents,
				cmpopts.EquateEmpty(),
				cmpopts.IgnoreTypes(netip.Addr{}),
				protocmp.Transform(),
			))
		})
	}
}

func TestCombinedMetricsKeyOrdered(t *testing.T) {
	// To Allow for retrieving combined metrics by time range, the metrics should
	// be ordered by processing time.
	ts := time.Now().Add(-time.Hour)
	ivl := time.Minute

	before := CombinedMetricsKey{
		ProcessingTime: ts.Truncate(time.Minute),
		Interval:       ivl,
		ID:             "cm01",
	}
	marshaledBufferSize := before.SizeBinary()
	beforeBytes := make([]byte, marshaledBufferSize)
	afterBytes := make([]byte, marshaledBufferSize)

	for i := 0; i < 10; i++ {
		ts = ts.Add(time.Minute)
		after := CombinedMetricsKey{
			ProcessingTime: ts.Truncate(time.Minute),
			Interval:       ivl,
			// combined metrics ID shouldn't matter. Keep length to be
			// 5 to ensure it is within expected bounds of the
			// sized buffer.
			ID: fmt.Sprintf("cm%02d", rand.Intn(100)),
		}
		require.NoError(t, after.MarshalBinaryToSizedBuffer(afterBytes))
		require.NoError(t, before.MarshalBinaryToSizedBuffer(beforeBytes))

		// before should always come first
		assert.Equal(t, -1, pebble.DefaultComparer.Compare(beforeBytes, afterBytes))

		before = after
	}
}

// Keys should be ordered such that all the partitions for a specific ID is listed
// before any other combined metrics ID.
func TestCombinedMetricsKeyOrderedByProjectID(t *testing.T) {
	// To Allow for retrieving combined metrics by time range, the metrics should
	// be ordered by processing time.
	ts := time.Now().Add(-time.Hour)
	ivl := time.Minute

	keyTemplate := CombinedMetricsKey{
		ProcessingTime: ts.Truncate(time.Minute),
		Interval:       ivl,
	}
	cmCount := 1000
	pidCount := 500
	keys := make([]CombinedMetricsKey, 0, cmCount*pidCount)

	for i := 0; i < cmCount; i++ {
		cmID := fmt.Sprintf("cm%05d", i)
		for k := 0; k < pidCount; k++ {
			key := keyTemplate
			key.PartitionID = uint16(k)
			key.ID = cmID
			keys = append(keys, key)
		}
	}

	before := keys[0]
	marshaledBufferSize := before.SizeBinary()
	beforeBytes := make([]byte, marshaledBufferSize)
	afterBytes := make([]byte, marshaledBufferSize)

	for i := 1; i < len(keys); i++ {
		ts = ts.Add(time.Minute)
		after := keys[i]
		require.NoError(t, after.MarshalBinaryToSizedBuffer(afterBytes))
		require.NoError(t, before.MarshalBinaryToSizedBuffer(beforeBytes))

		// before should always come first
		if !assert.Equal(
			t, -1,
			pebble.DefaultComparer.Compare(beforeBytes, afterBytes),
			fmt.Sprintf("(%s, %d) should come before (%s, %d)", before.ID, before.PartitionID, after.ID, after.PartitionID),
		) {
			assert.FailNow(t, "keys not in expected order")
		}

		before = after
	}
}

func TestHarvest(t *testing.T) {
	cmCount := 5
	ivls := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	m := make(map[time.Duration]map[string]bool)
	processorDone := make(chan struct{})
	processor := func(
		_ context.Context,
		cmk CombinedMetricsKey,
		cm CombinedMetrics,
		ivl time.Duration,
	) error {
		cmMap, ok := m[ivl]
		if !ok {
			m[ivl] = make(map[string]bool)
			cmMap = m[ivl]
		}
		// For each unique interval, we should only have a single combined metrics ID
		if _, ok := cmMap[cmk.ID]; ok {
			assert.FailNow(t, "duplicate combined metrics ID found")
		}
		cmMap[cmk.ID] = true
		// For successful harvest, all combined metrics IDs foreach interval should be
		// harvested
		if len(m) == len(ivls) {
			var remaining bool
			for k := range m {
				if len(m[k]) != cmCount {
					remaining = true
				}
			}
			if !remaining {
				close(processorDone)
			}
		}
		return nil
	}
	gatherer, err := apmotel.NewGatherer()
	require.NoError(t, err)

	agg, err := New(
		WithDataDir(t.TempDir()),
		WithLimits(Limits{
			MaxSpanGroups:                         1000,
			MaxTransactionGroups:                  100,
			MaxTransactionGroupsPerService:        10,
			MaxServiceTransactionGroups:           100,
			MaxServiceTransactionGroupsPerService: 10,
			MaxServices:                           10,
			MaxServiceInstanceGroupsPerService:    10,
		}),
		WithProcessor(processor),
		WithAggregationIntervals(ivls),
		WithMeter(metric.NewMeterProvider(metric.WithReader(gatherer)).Meter("test")),
		WithCombinedMetricsIDToKVs(func(id string) []attribute.KeyValue {
			return []attribute.KeyValue{attribute.String("id_key", id)}
		}),
	)
	require.NoError(t, err)
	go func() {
		agg.Run(context.Background())
	}()
	t.Cleanup(func() {
		agg.Stop(context.Background())
	})

	var batch modelpb.Batch
	batch = append(batch, &modelpb.APMEvent{
		Processor: modelpb.TransactionProcessor(),
		Transaction: &modelpb.Transaction{
			Name:                "txn",
			RepresentativeCount: 1,
		},
	})
	expectedMeasurements := make([]apmmodel.Metrics, 0, cmCount+(cmCount*len(ivls)))
	for i := 0; i < cmCount; i++ {
		cmID := fmt.Sprintf("testid%d", i)
		require.NoError(t, agg.AggregateBatch(context.Background(), cmID, &batch))
		expectedMeasurements = append(expectedMeasurements, apmmodel.Metrics{
			Samples: map[string]apmmodel.Metric{
				"aggregator.requests.total": {Value: 1},
				"aggregator.bytes.ingested": {Value: 282},
			},
			Labels: apmmodel.StringMap{
				apmmodel.StringMapItem{Key: "id_key", Value: cmID},
			},
		})
		for _, ivl := range ivls {
			expectedMeasurements = append(expectedMeasurements, apmmodel.Metrics{
				Samples: map[string]apmmodel.Metric{
					"aggregator.events.total":     {Value: float64(len(batch))},
					"aggregator.events.processed": {Value: float64(len(batch))},
					"events.processing-delay":     {Type: "histogram", Counts: []uint64{1}, Values: []float64{0}},
					"events.queued-delay":         {Type: "histogram", Counts: []uint64{1}, Values: []float64{0}},
				},
				Labels: apmmodel.StringMap{
					apmmodel.StringMapItem{Key: aggregationIvlKey, Value: ivl.String()},
					apmmodel.StringMapItem{Key: "id_key", Value: cmID},
				},
			})
		}
	}

	// The test is designed to timeout if it fails. The test asserts most of the
	// logic in processor. If all expected metrics are harvested then the
	// processor broadcasts this by closing the processorDone channel and we call
	// it a success. If the harvest hasn't finished then the test times out and
	// we call it a failure. Due to the nature of how the aggregator works, it is
	// possible that this test becomes flaky if there is a bug.
	select {
	case <-processorDone:
	case <-time.After(8 * time.Second):
		t.Fatal("harvest didn't finish within expected time")
	}
	assert.Empty(t, cmp.Diff(
		expectedMeasurements,
		gatherMetrics(
			gatherer,
			withIgnoreMetricPrefix("pebble."),
			withZeroHistogramValues(true),
		),
		cmpopts.IgnoreUnexported(apmmodel.Time{}),
		cmpopts.SortSlices(func(a, b apmmodel.Metrics) bool {
			if len(a.Labels) != len(b.Labels) {
				return len(a.Labels) < len(b.Labels)
			}
			for i := 0; i < len(a.Labels); i++ {
				// assuming keys are ordered
				if a.Labels[i].Value != b.Labels[i].Value {
					return a.Labels[i].Value < b.Labels[i].Value
				}
			}
			return false
		}),
	))
}

func TestAggregateAndHarvest(t *testing.T) {
	txnDuration := 100 * time.Millisecond
	batch := modelpb.Batch{
		{
			Processor: modelpb.TransactionProcessor(),
			Event: &modelpb.Event{
				Outcome:  "success",
				Duration: durationpb.New(txnDuration),
			},
			Transaction: &modelpb.Transaction{
				Name:                "foo",
				Type:                "txtype",
				RepresentativeCount: 1,
			},
			Service: &modelpb.Service{Name: "svc"},
			Labels: modelpb.Labels{
				"department_name": &modelpb.LabelValue{Global: true, Value: "apm"},
				"organization":    &modelpb.LabelValue{Global: true, Value: "observability"},
				"company":         &modelpb.LabelValue{Global: true, Value: "elastic"},
				"mylabel":         &modelpb.LabelValue{Global: false, Value: "myvalue"},
			},
			NumericLabels: modelpb.NumericLabels{
				"user_id":        &modelpb.NumericLabelValue{Global: true, Value: 100},
				"cost_center":    &modelpb.NumericLabelValue{Global: true, Value: 10},
				"mynumericlabel": &modelpb.NumericLabelValue{Global: false, Value: 1},
			},
		},
	}
	var events []*modelpb.APMEvent
	agg, err := New(
		WithDataDir(t.TempDir()),
		WithLimits(Limits{
			MaxSpanGroups:                         1000,
			MaxSpanGroupsPerService:               100,
			MaxTransactionGroups:                  100,
			MaxTransactionGroupsPerService:        10,
			MaxServiceTransactionGroups:           100,
			MaxServiceTransactionGroupsPerService: 10,
			MaxServices:                           10,
			MaxServiceInstanceGroupsPerService:    10,
		}),
		WithProcessor(sliceProcessor(&events)),
		WithAggregationIntervals([]time.Duration{time.Second}),
	)
	require.NoError(t, err)
	require.NoError(t, agg.AggregateBatch(context.Background(), "test", &batch))
	require.NoError(t, agg.Stop(context.Background()))

	expected := []*modelpb.APMEvent{
		{
			Timestamp: timestamppb.New(time.Unix(0, 0).UTC()),
			Processor: modelpb.MetricsetProcessor(),
			Event: &modelpb.Event{
				SuccessCount: &modelpb.SummaryMetric{
					Count: 1,
					Sum:   1,
				},
				Outcome: "success",
			},
			Transaction: &modelpb.Transaction{
				Name: "foo",
				Type: "txtype",
				Root: true,
				DurationSummary: &modelpb.SummaryMetric{
					Count: 1,
					Sum:   100351, // Estimate from histogram
				},
				DurationHistogram: &modelpb.Histogram{
					Values: []float64{100351},
					Counts: []int64{1},
				},
			},
			Service: &modelpb.Service{
				Name: "svc",
			},
			Labels: modelpb.Labels{
				"department_name": &modelpb.LabelValue{Global: true, Value: "apm"},
				"organization":    &modelpb.LabelValue{Global: true, Value: "observability"},
				"company":         &modelpb.LabelValue{Global: true, Value: "elastic"},
			},
			NumericLabels: modelpb.NumericLabels{
				"user_id":     &modelpb.NumericLabelValue{Global: true, Value: 100},
				"cost_center": &modelpb.NumericLabelValue{Global: true, Value: 10},
			},
			Metricset: &modelpb.Metricset{
				Name:     "transaction",
				DocCount: 1,
				Interval: "1s",
			},
		},
		{
			Timestamp: timestamppb.New(time.Unix(0, 0).UTC()),
			Processor: modelpb.MetricsetProcessor(),
			Service: &modelpb.Service{
				Name: "svc",
			},
			Labels: modelpb.Labels{
				"department_name": &modelpb.LabelValue{Global: true, Value: "apm"},
				"organization":    &modelpb.LabelValue{Global: true, Value: "observability"},
				"company":         &modelpb.LabelValue{Global: true, Value: "elastic"},
			},
			NumericLabels: modelpb.NumericLabels{
				"user_id":     &modelpb.NumericLabelValue{Global: true, Value: 100},
				"cost_center": &modelpb.NumericLabelValue{Global: true, Value: 10},
			},
			Metricset: &modelpb.Metricset{
				Name:     "service_summary",
				Interval: "1s",
			},
		},
		{
			Timestamp: timestamppb.New(time.Unix(0, 0).UTC()),
			Processor: modelpb.MetricsetProcessor(),
			Event: &modelpb.Event{
				SuccessCount: &modelpb.SummaryMetric{
					Count: 1,
					Sum:   1,
				},
			},
			Transaction: &modelpb.Transaction{
				Type: "txtype",
				DurationSummary: &modelpb.SummaryMetric{
					Count: 1,
					Sum:   100351, // Estimate from histogram
				},
				DurationHistogram: &modelpb.Histogram{
					Values: []float64{100351},
					Counts: []int64{1},
				},
			},
			Service: &modelpb.Service{
				Name: "svc",
			},
			Labels: modelpb.Labels{
				"department_name": &modelpb.LabelValue{Global: true, Value: "apm"},
				"organization":    &modelpb.LabelValue{Global: true, Value: "observability"},
				"company":         &modelpb.LabelValue{Global: true, Value: "elastic"},
			},
			NumericLabels: modelpb.NumericLabels{
				"user_id":     &modelpb.NumericLabelValue{Global: true, Value: 100},
				"cost_center": &modelpb.NumericLabelValue{Global: true, Value: 10},
			},
			Metricset: &modelpb.Metricset{
				Name:     "service_transaction",
				DocCount: 1,
				Interval: "1s",
			},
		},
	}
	assert.Empty(t, cmp.Diff(
		expected,
		events,
		cmpopts.IgnoreTypes(netip.Addr{}),
		cmpopts.SortSlices(func(a, b *modelpb.APMEvent) bool {
			return a.Metricset.Name < b.Metricset.Name
		}),
		protocmp.Transform(),
	))
}

func TestRunStopOrchestration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var firstHarvestDone atomic.Bool
	newAggregator := func() *Aggregator {
		agg, err := New(
			WithDataDir(t.TempDir()),
			WithProcessor(func(_ context.Context, _ CombinedMetricsKey, _ CombinedMetrics, _ time.Duration) error {
				firstHarvestDone.Swap(true)
				return nil
			}),
			WithAggregationIntervals([]time.Duration{time.Second}),
		)
		if err != nil {
			t.Fatal("failed to create test aggregator", err)
		}
		return agg
	}
	callAggregateBatch := func(agg *Aggregator) error {
		return agg.AggregateBatch(
			context.Background(),
			"testid",
			&modelpb.Batch{
				&modelpb.APMEvent{
					Processor: modelpb.TransactionProcessor(),
					Event:     &modelpb.Event{Duration: durationpb.New(time.Millisecond)},
					Transaction: &modelpb.Transaction{
						Name:                "T-1000",
						RepresentativeCount: 1,
					},
				},
			},
		)
	}

	t.Run("run_before_stop", func(t *testing.T) {
		agg := newAggregator()
		// Should aggregate even without running
		assert.NoError(t, callAggregateBatch(agg))
		go func() { agg.Run(ctx) }()
		assert.Eventually(t, func() bool {
			return firstHarvestDone.Load()
		}, 10*time.Second, 10*time.Millisecond, "failed while waiting for first harvest")
		assert.NoError(t, callAggregateBatch(agg))
		assert.NoError(t, agg.Stop(ctx))
		assert.ErrorIs(t, callAggregateBatch(agg), ErrAggregatorStopped)
	})
	t.Run("stop_before_run", func(t *testing.T) {
		agg := newAggregator()
		assert.NoError(t, agg.Stop(ctx))
		assert.ErrorIs(t, callAggregateBatch(agg), ErrAggregatorStopped)
		assert.ErrorIs(t, agg.Run(ctx), ErrAggregatorStopped)
	})
	t.Run("multiple_run", func(t *testing.T) {
		agg := newAggregator()
		defer agg.Stop(ctx)

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error { return agg.Run(ctx) })
		g.Go(func() error { return agg.Run(ctx) })
		assert.ErrorIs(t, g.Wait(), ErrAggregatorAlreadyRunning)
	})
	t.Run("multiple_stop", func(t *testing.T) {
		agg := newAggregator()
		defer agg.Stop(ctx)
		go func() { agg.Run(ctx) }()
		time.Sleep(time.Second)

		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error { return agg.Stop(ctx) })
		g.Go(func() error { return agg.Stop(ctx) })
		assert.NoError(t, g.Wait())
	})
}

func BenchmarkAggregateCombinedMetrics(b *testing.B) {
	b.ReportAllocs()
	gatherer, err := apmotel.NewGatherer()
	if err != nil {
		b.Fatal(err)
	}
	mp := metric.NewMeterProvider(metric.WithReader(gatherer))
	aggIvl := time.Minute
	agg, err := New(
		WithDataDir(b.TempDir()),
		WithLimits(Limits{
			MaxSpanGroups:                         1000,
			MaxSpanGroupsPerService:               100,
			MaxTransactionGroups:                  1000,
			MaxTransactionGroupsPerService:        100,
			MaxServiceTransactionGroups:           1000,
			MaxServiceTransactionGroupsPerService: 100,
			MaxServices:                           100,
			MaxServiceInstanceGroupsPerService:    100,
		}),
		WithProcessor(noOpProcessor()),
		WithMeter(mp.Meter("test")),
		WithLogger(zap.NewNop()),
	)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		agg.Run(context.Background())
	}()
	b.Cleanup(func() {
		agg.Stop(context.Background())
	})
	cmk := CombinedMetricsKey{
		Interval:       aggIvl,
		ProcessingTime: time.Now().Truncate(aggIvl),
		ID:             "testid",
	}
	kvs, err := EventToCombinedMetrics(
		&modelpb.APMEvent{
			Processor: modelpb.TransactionProcessor(),
			Event:     &modelpb.Event{Duration: durationpb.New(time.Millisecond)},
			Transaction: &modelpb.Transaction{
				Name:                "T-1000",
				RepresentativeCount: 1,
			},
		},
		cmk, NewHashPartitioner(1),
	)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(func() { cancel() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for cmk, cm := range kvs {
			if err := agg.AggregateCombinedMetrics(ctx, cmk, *cm); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkAggregateBatchSerial(b *testing.B) {
	b.ReportAllocs()
	agg := newTestAggregator(b)
	batch := newTestBatchForBenchmark()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := agg.AggregateBatch(context.Background(), "test", batch); err != nil {
			b.Fatal(err)
		}
	}
	flushTestAggregator(b, agg)
}

func BenchmarkAggregateBatchParallel(b *testing.B) {
	b.ReportAllocs()
	agg := newTestAggregator(b)
	batch := newTestBatchForBenchmark()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := agg.AggregateBatch(context.Background(), "test", batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	flushTestAggregator(b, agg)
}

func newTestAggregator(tb testing.TB) *Aggregator {
	agg, err := New(
		WithDataDir(tb.TempDir()),
		WithLimits(Limits{
			MaxSpanGroups:                         1000,
			MaxSpanGroupsPerService:               100,
			MaxTransactionGroups:                  1000,
			MaxTransactionGroupsPerService:        100,
			MaxServiceTransactionGroups:           1000,
			MaxServiceTransactionGroupsPerService: 100,
			MaxServices:                           100,
			MaxServiceInstanceGroupsPerService:    100,
		}),
		WithProcessor(noOpProcessor()),
		WithAggregationIntervals([]time.Duration{time.Second, time.Minute, time.Hour}),
		WithLogger(zap.NewNop()),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return agg
}

func flushTestAggregator(tb testing.TB, agg *Aggregator) {
	if agg.batch != nil {
		if err := agg.batch.Commit(pebble.Sync); err != nil {
			tb.Fatal(err)
		}
		if err := agg.batch.Close(); err != nil {
			tb.Fatal(err)
		}
		agg.batch = nil
	}
	if err := agg.db.Close(); err != nil {
		tb.Fatal(err)
	}
}

func newTestBatchForBenchmark() *modelpb.Batch {
	return &modelpb.Batch{
		&modelpb.APMEvent{
			Processor: modelpb.TransactionProcessor(),
			Event:     &modelpb.Event{Duration: durationpb.New(time.Millisecond)},
			Transaction: &modelpb.Transaction{
				Name:                "T-1000",
				RepresentativeCount: 1,
			},
		},
	}
}

func noOpProcessor() Processor {
	return func(_ context.Context, _ CombinedMetricsKey, _ CombinedMetrics, _ time.Duration) error {
		return nil
	}
}

func combinedMetricsProcessor(out chan<- CombinedMetrics) Processor {
	return func(
		_ context.Context,
		_ CombinedMetricsKey,
		cm CombinedMetrics,
		_ time.Duration,
	) error {
		out <- cm
		return nil
	}
}

func sliceProcessor(slice *[]*modelpb.APMEvent) Processor {
	return func(
		ctx context.Context,
		cmk CombinedMetricsKey,
		cm CombinedMetrics,
		aggregationIvl time.Duration,
	) error {
		batch, err := CombinedMetricsToBatch(cm, cmk.ProcessingTime, aggregationIvl)
		if err != nil {
			return err
		}
		if batch != nil {
			for _, e := range *batch {
				*slice = append(*slice, e)
			}
		}
		return nil
	}
}

type gatherMetricsCfg struct {
	ignoreMetricPrefix  string
	zeroHistogramValues bool
}

type gatherMetricsOpt func(gatherMetricsCfg) gatherMetricsCfg

// withIgnoreMetricPrefix ignores some metric prefixes from the gathered
// metrics.
func withIgnoreMetricPrefix(s string) gatherMetricsOpt {
	return func(cfg gatherMetricsCfg) gatherMetricsCfg {
		cfg.ignoreMetricPrefix = s
		return cfg
	}
}

// withZeroHistogramValues zeroes all histogram values if true. Useful
// for testing where histogram values are harder to estimate correctly.
func withZeroHistogramValues(b bool) gatherMetricsOpt {
	return func(cfg gatherMetricsCfg) gatherMetricsCfg {
		cfg.zeroHistogramValues = b
		return cfg
	}
}

func gatherMetrics(g apm.MetricsGatherer, opts ...gatherMetricsOpt) []apmmodel.Metrics {
	var cfg gatherMetricsCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()
	tracer.RegisterMetricsGatherer(g)
	tracer.SendMetrics(nil)
	metrics := tracer.Payloads().Metrics
	for i := range metrics {
		metrics[i].Timestamp = apmmodel.Time{}
	}

	for i, m := range metrics {
		for k, s := range m.Samples {
			// Remove internal metrics
			if strings.HasPrefix(k, "golang.") || strings.HasPrefix(k, "system.") {
				delete(m.Samples, k)
				continue
			}
			// Remove any metrics that has been explicitly ignored
			if cfg.ignoreMetricPrefix != "" && strings.HasPrefix(k, cfg.ignoreMetricPrefix) {
				delete(m.Samples, k)
				continue
			}
			// Zero out histogram values if required
			if s.Type == "histogram" && cfg.zeroHistogramValues {
				for j := range s.Values {
					s.Values[j] = 0
				}
			}
		}

		if len(m.Samples) == 0 {
			metrics[i] = metrics[len(metrics)-1]
			metrics = metrics[:len(metrics)-1]
		}
	}
	return metrics
}

func makeSpan(
	ts time.Time,
	serviceName, agentName, destinationServiceResource, targetType, targetName, outcome string,
	duration time.Duration,
	representativeCount float64,
	labels modelpb.Labels,
	numericLabels modelpb.NumericLabels,
) *modelpb.APMEvent {
	event := &modelpb.APMEvent{
		Timestamp: timestamppb.New(ts),
		Agent:     &modelpb.Agent{Name: agentName},
		Service:   &modelpb.Service{Name: serviceName},
		Event: &modelpb.Event{
			Outcome:  outcome,
			Duration: durationpb.New(duration),
		},
		Processor: modelpb.SpanProcessor(),
		Span: &modelpb.Span{
			Name:                serviceName + ":" + destinationServiceResource,
			RepresentativeCount: representativeCount,
		},
		Labels:        labels,
		NumericLabels: numericLabels,
	}
	if destinationServiceResource != "" {
		event.Span.DestinationService = &modelpb.DestinationService{
			Resource: destinationServiceResource,
		}
	}
	if targetType != "" {
		event.Service.Target = &modelpb.ServiceTarget{
			Type: targetType,
			Name: targetName,
		}
	}
	return event
}
