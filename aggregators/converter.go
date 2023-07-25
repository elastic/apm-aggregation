// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/axiomhq/hyperloglog"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	tspb "github.com/elastic/apm-aggregation/aggregators/internal/timestamppb"
	"github.com/elastic/apm-aggregation/aggregators/nullable"
	"github.com/elastic/apm-data/model/modelpb"
)

const (
	spanMetricsetName    = "service_destination"
	txnMetricsetName     = "transaction"
	svcTxnMetricsetName  = "service_transaction"
	summaryMetricsetName = "service_summary"

	overflowBucketName = "_other"
)

// EventToCombinedMetrics converts APMEvent to one or more CombinedMetrics and
// calls the provided callback for each pair of CombinedMetricsKey and
// CombinedMetrics. The callback MUST NOT hold the reference of the passed
// CombinedMetrics. If required, the callback can call CloneVT to clone the
// CombinedMetrics. If an event results in multiple metrics, they may be spread
// across different partitions.
//
// EventToCombinedMetrics will never produce overflow metrics, as it applies to a
// single APMEvent.
func EventToCombinedMetrics(
	e *modelpb.APMEvent,
	unpartitionedKey CombinedMetricsKey,
	p Partitioner,
	callback func(CombinedMetricsKey, *aggregationpb.CombinedMetrics) error,
) error {
	collector := combinedMetricsCollector{}
	svcKey := serviceKey(e, unpartitionedKey.Interval)
	svcInstanceKey, err := serviceInstanceKey(e)
	if err != nil {
		return err
	}
	hasher := Hasher{}.
		Chain(svcKey).
		Chain(svcInstanceKey)

	switch e.Type() {
	case modelpb.TransactionEventType:
		repCount := e.GetTransaction().GetRepresentativeCount()
		if repCount <= 0 {
			return nil
		}
		hdr := hdrhistogram.New()
		hdr.RecordDuration(e.GetEvent().GetDuration().AsDuration(), repCount)

		txnKey, sim := eventToTxnMetrics(e, hdr)
		collector.add(p.Partition(hasher.Chain(txnKey)), sim)

		svcTxnKey, sim := eventToServiceTxnMetrics(e, hdr)
		collector.add(p.Partition(hasher.Chain(svcTxnKey)), sim)

		for _, dss := range e.GetTransaction().GetDroppedSpansStats() {
			dssKey, sim := eventToDSSMetrics(repCount, dss)
			collector.add(p.Partition(hasher.Chain(dssKey)), sim)
		}
	case modelpb.SpanEventType:
		target := e.GetService().GetTarget()
		repCount := e.GetSpan().GetRepresentativeCount()
		destSvc := e.GetSpan().GetDestinationService().GetResource()
		if repCount <= 0 || (target == nil && destSvc == "") {
			return nil
		}

		spanKey, sim := eventToSpanMetrics(repCount, e)
		collector.add(p.Partition(hasher.Chain(spanKey)), sim)
	default:
		// All other event types should result in service summary metrics
		sim := aggregationpb.ServiceInstanceMetricsFromVTPool()
		collector.add(p.Partition(hasher), sim)
	}

	// Approximate events total by uniformly distributing the events total
	// amongst the partitioned key values.
	weightedEventsTotal := 1 / float64(len(collector))
	eventTS := tspb.TimeToPBTimestamp(e.GetEvent().GetReceived().AsTime())

	ksim := aggregationpb.KeyedServiceInstanceMetricsFromVTPool()
	ksim.Key = svcInstanceKey

	ksm := aggregationpb.KeyedServiceMetricsFromVTPool()
	ksm.Key, ksm.Metrics = svcKey, aggregationpb.ServiceMetricsFromVTPool()
	ksm.Metrics.ServiceInstanceMetrics = append(ksm.Metrics.ServiceInstanceMetrics, ksim)

	cm := aggregationpb.CombinedMetricsFromVTPool()
	defer cm.ReturnToVTPool()
	cm.ServiceMetrics = append(cm.ServiceMetrics, ksm)

	var errs []error
	for partitionID, sim := range collector {
		key := unpartitionedKey
		key.PartitionID = partitionID

		cm.ServiceMetrics[0].Metrics.ServiceInstanceMetrics[0].Metrics = sim
		cm.EventsTotal = weightedEventsTotal
		cm.YoungestEventTimestamp = uint64(eventTS)
		if err := callback(key, cm); err != nil {
			errs = append(errs, err)
		}
		cm.ServiceMetrics[0].Metrics.ServiceInstanceMetrics[0].Metrics = nil
		sim.ReturnToVTPool()
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed while executing callback: %w", errors.Join(errs...))
	}
	return nil
}

// CombinedMetricsToBatch converts CombinedMetrics to a batch of APMEvents.
func CombinedMetricsToBatch(
	cm CombinedMetrics,
	processingTime time.Time,
	aggInterval time.Duration,
) (*modelpb.Batch, error) {
	if len(cm.Services) == 0 {
		return nil, nil
	}

	batchSize := 0
	// service_summary overflow metric
	if cm.OverflowServiceInstancesEstimator != nil {
		batchSize++
	}

	for _, sm := range cm.Services {
		for _, sim := range sm.ServiceInstanceGroups {
			batchSize += len(sim.TransactionGroups)
			batchSize += len(sim.ServiceTransactionGroups)
			batchSize += len(sim.SpanGroups)

			// Each service instance will create a service summary metric
			batchSize++
		}

		if !sm.OverflowGroups.OverflowTransaction.Empty() {
			batchSize++
		}
		if !sm.OverflowGroups.OverflowServiceTransaction.Empty() {
			batchSize++
		}
		if !sm.OverflowGroups.OverflowSpan.Empty() {
			batchSize++
		}
	}

	b := make(modelpb.Batch, 0, batchSize)
	aggIntervalStr := formatDuration(aggInterval)
	for sk, sm := range cm.Services {
		for sik, sim := range sm.ServiceInstanceGroups {
			var gl GlobalLabels
			err := gl.UnmarshalString(sik.GlobalLabelsStr)
			if err != nil {
				return nil, err
			}
			getBaseEventWithLabels := func() *modelpb.APMEvent {
				event := getBaseEvent(sk)
				event.Labels = gl.Labels
				event.NumericLabels = gl.NumericLabels
				return event
			}

			// transaction metrics
			for tk, ktm := range sim.TransactionGroups {
				event := getBaseEventWithLabels()
				txnMetricsToAPMEvent(tk, ktm.Metrics, event, aggIntervalStr)
				b = append(b, event)
			}
			// service transaction metrics
			for stk, kstm := range sim.ServiceTransactionGroups {
				event := getBaseEventWithLabels()
				svcTxnMetricsToAPMEvent(stk, kstm.Metrics, event, aggIntervalStr)
				b = append(b, event)
			}
			// service destination metrics
			for spk, kspm := range sim.SpanGroups {
				event := getBaseEventWithLabels()
				spanMetricsToAPMEvent(spk, kspm.Metrics, event, aggIntervalStr)
				b = append(b, event)
			}

			// service summary metrics
			event := getBaseEventWithLabels()
			serviceMetricsToAPMEvent(event, aggIntervalStr)
			b = append(b, event)
		}

		if !sm.OverflowGroups.OverflowTransaction.Empty() {
			event := getBaseEvent(sk)
			overflowTxnMetricsToAPMEvent(
				processingTime,
				sm.OverflowGroups.OverflowTransaction,
				event,
				aggIntervalStr,
			)
			b = append(b, event)
		}
		if !sm.OverflowGroups.OverflowServiceTransaction.Empty() {
			event := getBaseEvent(sk)
			overflowSvcTxnMetricsToAPMEvent(
				processingTime,
				sm.OverflowGroups.OverflowServiceTransaction,
				event,
				aggIntervalStr,
			)
			b = append(b, event)
		}
		if !sm.OverflowGroups.OverflowSpan.Empty() {
			event := getBaseEvent(sk)
			overflowSpanMetricsToAPMEvent(
				processingTime,
				sm.OverflowGroups.OverflowSpan,
				event,
				aggIntervalStr,
			)
			b = append(b, event)
		}
	}
	if cm.OverflowServiceInstancesEstimator != nil {
		getOverflowBaseEvent := func() *modelpb.APMEvent {
			return &modelpb.APMEvent{
				Metricset: &modelpb.Metricset{},
				Service: &modelpb.Service{
					Name: overflowBucketName,
				},
			}
		}
		event := getOverflowBaseEvent()
		overflowServiceMetricsToAPMEvent(
			processingTime,
			cm.OverflowServiceInstancesEstimator,
			event,
			aggIntervalStr,
		)
		b = append(b, event)
		if !cm.OverflowServices.OverflowTransaction.Empty() {
			event := getOverflowBaseEvent()
			overflowTxnMetricsToAPMEvent(
				processingTime,
				cm.OverflowServices.OverflowTransaction,
				event,
				aggIntervalStr,
			)
			b = append(b, event)
		}
		if !cm.OverflowServices.OverflowServiceTransaction.Empty() {
			event := getOverflowBaseEvent()
			overflowSvcTxnMetricsToAPMEvent(
				processingTime,
				cm.OverflowServices.OverflowServiceTransaction,
				event,
				aggIntervalStr,
			)
			b = append(b, event)
		}
		if !cm.OverflowServices.OverflowSpan.Empty() {
			event := getOverflowBaseEvent()
			overflowSpanMetricsToAPMEvent(
				processingTime,
				cm.OverflowServices.OverflowSpan,
				event,
				aggIntervalStr,
			)
			b = append(b, event)
		}
	}
	return &b, nil
}

// eventToTxnMetrics converts an APMEvent to a transaction metrics. It accepts
// a histogram so that it can be reused between txn and service txn metrics.
func eventToTxnMetrics(
	e *modelpb.APMEvent,
	hdr *hdrhistogram.HistogramRepresentation,
) (
	*aggregationpb.TransactionAggregationKey,
	*aggregationpb.ServiceInstanceMetrics,
) {
	tm := aggregationpb.TransactionMetricsFromVTPool()
	tm.Histogram = HistogramToProto(hdr)

	txnKey := transactionKey(e)
	ktm := aggregationpb.KeyedTransactionMetricsFromVTPool()
	ktm.Key, ktm.Metrics = txnKey, tm

	sim := aggregationpb.ServiceInstanceMetricsFromVTPool()
	sim.TransactionMetrics = append(sim.TransactionMetrics, ktm)
	return txnKey, sim
}

// eventToServiceTxnMetrics converts an APMEvent to a transaction metrics. It
// accepts a histogram so that it can be reused between txn and service txn metrics.
func eventToServiceTxnMetrics(
	e *modelpb.APMEvent,
	hdr *hdrhistogram.HistogramRepresentation,
) (
	*aggregationpb.ServiceTransactionAggregationKey,
	*aggregationpb.ServiceInstanceMetrics,
) {
	stm := aggregationpb.ServiceTransactionMetricsFromVTPool()
	stm.Histogram = HistogramToProto(hdr)
	setMetricCountBasedOnOutcome(stm, e)

	svcTxnKey := serviceTransactionKey(e)
	kstm := aggregationpb.KeyedServiceTransactionMetricsFromVTPool()
	kstm.Key, kstm.Metrics = svcTxnKey, stm

	sim := aggregationpb.ServiceInstanceMetricsFromVTPool()
	sim.ServiceTransactionMetrics = append(sim.ServiceTransactionMetrics, kstm)
	return svcTxnKey, sim
}

func eventToSpanMetrics(
	repCount float64,
	e *modelpb.APMEvent,
) (
	*aggregationpb.SpanAggregationKey,
	*aggregationpb.ServiceInstanceMetrics,
) {
	var count uint32 = 1
	duration := e.GetEvent().GetDuration().AsDuration()
	if composite := e.GetSpan().GetComposite(); composite != nil {
		count = composite.GetCount()
		duration = time.Duration(composite.GetSum() * float64(time.Millisecond))
	}

	spm := aggregationpb.SpanMetricsFromVTPool()
	spm.Count = float64(count) * repCount
	spm.Sum = float64(duration) * repCount

	spanKey := spanKey(e)
	kspm := aggregationpb.KeyedSpanMetricsFromVTPool()
	kspm.Key, kspm.Metrics = spanKey, spm

	sim := aggregationpb.ServiceInstanceMetricsFromVTPool()
	sim.SpanMetrics = append(sim.SpanMetrics, kspm)
	return spanKey, sim
}

func eventToDSSMetrics(
	repCount float64,
	dss *modelpb.DroppedSpanStats,
) (
	*aggregationpb.SpanAggregationKey,
	*aggregationpb.ServiceInstanceMetrics,
) {
	spm := aggregationpb.SpanMetricsFromVTPool()
	spm.Count = float64(dss.GetDuration().GetCount()) * repCount
	spm.Sum = float64(dss.GetDuration().GetSum().AsDuration()) * repCount

	dssKey := droppedSpanStatsKey(dss)
	kspm := aggregationpb.KeyedSpanMetricsFromVTPool()
	kspm.Key, kspm.Metrics = dssKey, spm

	sim := aggregationpb.ServiceInstanceMetricsFromVTPool()
	sim.SpanMetrics = append(sim.SpanMetrics, kspm)
	return dssKey, sim
}

func getBaseEvent(key ServiceAggregationKey) *modelpb.APMEvent {
	event := &modelpb.APMEvent{
		Timestamp: timestamppb.New(key.Timestamp),
		Metricset: &modelpb.Metricset{},
		Service: &modelpb.Service{
			Name:        key.ServiceName,
			Environment: key.ServiceEnvironment,
		},
	}

	if key.ServiceLanguageName != "" {
		event.Service.Language = &modelpb.Language{
			Name: key.ServiceLanguageName,
		}
	}

	if key.AgentName != "" {
		event.Agent = &modelpb.Agent{
			Name: key.AgentName,
		}
	}

	return event
}

func serviceMetricsToAPMEvent(
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	// Most service keys will already be present in the base event
	baseEvent.Metricset = populateNil(baseEvent.Metricset)
	baseEvent.Metricset.Name = summaryMetricsetName
	baseEvent.Metricset.Interval = intervalStr
}

func txnMetricsToAPMEvent(
	key TransactionAggregationKey,
	metrics *aggregationpb.TransactionMetrics,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	histogram := hdrhistogram.New()
	HistogramFromProto(histogram, metrics.Histogram)
	totalCount, counts, values := histogram.Buckets()
	var eventSuccessCount modelpb.SummaryMetric
	switch key.EventOutcome {
	case "success":
		eventSuccessCount.Count = totalCount
		eventSuccessCount.Sum = float64(totalCount)
	case "failure":
		eventSuccessCount.Count = totalCount
	case "unknown":
		// Keep both Count and Sum as 0.
	}
	transactionDurationSummary := modelpb.SummaryMetric{
		Count: totalCount,
	}
	for i, v := range values {
		transactionDurationSummary.Sum += v * float64(counts[i])
	}

	baseEvent.Transaction = populateNil(baseEvent.Transaction)
	baseEvent.Transaction.Name = key.TransactionName
	baseEvent.Transaction.Type = key.TransactionType
	baseEvent.Transaction.Result = key.TransactionResult
	baseEvent.Transaction.Root = key.TraceRoot
	baseEvent.Transaction.DurationSummary = &transactionDurationSummary
	baseEvent.Transaction.DurationHistogram = &modelpb.Histogram{
		Counts: counts,
		Values: values,
	}

	baseEvent.Metricset = populateNil(baseEvent.Metricset)
	baseEvent.Metricset.Name = txnMetricsetName
	baseEvent.Metricset.DocCount = totalCount
	baseEvent.Metricset.Interval = intervalStr

	baseEvent.Event = populateNil(baseEvent.Event)
	baseEvent.Event.Outcome = key.EventOutcome
	baseEvent.Event.SuccessCount = &eventSuccessCount

	if key.ContainerID != "" {
		baseEvent.Container = populateNil(baseEvent.Container)
		baseEvent.Container.Id = key.ContainerID
	}

	if key.KubernetesPodName != "" {
		baseEvent.Kubernetes = populateNil(baseEvent.Kubernetes)
		baseEvent.Kubernetes.PodName = key.KubernetesPodName
	}

	if key.ServiceVersion != "" {
		baseEvent.Service = populateNil(baseEvent.Service)
		baseEvent.Service.Version = key.ServiceVersion
	}

	if key.ServiceNodeName != "" {
		baseEvent.Service = populateNil(baseEvent.Service)
		baseEvent.Service.Node = populateNil(baseEvent.Service.Node)
		baseEvent.Service.Node.Name = key.ServiceNodeName
	}

	if key.ServiceRuntimeName != "" ||
		key.ServiceRuntimeVersion != "" {

		baseEvent.Service = populateNil(baseEvent.Service)
		baseEvent.Service.Runtime = populateNil(baseEvent.Service.Runtime)
		baseEvent.Service.Runtime.Name = key.ServiceRuntimeName
		baseEvent.Service.Runtime.Version = key.ServiceRuntimeVersion
	}

	if key.ServiceLanguageVersion != "" {
		baseEvent.Service = populateNil(baseEvent.Service)
		baseEvent.Service.Language = populateNil(baseEvent.Service.Language)
		baseEvent.Service.Language.Version = key.ServiceLanguageVersion
	}

	if key.HostHostname != "" ||
		key.HostName != "" {

		baseEvent.Host = populateNil(baseEvent.Host)
		baseEvent.Host.Hostname = key.HostHostname
		baseEvent.Host.Name = key.HostName
	}

	if key.HostOSPlatform != "" {
		baseEvent.Host = populateNil(baseEvent.Host)
		baseEvent.Host.Os = populateNil(baseEvent.Host.Os)
		baseEvent.Host.Os.Platform = key.HostOSPlatform
	}

	if key.FAASColdstart != nullable.Nil ||
		key.FAASID != "" ||
		key.FAASName != "" ||
		key.FAASVersion != "" ||
		key.FAASTriggerType != "" {

		baseEvent.Faas = populateNil(baseEvent.Faas)
		baseEvent.Faas.ColdStart = key.FAASColdstart.ToBoolPtr()
		baseEvent.Faas.Id = key.FAASID
		baseEvent.Faas.Name = key.FAASName
		baseEvent.Faas.Version = key.FAASVersion
		baseEvent.Faas.TriggerType = key.FAASTriggerType
	}

	if key.CloudProvider != "" ||
		key.CloudRegion != "" ||
		key.CloudAvailabilityZone != "" ||
		key.CloudServiceName != "" ||
		key.CloudAccountID != "" ||
		key.CloudAccountName != "" ||
		key.CloudMachineType != "" ||
		key.CloudProjectID != "" ||
		key.CloudProjectName != "" {

		baseEvent.Cloud = populateNil(baseEvent.Cloud)
		baseEvent.Cloud.Provider = key.CloudProvider
		baseEvent.Cloud.Region = key.CloudRegion
		baseEvent.Cloud.AvailabilityZone = key.CloudAvailabilityZone
		baseEvent.Cloud.ServiceName = key.CloudServiceName
		baseEvent.Cloud.AccountId = key.CloudAccountID
		baseEvent.Cloud.AccountName = key.CloudAccountName
		baseEvent.Cloud.MachineType = key.CloudMachineType
		baseEvent.Cloud.ProjectId = key.CloudProjectID
		baseEvent.Cloud.ProjectName = key.CloudProjectName
	}
}

func svcTxnMetricsToAPMEvent(
	key ServiceTransactionAggregationKey,
	metrics *aggregationpb.ServiceTransactionMetrics,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	histogram := hdrhistogram.New()
	HistogramFromProto(histogram, metrics.Histogram)
	totalCount, counts, values := histogram.Buckets()
	transactionDurationSummary := modelpb.SummaryMetric{
		Count: totalCount,
	}
	for i, v := range values {
		transactionDurationSummary.Sum += v * float64(counts[i])
	}

	baseEvent.Metricset = populateNil(baseEvent.Metricset)
	baseEvent.Metricset.Name = svcTxnMetricsetName
	baseEvent.Metricset.DocCount = totalCount
	baseEvent.Metricset.Interval = intervalStr

	baseEvent.Transaction = populateNil(baseEvent.Transaction)
	baseEvent.Transaction.Type = key.TransactionType
	baseEvent.Transaction.DurationSummary = &transactionDurationSummary
	baseEvent.Transaction.DurationHistogram = &modelpb.Histogram{
		Counts: counts,
		Values: values,
	}

	baseEvent.Event = populateNil(baseEvent.Event)
	baseEvent.Event.SuccessCount = &modelpb.SummaryMetric{
		Count: int64(math.Round(metrics.SuccessCount + metrics.FailureCount)),
		Sum:   math.Round(metrics.SuccessCount),
	}
}

func spanMetricsToAPMEvent(
	key SpanAggregationKey,
	metrics *aggregationpb.SpanMetrics,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	var target *modelpb.ServiceTarget
	if key.TargetName != "" || key.TargetType != "" {
		target = &modelpb.ServiceTarget{
			Type: key.TargetType,
			Name: key.TargetName,
		}
	}
	baseEvent.Service = populateNil(baseEvent.Service)
	baseEvent.Service.Target = target

	baseEvent.Metricset = populateNil(baseEvent.Metricset)
	baseEvent.Metricset.Name = spanMetricsetName
	baseEvent.Metricset.DocCount = int64(math.Round(metrics.Count))
	baseEvent.Metricset.Interval = intervalStr

	baseEvent.Span = populateNil(baseEvent.Span)
	baseEvent.Span.Name = key.SpanName

	baseEvent.Span.DestinationService = populateNil(baseEvent.Span.DestinationService)
	baseEvent.Span.DestinationService.Resource = key.Resource
	baseEvent.Span.DestinationService.ResponseTime = &modelpb.AggregatedDuration{
		Count: int64(math.Round(metrics.Count)),
		Sum:   durationpb.New(time.Duration(math.Round(metrics.Sum))),
	}

	if key.Outcome != "" {
		baseEvent.Event = populateNil(baseEvent.Event)
		baseEvent.Event.Outcome = key.Outcome
	}
}

func overflowServiceMetricsToAPMEvent(
	processingTime time.Time,
	overflowEstimator *hyperloglog.Sketch,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	// Overflow metrics use the processing time as their timestamp rather than
	// the event time. This makes sure that they can be associated with the
	// appropriate time when the event volume caused them to overflow.
	baseEvent.Timestamp = timestamppb.New(processingTime)
	overflowCount := overflowEstimator.Estimate()
	serviceMetricsToAPMEvent(baseEvent, intervalStr)

	samples := baseEvent.GetMetricset().GetSamples()
	samples = append(samples, &modelpb.MetricsetSample{
		Name:  "service_summary.aggregation.overflow_count",
		Value: float64(overflowCount),
	})
	if baseEvent.Metricset == nil {
		baseEvent.Metricset = &modelpb.Metricset{}
	}
	baseEvent.Metricset.Samples = samples
}

// overflowTxnMetricsToAPMEvent maps the fields of overflow
// transaction to the passed APMEvent. This only updates transcation
// metrics related fields and expects that service related fields
// are present in the passed APMEvent.
//
// For the doc count, unlike the span metrics which uses estimated
// overflow count, the transaction metrics uses the value derived
// from the histogram to avoid consistency issues between the
// overflow estimate and the histogram.
func overflowTxnMetricsToAPMEvent(
	processingTime time.Time,
	overflow OverflowTransaction,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	// Overflow metrics use the processing time as their timestamp rather than
	// the event time. This makes sure that they can be associated with the
	// appropriate time when the event volume caused them to overflow.
	baseEvent.Timestamp = timestamppb.New(processingTime)
	overflowCount := int64(overflow.Estimator.Estimate())
	overflowKey := TransactionAggregationKey{
		TransactionName: overflowBucketName,
	}
	txnMetricsToAPMEvent(overflowKey, overflow.Metrics, baseEvent, intervalStr)

	samples := baseEvent.GetMetricset().GetSamples()
	samples = append(samples, &modelpb.MetricsetSample{
		Name:  "transaction.aggregation.overflow_count",
		Value: float64(overflowCount),
	})
	if baseEvent.Metricset == nil {
		baseEvent.Metricset = &modelpb.Metricset{}
	}
	baseEvent.Metricset.Samples = samples
}

func overflowSvcTxnMetricsToAPMEvent(
	processingTime time.Time,
	overflow OverflowServiceTransaction,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	// Overflow metrics use the processing time as their timestamp rather than
	// the event time. This makes sure that they can be associated with the
	// appropriate time when the event volume caused them to overflow.
	baseEvent.Timestamp = timestamppb.New(processingTime)
	overflowCount := int64(overflow.Estimator.Estimate())
	overflowKey := ServiceTransactionAggregationKey{
		TransactionType: overflowBucketName,
	}
	svcTxnMetricsToAPMEvent(overflowKey, overflow.Metrics, baseEvent, intervalStr)

	samples := baseEvent.GetMetricset().GetSamples()
	samples = append(samples, &modelpb.MetricsetSample{
		Name:  "service_transaction.aggregation.overflow_count",
		Value: float64(overflowCount),
	})
	if baseEvent.Metricset == nil {
		baseEvent.Metricset = &modelpb.Metricset{}
	}
	baseEvent.Metricset.Samples = samples
}

func overflowSpanMetricsToAPMEvent(
	processingTime time.Time,
	overflow OverflowSpan,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	// Overflow metrics use the processing time as their timestamp rather than
	// the event time. This makes sure that they can be associated with the
	// appropriate time when the event volume caused them to overflow.
	baseEvent.Timestamp = timestamppb.New(processingTime)
	overflowCount := int64(overflow.Estimator.Estimate())
	overflowKey := SpanAggregationKey{
		TargetName: overflowBucketName,
	}
	spanMetricsToAPMEvent(overflowKey, overflow.Metrics, baseEvent, intervalStr)

	samples := baseEvent.GetMetricset().GetSamples()
	samples = append(samples, &modelpb.MetricsetSample{
		Name:  "service_destination.aggregation.overflow_count",
		Value: float64(overflowCount),
	})
	if baseEvent.Metricset == nil {
		baseEvent.Metricset = &modelpb.Metricset{}
	}
	baseEvent.Metricset.Samples = samples
	baseEvent.Metricset.DocCount = int64(overflowCount)
}

func serviceKey(e *modelpb.APMEvent, aggInterval time.Duration) *aggregationpb.ServiceAggregationKey {
	key := aggregationpb.ServiceAggregationKeyFromVTPool()
	key.Timestamp = tspb.TimeToPBTimestamp(
		e.GetTimestamp().AsTime().Truncate(aggInterval),
	)
	key.ServiceName = e.GetService().GetName()
	key.ServiceEnvironment = e.GetService().GetEnvironment()
	key.ServiceLanguageName = e.GetService().GetLanguage().GetName()
	key.AgentName = e.GetAgent().GetName()

	return key
}

func serviceInstanceKey(e *modelpb.APMEvent) (*aggregationpb.ServiceInstanceAggregationKey, error) {
	var gl GlobalLabels
	for k, v := range e.Labels {
		if !v.Global {
			continue
		}
		if (&gl).Labels == nil {
			(&gl).Labels = make(modelpb.Labels)
		}
		gl.Labels[k] = v
	}
	for k, v := range e.NumericLabels {
		if !v.Global {
			continue
		}
		if (&gl).NumericLabels == nil {
			(&gl).NumericLabels = make(modelpb.NumericLabels)
		}
		gl.NumericLabels[k] = v
	}
	glb, err := gl.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to parse global labels: %w", err)
	}

	key := aggregationpb.ServiceInstanceAggregationKeyFromVTPool()
	key.GlobalLabelsStr = glb

	return key, nil
}

func transactionKey(e *modelpb.APMEvent) *aggregationpb.TransactionAggregationKey {
	var faasColdstart nullable.Bool
	faas := e.GetFaas()
	if faas != nil {
		faasColdstart.ParseBoolPtr(faas.ColdStart)
	}

	key := aggregationpb.TransactionAggregationKeyFromVTPool()
	key.TraceRoot = e.GetParentId() == ""

	key.ContainerId = e.GetContainer().GetId()
	key.KubernetesPodName = e.GetKubernetes().GetPodName()

	key.ServiceVersion = e.GetService().GetVersion()
	key.ServiceNodeName = e.GetService().GetNode().GetName()

	key.ServiceRuntimeName = e.GetService().GetRuntime().GetName()
	key.ServiceRuntimeVersion = e.GetService().GetRuntime().GetVersion()
	key.ServiceLanguageVersion = e.GetService().GetLanguage().GetVersion()

	key.HostHostname = e.GetHost().GetHostname()
	key.HostName = e.GetHost().GetName()
	key.HostOsPlatform = e.GetHost().GetOs().GetPlatform()

	key.EventOutcome = e.GetEvent().GetOutcome()

	key.TransactionName = e.GetTransaction().GetName()
	key.TransactionType = e.GetTransaction().GetType()
	key.TransactionResult = e.GetTransaction().GetResult()

	key.FaasColdstart = uint32(faasColdstart)
	key.FaasId = faas.GetId()
	key.FaasName = faas.GetName()
	key.FaasVersion = faas.GetVersion()
	key.FaasTriggerType = faas.GetTriggerType()

	key.CloudProvider = e.GetCloud().GetProvider()
	key.CloudRegion = e.GetCloud().GetRegion()
	key.CloudAvailabilityZone = e.GetCloud().GetAvailabilityZone()
	key.CloudServiceName = e.GetCloud().GetServiceName()
	key.CloudAccountId = e.GetCloud().GetAccountId()
	key.CloudAccountName = e.GetCloud().GetAccountName()
	key.CloudMachineType = e.GetCloud().GetMachineType()
	key.CloudProjectId = e.GetCloud().GetProjectId()
	key.CloudProjectName = e.GetCloud().GetProjectName()

	return key
}

func serviceTransactionKey(e *modelpb.APMEvent) *aggregationpb.ServiceTransactionAggregationKey {
	key := aggregationpb.ServiceTransactionAggregationKeyFromVTPool()
	key.TransactionType = e.GetTransaction().GetType()

	return key
}

func spanKey(e *modelpb.APMEvent) *aggregationpb.SpanAggregationKey {
	var resource, targetType, targetName string
	target := e.GetService().GetTarget()
	if target != nil {
		targetType = target.GetType()
		targetName = target.GetName()
	}
	destSvc := e.GetSpan().GetDestinationService()
	if destSvc != nil {
		resource = destSvc.GetResource()
	}

	key := aggregationpb.SpanAggregationKeyFromVTPool()
	key.SpanName = e.GetSpan().GetName()
	key.Outcome = e.GetEvent().GetOutcome()

	key.TargetType = targetType
	key.TargetName = targetName

	key.Resource = resource

	return key
}

func droppedSpanStatsKey(dss *modelpb.DroppedSpanStats) *aggregationpb.SpanAggregationKey {
	key := aggregationpb.SpanAggregationKeyFromVTPool()
	// Dropped span statistics do not contain span name because it
	// would be too expensive to track dropped span stats per span name.
	key.SpanName = ""
	key.Outcome = dss.GetOutcome()

	key.TargetType = dss.GetServiceTargetType()
	key.TargetName = dss.GetServiceTargetName()

	key.Resource = dss.GetDestinationServiceResource()

	return key
}

func setMetricCountBasedOnOutcome(
	stm *aggregationpb.ServiceTransactionMetrics,
	from *modelpb.APMEvent,
) {
	txn := from.GetTransaction()
	switch from.GetEvent().GetOutcome() {
	case "failure":
		stm.FailureCount = txn.GetRepresentativeCount()
	case "success":
		stm.SuccessCount = txn.GetRepresentativeCount()
	}
}

func formatDuration(d time.Duration) string {
	if duration := d.Minutes(); duration >= 1 {
		return fmt.Sprintf("%.0fm", duration)
	}
	return fmt.Sprintf("%.0fs", d.Seconds())
}

func populateNil[T any](a *T) *T {
	if a == nil {
		return new(T)
	}
	return a
}

// combinedMetricsCollector collects and categorizes partitioned metrics into the
// alloted partitions. If more than one metrics are added mapping to the same partition
// then they are merged.
type combinedMetricsCollector map[uint16]*aggregationpb.ServiceInstanceMetrics

func (c combinedMetricsCollector) add(
	partitionID uint16,
	from *aggregationpb.ServiceInstanceMetrics,
) {
	to, ok := c[partitionID]
	if !ok {
		c[partitionID] = from
		return
	}
	to.ServiceTransactionMetrics = mergeSlices[aggregationpb.KeyedServiceTransactionMetrics](
		to.ServiceTransactionMetrics, from.ServiceTransactionMetrics,
	)
	to.TransactionMetrics = mergeSlices[aggregationpb.KeyedTransactionMetrics](
		to.TransactionMetrics, from.TransactionMetrics,
	)
	to.SpanMetrics = mergeSlices[aggregationpb.KeyedSpanMetrics](
		to.SpanMetrics, from.SpanMetrics,
	)
	// nil out the slices to avoid ReturnToVTPool from releasing the underlying metrics in the slices
	nilSlice(from.ServiceTransactionMetrics)
	nilSlice(from.TransactionMetrics)
	nilSlice(from.SpanMetrics)
	from.ReturnToVTPool()
}

func mergeSlices[T any](to []*T, from []*T) []*T {
	if len(from) == 0 {
		return to
	}
	to = slices.Grow(to, len(from))
	return append(to, from...)
}

func nilSlice[T any](s []*T) {
	for i := 0; i < len(s); i++ {
		s[i] = nil
	}
}
