// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"fmt"
	"math"
	"time"

	"github.com/axiomhq/hyperloglog"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/elastic/apm-data/model/modelpb"
)

const (
	spanMetricsetName    = "service_destination"
	txnMetricsetName     = "transaction"
	svcTxnMetricsetName  = "service_transaction"
	summaryMetricsetName = "service_summary"

	overflowBucketName = "_other"
)

func setMetricCountBasedOnOutcome(stm *ServiceTransactionMetrics, from *modelpb.APMEvent) {
	txn := from.GetTransaction()
	switch from.GetEvent().GetOutcome() {
	case "failure":
		stm.FailureCount = txn.GetRepresentativeCount()
	case "success":
		stm.SuccessCount = txn.GetRepresentativeCount()
	}
}

// EventToCombinedMetrics converts APMEvent to CombinedMetrics. Metrics are
// partitioned into smaller partitions based on the partitioning logic. It
// will ignore overflows as the partition will be executed on a single
// APMEvent with no possibility of overflows.
func EventToCombinedMetrics(
	e *modelpb.APMEvent,
	unpartitionedKey CombinedMetricsKey,
	partitioner Partitioner,
) ([]CombinedKV, error) {
	var (
		kvs []CombinedKV
		gl  GlobalLabels
	)
	gl.fromLabelsAndNumericLabels(e.Labels, e.NumericLabels)
	gls, err := gl.MarshalString()
	if err != nil {
		return nil, err
	}

	svcKey := serviceKey(e, unpartitionedKey.Interval)
	svcInstanceKey := ServiceInstanceAggregationKey{GlobalLabelsStr: gls}
	hasher := Hasher{}.Chain(svcKey).Chain(svcInstanceKey)

	processor := e.GetProcessor()
	switch {
	case processor.IsTransaction():
		repCount := e.GetTransaction().GetRepresentativeCount()
		if repCount <= 0 {
			return nil, nil
		}
		tm := newTransactionMetrics()
		tm.Histogram.RecordDuration(e.GetEvent().GetDuration().AsDuration(), repCount)
		txnKey := transactionKey(e)
		cmk := unpartitionedKey
		cmk.PartitionID = partitioner.Partition(hasher.Chain(txnKey).Sum())
		kvs = append(kvs, CombinedKV{
			Key: cmk,
			Value: CombinedMetrics{
				Services: map[ServiceAggregationKey]ServiceMetrics{
					svcKey: ServiceMetrics{
						ServiceInstanceGroups: map[ServiceInstanceAggregationKey]ServiceInstanceMetrics{
							svcInstanceKey: ServiceInstanceMetrics{
								TransactionGroups: map[TransactionAggregationKey]TransactionMetrics{txnKey: tm},
							},
						},
					},
				},
			},
		})

		stm := newServiceTransactionMetrics()
		stm.Histogram.RecordDuration(e.GetEvent().GetDuration().AsDuration(), repCount)
		setMetricCountBasedOnOutcome(&stm, e)
		svcTxnKey := serviceTransactionKey(e)
		cmk.PartitionID = partitioner.Partition(hasher.Chain(svcTxnKey).Sum())
		kvs = append(kvs, CombinedKV{
			Key: cmk,
			Value: CombinedMetrics{
				Services: map[ServiceAggregationKey]ServiceMetrics{
					svcKey: ServiceMetrics{
						ServiceInstanceGroups: map[ServiceInstanceAggregationKey]ServiceInstanceMetrics{
							svcInstanceKey: ServiceInstanceMetrics{
								ServiceTransactionGroups: map[ServiceTransactionAggregationKey]ServiceTransactionMetrics{svcTxnKey: stm},
							},
						},
					},
				},
			},
		})

		// Handle dropped span stats
		for _, dss := range e.GetTransaction().GetDroppedSpansStats() {
			dssKey := droppedSpanStatsKey(dss)
			cmk.PartitionID = partitioner.Partition(hasher.Chain(dssKey).Sum())
			kvs = append(kvs, CombinedKV{
				Key: cmk,
				Value: CombinedMetrics{
					Services: map[ServiceAggregationKey]ServiceMetrics{
						svcKey: ServiceMetrics{
							ServiceInstanceGroups: map[ServiceInstanceAggregationKey]ServiceInstanceMetrics{
								svcInstanceKey: ServiceInstanceMetrics{
									SpanGroups: map[SpanAggregationKey]SpanMetrics{
										dssKey: SpanMetrics{
											Count: float64(dss.GetDuration().GetCount()) * repCount,
											Sum:   float64(dss.GetDuration().GetSum().AsDuration()) * repCount,
										},
									},
								},
							},
						},
					},
				},
			})
		}
	case processor.IsSpan():
		target := e.GetService().GetTarget()
		repCount := e.GetSpan().GetRepresentativeCount()
		destSvc := e.GetSpan().GetDestinationService().GetResource()
		if repCount <= 0 || (target == nil && destSvc == "") {
			return nil, nil
		}

		var count uint32 = 1
		duration := e.GetEvent().GetDuration().AsDuration()
		if composite := e.GetSpan().GetComposite(); composite != nil {
			count = composite.GetCount()
			duration = time.Duration(composite.GetSum() * float64(time.Millisecond))
		}
		spanKey := spanKey(e)
		cmk := unpartitionedKey
		cmk.PartitionID = partitioner.Partition(hasher.Chain(spanKey).Sum())
		kvs = append(kvs, CombinedKV{
			Key: cmk,
			Value: CombinedMetrics{
				Services: map[ServiceAggregationKey]ServiceMetrics{
					svcKey: ServiceMetrics{
						ServiceInstanceGroups: map[ServiceInstanceAggregationKey]ServiceInstanceMetrics{
							svcInstanceKey: ServiceInstanceMetrics{
								SpanGroups: map[SpanAggregationKey]SpanMetrics{
									spanKey: SpanMetrics{
										Count: float64(count) * repCount,
										Sum:   float64(duration) * repCount,
									},
								},
							},
						},
					},
				},
			},
		})
	}

	// Approximate events total by uniformly distributing the events total
	// amongst the partitioned key values.
	weightedEventsTotal := 1 / float64(len(kvs))
	eventTs := e.GetEvent().GetReceived().AsTime()
	for i := range kvs {
		cv := &kvs[i].Value
		cv.eventsTotal = weightedEventsTotal
		cv.youngestEventTimestamp = eventTs
	}
	return kvs, nil
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
			for tk, tv := range sim.TransactionGroups {
				event := getBaseEventWithLabels()
				txnMetricsToAPMEvent(tk, tv, event, aggIntervalStr)
				b = append(b, event)
			}
			// service transaction metrics
			for stk, stv := range sim.ServiceTransactionGroups {
				event := getBaseEventWithLabels()
				svcTxnMetricsToAPMEvent(stk, stv, event, aggIntervalStr)
				b = append(b, event)
			}
			// service destination metrics
			for spk, spv := range sim.SpanGroups {
				event := getBaseEventWithLabels()
				spanMetricsToAPMEvent(spk, spv, event, aggIntervalStr)
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
				Processor: modelpb.MetricsetProcessor(),
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

func getBaseEvent(key ServiceAggregationKey) *modelpb.APMEvent {
	event := &modelpb.APMEvent{
		Timestamp: timestamppb.New(key.Timestamp),
		Processor: modelpb.MetricsetProcessor(),
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
	metrics TransactionMetrics,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	totalCount, counts, values := metrics.Histogram.Buckets()
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

	if key.FAASColdstart != Nil ||
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
	metrics ServiceTransactionMetrics,
	baseEvent *modelpb.APMEvent,
	intervalStr string,
) {
	totalCount, counts, values := metrics.Histogram.Buckets()

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
	metrics SpanMetrics,
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

func serviceKey(e *modelpb.APMEvent, aggInterval time.Duration) ServiceAggregationKey {
	return ServiceAggregationKey{
		Timestamp:           e.GetTimestamp().AsTime().Truncate(aggInterval),
		ServiceName:         e.GetService().GetName(),
		ServiceEnvironment:  e.GetService().GetEnvironment(),
		ServiceLanguageName: e.GetService().GetLanguage().GetName(),
		AgentName:           e.GetAgent().GetName(),
	}
}

func transactionKey(e *modelpb.APMEvent) TransactionAggregationKey {
	var faasColdstart NullableBool
	faas := e.GetFaas()
	if faas != nil {
		faasColdstart.ParseBoolPtr(faas.ColdStart)
	}
	return TransactionAggregationKey{
		TraceRoot: e.GetParentId() == "",

		ContainerID:       e.GetContainer().GetId(),
		KubernetesPodName: e.GetKubernetes().GetPodName(),

		ServiceVersion:  e.GetService().GetVersion(),
		ServiceNodeName: e.GetService().GetNode().GetName(),

		ServiceRuntimeName:     e.GetService().GetRuntime().GetName(),
		ServiceRuntimeVersion:  e.GetService().GetRuntime().GetVersion(),
		ServiceLanguageVersion: e.GetService().GetLanguage().GetVersion(),

		HostHostname:   e.GetHost().GetHostname(),
		HostName:       e.GetHost().GetName(),
		HostOSPlatform: e.GetHost().GetOs().GetPlatform(),

		EventOutcome: e.GetEvent().GetOutcome(),

		TransactionName:   e.GetTransaction().GetName(),
		TransactionType:   e.GetTransaction().GetType(),
		TransactionResult: e.GetTransaction().GetResult(),

		FAASColdstart:   faasColdstart,
		FAASID:          faas.GetId(),
		FAASName:        faas.GetName(),
		FAASVersion:     faas.GetVersion(),
		FAASTriggerType: faas.GetTriggerType(),

		CloudProvider:         e.GetCloud().GetProvider(),
		CloudRegion:           e.GetCloud().GetRegion(),
		CloudAvailabilityZone: e.GetCloud().GetAvailabilityZone(),
		CloudServiceName:      e.GetCloud().GetServiceName(),
		CloudAccountID:        e.GetCloud().GetAccountId(),
		CloudAccountName:      e.GetCloud().GetAccountName(),
		CloudMachineType:      e.GetCloud().GetMachineType(),
		CloudProjectID:        e.GetCloud().GetProjectId(),
		CloudProjectName:      e.GetCloud().GetProjectName(),
	}
}

func serviceTransactionKey(e *modelpb.APMEvent) ServiceTransactionAggregationKey {
	return ServiceTransactionAggregationKey{
		TransactionType: e.GetTransaction().GetType(),
	}
}

func spanKey(e *modelpb.APMEvent) SpanAggregationKey {
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
	return SpanAggregationKey{
		SpanName: e.GetSpan().GetName(),
		Outcome:  e.GetEvent().GetOutcome(),

		TargetType: targetType,
		TargetName: targetName,

		Resource: resource,
	}
}

func droppedSpanStatsKey(dss *modelpb.DroppedSpanStats) SpanAggregationKey {
	return SpanAggregationKey{
		// Dropped span statistics do not contain span name because it
		// would be too expensive to track dropped span stats per span name.
		SpanName: "",
		Outcome:  dss.GetOutcome(),

		TargetType: dss.GetServiceTargetType(),
		TargetName: dss.GetServiceTargetName(),

		Resource: dss.GetDestinationServiceResource(),
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
