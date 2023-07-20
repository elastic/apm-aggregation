// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

// TODO(lahsivjar): Add a test using reflect to validate if all
// fields are properly set.

import (
	"encoding/binary"
	"errors"
	"sort"
	"time"

	"github.com/axiomhq/hyperloglog"

	"github.com/elastic/apm-aggregation/aggregationpb"
	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-aggregation/aggregators/internal/timestamppb"
	"github.com/elastic/apm-aggregation/aggregators/nullable"
	"github.com/elastic/apm-data/model/modelpb"
)

// MarshalBinaryToSizedBuffer will marshal the combined metrics key into
// its binary representation. The encoded byte slice will be used as a
// key in pebbledb. To ensure efficient sorting and time range based
// query, the first 2 bytes of the encoded slice is the aggregation
// interval, the next 8 bytes of the encoded slice is the processing time
// followed by combined metrics ID, the last 2 bytes is the partition ID.
// The binary representation ensures that all entries are ordered by the
// ID first and then ordered by the partition ID.
func (k *CombinedMetricsKey) MarshalBinaryToSizedBuffer(data []byte) error {
	ivlSeconds := uint16(k.Interval.Seconds())
	if len(data) != k.SizeBinary() {
		return errors.New("failed to marshal due to incorrect sized buffer")
	}
	var offset int

	binary.BigEndian.PutUint16(data[offset:], ivlSeconds)
	offset += 2

	binary.BigEndian.PutUint64(data[offset:], uint64(k.ProcessingTime.Unix()))
	offset += 8

	copy(data[offset:], k.ID[:])
	offset += 16

	binary.BigEndian.PutUint16(data[offset:], k.PartitionID)
	return nil
}

// UnmarshalBinary will convert the byte encoded data into CombinedMetricsKey.
func (k *CombinedMetricsKey) UnmarshalBinary(data []byte) error {
	if len(data) < 12 {
		return errors.New("invalid encoded data of insufficient length")
	}
	var offset int
	k.Interval = time.Duration(binary.BigEndian.Uint16(data[offset:2])) * time.Second
	offset += 2

	k.ProcessingTime = time.Unix(int64(binary.BigEndian.Uint64(data[offset:offset+8])), 0)
	offset += 8

	copy(k.ID[:], data[offset:offset+len(k.ID)])
	offset += len(k.ID)

	k.PartitionID = binary.BigEndian.Uint16(data[offset:])
	return nil
}

// SizeBinary returns the size of the byte array required to encode
// combined metrics key.
func (k *CombinedMetricsKey) SizeBinary() int {
	// 2 bytes for interval encoding
	// 8 bytes for timestamp encoding
	// 16 bytes for ID encoding
	// 2 bytes for partition ID
	return 2 + 8 + 16 + 2
}

// ToProto converts CombinedMetrics to its protobuf representation.
func (m *CombinedMetrics) ToProto() *aggregationpb.CombinedMetrics {
	pb := aggregationpb.CombinedMetricsFromVTPool()
	if len(m.Services) > cap(pb.ServiceMetrics) {
		pb.ServiceMetrics = make([]*aggregationpb.KeyedServiceMetrics, 0, len(m.Services))
	}
	for k, m := range m.Services {
		ksm := aggregationpb.KeyedServiceMetricsFromVTPool()
		ksm.Key = k.ToProto()
		ksm.Metrics = m.ToProto()
		pb.ServiceMetrics = append(pb.ServiceMetrics, ksm)
	}
	if pb.OverflowServiceInstancesEstimator != nil {
		pb.OverflowServices = m.OverflowServices.ToProto()
		pb.OverflowServiceInstancesEstimator = hllBytes(m.OverflowServiceInstancesEstimator)
	}
	pb.EventsTotal = m.eventsTotal
	pb.YoungestEventTimestamp = timestamppb.TimeToPBTimestamp(m.youngestEventTimestamp)
	return pb
}

// FromProto converts protobuf representation to CombinedMetrics.
func (m *CombinedMetrics) FromProto(pb *aggregationpb.CombinedMetrics) {
	m.Services = make(map[ServiceAggregationKey]ServiceMetrics, len(pb.ServiceMetrics))
	for _, ksm := range pb.ServiceMetrics {
		var k ServiceAggregationKey
		var v ServiceMetrics
		k.FromProto(ksm.Key)
		v.FromProto(ksm.Metrics)
		m.Services[k] = v
	}
	if pb.OverflowServices != nil {
		m.OverflowServices.FromProto(pb.OverflowServices)
		m.OverflowServiceInstancesEstimator = hllSketch(pb.OverflowServiceInstancesEstimator)
	}
	m.eventsTotal = pb.EventsTotal
	m.youngestEventTimestamp = timestamppb.PBTimestampToTime(pb.YoungestEventTimestamp)
}

// MarshalBinary marshals CombinedMetrics to binary using protobuf.
func (m *CombinedMetrics) MarshalBinary() ([]byte, error) {
	pb := m.ToProto()
	defer pb.ReturnToVTPool()
	return pb.MarshalVT()
}

// UnmarshalBinary unmarshals binary protobuf to CombinedMetrics.
func (m *CombinedMetrics) UnmarshalBinary(data []byte) error {
	pb := aggregationpb.CombinedMetricsFromVTPool()
	defer pb.ReturnToVTPool()
	if err := pb.UnmarshalVT(data); err != nil {
		return err
	}
	m.FromProto(pb)
	return nil
}

// ToProto converts ServiceAggregationKey to its protobuf representation.
func (k *ServiceAggregationKey) ToProto() *aggregationpb.ServiceAggregationKey {
	pb := aggregationpb.ServiceAggregationKeyFromVTPool()
	pb.Timestamp = timestamppb.TimeToPBTimestamp(k.Timestamp)
	pb.ServiceName = k.ServiceName
	pb.ServiceEnvironment = k.ServiceEnvironment
	pb.ServiceLanguageName = k.ServiceLanguageName
	pb.AgentName = k.AgentName
	return pb
}

// FromProto converts protobuf representation to ServiceAggregationKey.
func (k *ServiceAggregationKey) FromProto(pb *aggregationpb.ServiceAggregationKey) {
	k.Timestamp = timestamppb.PBTimestampToTime(pb.Timestamp)
	k.ServiceName = pb.ServiceName
	k.ServiceEnvironment = pb.ServiceEnvironment
	k.ServiceLanguageName = pb.ServiceLanguageName
	k.AgentName = pb.AgentName
}

// ToProto converts ServiceMetrics to its protobuf representation.
func (m *ServiceMetrics) ToProto() *aggregationpb.ServiceMetrics {
	pb := aggregationpb.ServiceMetricsFromVTPool()
	if len(m.ServiceInstanceGroups) > cap(pb.ServiceInstanceMetrics) {
		pb.ServiceInstanceMetrics = make([]*aggregationpb.KeyedServiceInstanceMetrics, 0, len(m.ServiceInstanceGroups))
	}
	for k, m := range m.ServiceInstanceGroups {
		ksim := aggregationpb.KeyedServiceInstanceMetricsFromVTPool()
		ksim.Key = k.ToProto()
		ksim.Metrics = m.ToProto()
		pb.ServiceInstanceMetrics = append(pb.ServiceInstanceMetrics, ksim)
	}
	pb.OverflowGroups = m.OverflowGroups.ToProto()
	return pb
}

// FromProto converts protobuf representation to ServiceMetrics.
func (m *ServiceMetrics) FromProto(pb *aggregationpb.ServiceMetrics) {
	m.ServiceInstanceGroups = make(map[ServiceInstanceAggregationKey]ServiceInstanceMetrics, len(pb.ServiceInstanceMetrics))
	for _, ksim := range pb.ServiceInstanceMetrics {
		var k ServiceInstanceAggregationKey
		var v ServiceInstanceMetrics
		k.FromProto(ksim.Key)
		v.FromProto(ksim.Metrics)
		m.ServiceInstanceGroups[k] = v
	}
	if pb.OverflowGroups != nil {
		m.OverflowGroups.FromProto(pb.OverflowGroups)
	}
}

// ToProto converts ServiceInstanceAggregationKey to its protobuf representation.
func (k *ServiceInstanceAggregationKey) ToProto() *aggregationpb.ServiceInstanceAggregationKey {
	pb := aggregationpb.ServiceInstanceAggregationKeyFromVTPool()
	pb.GlobalLabelsStr = []byte(k.GlobalLabelsStr)
	return pb
}

// FromProto converts protobuf representation to ServiceInstanceAggregationKey.
func (k *ServiceInstanceAggregationKey) FromProto(pb *aggregationpb.ServiceInstanceAggregationKey) {
	k.GlobalLabelsStr = string(pb.GlobalLabelsStr)
}

// ToProto converts ServiceInstanceMetrics to its protobuf representation.
func (m *ServiceInstanceMetrics) ToProto() *aggregationpb.ServiceInstanceMetrics {
	pb := aggregationpb.ServiceInstanceMetricsFromVTPool()
	if len(m.TransactionGroups) > cap(pb.TransactionMetrics) {
		pb.TransactionMetrics = make([]*aggregationpb.KeyedTransactionMetrics, 0, len(m.TransactionGroups))
	}
	for k, m := range m.TransactionGroups {
		ktm := aggregationpb.KeyedTransactionMetricsFromVTPool()
		ktm.Key = k.ToProto()
		ktm.Metrics = m.ToProto()
		pb.TransactionMetrics = append(pb.TransactionMetrics, ktm)
	}
	if len(m.ServiceTransactionGroups) > cap(pb.ServiceTransactionMetrics) {
		pb.ServiceTransactionMetrics = make([]*aggregationpb.KeyedServiceTransactionMetrics, 0, len(m.ServiceTransactionGroups))
	}
	for k, m := range m.ServiceTransactionGroups {
		kstm := aggregationpb.KeyedServiceTransactionMetricsFromVTPool()
		kstm.Key = k.ToProto()
		kstm.Metrics = m.ToProto()
		pb.ServiceTransactionMetrics = append(pb.ServiceTransactionMetrics, kstm)
	}
	if len(m.SpanGroups) > cap(pb.SpanMetrics) {
		pb.SpanMetrics = make([]*aggregationpb.KeyedSpanMetrics, 0, len(m.SpanGroups))
	}
	for k, m := range m.SpanGroups {
		ksm := aggregationpb.KeyedSpanMetricsFromVTPool()
		ksm.Key = k.ToProto()
		ksm.Metrics = m.ToProto()
		pb.SpanMetrics = append(pb.SpanMetrics, ksm)
	}
	return pb
}

// FromProto converts protobuf representation to ServiceInstanceMetrics.
func (m *ServiceInstanceMetrics) FromProto(pb *aggregationpb.ServiceInstanceMetrics) {
	m.TransactionGroups = make(map[TransactionAggregationKey]TransactionMetrics, len(pb.TransactionMetrics))
	for _, ktm := range pb.TransactionMetrics {
		var k TransactionAggregationKey
		var v TransactionMetrics
		k.FromProto(ktm.Key)
		v.FromProto(ktm.Metrics)
		m.TransactionGroups[k] = v
	}
	m.ServiceTransactionGroups = make(map[ServiceTransactionAggregationKey]ServiceTransactionMetrics,
		len(pb.ServiceTransactionMetrics))
	for _, kstm := range pb.ServiceTransactionMetrics {
		var k ServiceTransactionAggregationKey
		var v ServiceTransactionMetrics
		k.FromProto(kstm.Key)
		v.FromProto(kstm.Metrics)
		m.ServiceTransactionGroups[k] = v
	}
	m.SpanGroups = make(map[SpanAggregationKey]SpanMetrics, len(pb.SpanMetrics))
	for _, ksm := range pb.SpanMetrics {
		var k SpanAggregationKey
		var v SpanMetrics
		k.FromProto(ksm.Key)
		v.FromProto(ksm.Metrics)
		m.SpanGroups[k] = v
	}
}

// ToProto converts TransactionAggregationKey to its protobuf representation.
func (k *TransactionAggregationKey) ToProto() *aggregationpb.TransactionAggregationKey {
	pb := aggregationpb.TransactionAggregationKeyFromVTPool()
	pb.TraceRoot = k.TraceRoot

	pb.ContainerId = k.ContainerID
	pb.KubernetesPodName = k.KubernetesPodName

	pb.ServiceVersion = k.ServiceVersion
	pb.ServiceNodeName = k.ServiceNodeName

	pb.ServiceRuntimeName = k.ServiceRuntimeName
	pb.ServiceRuntimeVersion = k.ServiceRuntimeVersion
	pb.ServiceLanguageVersion = k.ServiceLanguageVersion

	pb.HostHostname = k.HostHostname
	pb.HostName = k.HostName
	pb.HostOsPlatform = k.HostOSPlatform

	pb.EventOutcome = k.EventOutcome

	pb.TransactionName = k.TransactionName
	pb.TransactionType = k.TransactionType
	pb.TransactionResult = k.TransactionResult

	pb.FaasColdstart = uint32(k.FAASColdstart)
	pb.FaasId = k.FAASID
	pb.FaasName = k.FAASName
	pb.FaasVersion = k.FAASVersion
	pb.FaasTriggerType = k.FAASTriggerType

	pb.CloudProvider = k.CloudProvider
	pb.CloudRegion = k.CloudRegion
	pb.CloudAvailabilityZone = k.CloudAvailabilityZone
	pb.CloudServiceName = k.CloudServiceName
	pb.CloudAccountId = k.CloudAccountID
	pb.CloudAccountName = k.CloudAccountName
	pb.CloudMachineType = k.CloudMachineType
	pb.CloudProjectId = k.CloudProjectID
	pb.CloudProjectName = k.CloudProjectName
	return pb
}

// FromProto converts protobuf representation to TransactionAggregationKey.
func (k *TransactionAggregationKey) FromProto(pb *aggregationpb.TransactionAggregationKey) {
	k.TraceRoot = pb.TraceRoot

	k.ContainerID = pb.ContainerId
	k.KubernetesPodName = pb.KubernetesPodName

	k.ServiceVersion = pb.ServiceVersion
	k.ServiceNodeName = pb.ServiceNodeName

	k.ServiceRuntimeName = pb.ServiceRuntimeName
	k.ServiceRuntimeVersion = pb.ServiceRuntimeVersion
	k.ServiceLanguageVersion = pb.ServiceLanguageVersion

	k.HostHostname = pb.HostHostname
	k.HostName = pb.HostName
	k.HostOSPlatform = pb.HostOsPlatform

	k.EventOutcome = pb.EventOutcome

	k.TransactionName = pb.TransactionName
	k.TransactionType = pb.TransactionType
	k.TransactionResult = pb.TransactionResult

	k.FAASColdstart = nullable.NullableBool(pb.FaasColdstart)
	k.FAASID = pb.FaasId
	k.FAASName = pb.FaasName
	k.FAASVersion = pb.FaasVersion
	k.FAASTriggerType = pb.FaasTriggerType

	k.CloudProvider = pb.CloudProvider
	k.CloudRegion = pb.CloudRegion
	k.CloudAvailabilityZone = pb.CloudAvailabilityZone
	k.CloudServiceName = pb.CloudServiceName
	k.CloudAccountID = pb.CloudAccountId
	k.CloudAccountName = pb.CloudAccountName
	k.CloudMachineType = pb.CloudMachineType
	k.CloudProjectID = pb.CloudProjectId
	k.CloudProjectName = pb.CloudProjectName
}

// ToProto converts the TransactionMetrics to its protobuf representation.
func (m *TransactionMetrics) ToProto() *aggregationpb.TransactionMetrics {
	pb := aggregationpb.TransactionMetricsFromVTPool()
	pb.Histogram = HistogramToProto(m.Histogram)
	return pb
}

// FromProto converts protobuf representation to TransactionMetrics.
func (m *TransactionMetrics) FromProto(pb *aggregationpb.TransactionMetrics) {
	if m.Histogram == nil && pb.Histogram != nil {
		m.Histogram = hdrhistogram.New()
	}
	HistogramFromProto(m.Histogram, pb.Histogram)
}

// ToProto converts ServiceTransactionAggregationKey to its protobuf representation.
func (k *ServiceTransactionAggregationKey) ToProto() *aggregationpb.ServiceTransactionAggregationKey {
	pb := aggregationpb.ServiceTransactionAggregationKeyFromVTPool()
	pb.TransactionType = k.TransactionType
	return pb
}

// FromProto converts protobuf representation to ServiceTransactionAggregationKey.
func (k *ServiceTransactionAggregationKey) FromProto(pb *aggregationpb.ServiceTransactionAggregationKey) {
	k.TransactionType = pb.TransactionType
}

// ToProto converts the ServiceTransactionMetrics to its protobuf representation.
func (m *ServiceTransactionMetrics) ToProto() *aggregationpb.ServiceTransactionMetrics {
	pb := aggregationpb.ServiceTransactionMetricsFromVTPool()
	pb.Histogram = HistogramToProto(m.Histogram)
	pb.FailureCount = m.FailureCount
	pb.SuccessCount = m.SuccessCount
	return pb
}

// FromProto converts protobuf representation to ServiceTransactionMetrics.
func (m *ServiceTransactionMetrics) FromProto(pb *aggregationpb.ServiceTransactionMetrics) {
	m.FailureCount = pb.FailureCount
	m.SuccessCount = pb.SuccessCount
	if m.Histogram == nil && pb.Histogram != nil {
		m.Histogram = hdrhistogram.New()
	}
	HistogramFromProto(m.Histogram, pb.Histogram)
}

// HistogramToProto converts the histogram representation to protobuf.
func HistogramToProto(h *hdrhistogram.HistogramRepresentation) *aggregationpb.HDRHistogram {
	if h == nil {
		return nil
	}
	pb := aggregationpb.HDRHistogramFromVTPool()
	pb.LowestTrackableValue = h.LowestTrackableValue
	pb.HighestTrackableValue = h.HighestTrackableValue
	pb.SignificantFigures = h.SignificantFigures
	countsLen := h.CountsRep.Len()
	if countsLen > cap(pb.Buckets) {
		pb.Buckets = make([]int32, 0, countsLen)
	}
	if countsLen > cap(pb.Counts) {
		pb.Counts = make([]int64, 0, countsLen)
	}
	h.CountsRep.ForEach(func(bucket int32, value int64) {
		pb.Buckets = append(pb.Buckets, bucket)
		pb.Counts = append(pb.Counts, value)
	})
	return pb
}

// HistogramFromProto converts protobuf to histogram representation.
func HistogramFromProto(h *hdrhistogram.HistogramRepresentation, pb *aggregationpb.HDRHistogram) {
	if pb == nil {
		return
	}
	h.LowestTrackableValue = pb.LowestTrackableValue
	h.HighestTrackableValue = pb.HighestTrackableValue
	h.SignificantFigures = pb.SignificantFigures
	h.CountsRep.Reset()

	for i := 0; i < len(pb.Buckets); i++ {
		bucket := pb.Buckets[i]
		counts := pb.Counts[i]
		h.CountsRep.Add(bucket, counts)
	}
}

// ToProto converts SpanAggregationKey to its protobuf representation.
func (k *SpanAggregationKey) ToProto() *aggregationpb.SpanAggregationKey {
	pb := aggregationpb.SpanAggregationKeyFromVTPool()
	pb.SpanName = k.SpanName
	pb.Outcome = k.Outcome

	pb.TargetType = k.TargetType
	pb.TargetName = k.TargetName

	pb.Resource = k.Resource
	return pb
}

// FromProto converts protobuf representation to SpanAggregationKey.
func (k *SpanAggregationKey) FromProto(pb *aggregationpb.SpanAggregationKey) {
	k.SpanName = pb.SpanName
	k.Outcome = pb.Outcome

	k.TargetType = pb.TargetType
	k.TargetName = pb.TargetName

	k.Resource = pb.Resource
}

// ToProto converts the SpanMetrics to its protobuf representation.
func (m *SpanMetrics) ToProto() *aggregationpb.SpanMetrics {
	pb := aggregationpb.SpanMetricsFromVTPool()
	pb.Count = m.Count
	pb.Sum = m.Sum
	return pb
}

// FromProto converts protobuf representation to SpanMetrics.
func (m *SpanMetrics) FromProto(pb *aggregationpb.SpanMetrics) {
	m.Count = pb.Count
	m.Sum = pb.Sum
}

// ToProto converts Overflow to its protobuf representation.
func (o *Overflow) ToProto() *aggregationpb.Overflow {
	pb := aggregationpb.OverflowFromVTPool()
	if !o.OverflowTransaction.Empty() {
		pb.OverflowTransactions = o.OverflowTransaction.Metrics.ToProto()
		pb.OverflowTransactionsEstimator = hllBytes(o.OverflowTransaction.Estimator)
	}
	if !o.OverflowServiceTransaction.Empty() {
		pb.OverflowServiceTransactions = o.OverflowServiceTransaction.Metrics.ToProto()
		pb.OverflowServiceTransactionsEstimator = hllBytes(o.OverflowServiceTransaction.Estimator)
	}
	if !o.OverflowSpan.Empty() {
		pb.OverflowSpans = o.OverflowSpan.Metrics.ToProto()
		pb.OverflowSpansEstimator = hllBytes(o.OverflowSpan.Estimator)
	}
	return pb
}

// FromProto converts protobuf representation to Overflow.
func (o *Overflow) FromProto(pb *aggregationpb.Overflow) {
	if pb.OverflowTransactions != nil {
		o.OverflowTransaction.Metrics.FromProto(pb.OverflowTransactions)
		o.OverflowTransaction.Estimator = hllSketch(pb.OverflowTransactionsEstimator)
	}
	if pb.OverflowServiceTransactions != nil {
		o.OverflowServiceTransaction.Metrics.FromProto(pb.OverflowServiceTransactions)
		o.OverflowServiceTransaction.Estimator = hllSketch(pb.OverflowServiceTransactionsEstimator)
	}
	if pb.OverflowSpans != nil {
		o.OverflowSpan.Metrics.FromProto(pb.OverflowSpans)
		o.OverflowSpan.Estimator = hllSketch(pb.OverflowSpansEstimator)
	}
}

// ToProto converts GlobalLabels to its protobuf representation.
func (gl *GlobalLabels) ToProto() *aggregationpb.GlobalLabels {
	pb := aggregationpb.GlobalLabelsFromVTPool()

	// Keys must be sorted to ensure wire formats are deterministically generated and strings are directly comparable
	// i.e. Protobuf formats are equal if and only if the structs are equal
	if len(gl.Labels) > cap(pb.Labels) {
		pb.Labels = make([]*aggregationpb.Label, 0, len(gl.Labels))
	}
	for k, v := range gl.Labels {
		l := aggregationpb.LabelFromVTPool()
		l.Key = k
		l.Value = v.Value
		l.Values = v.Values

		pb.Labels = append(pb.Labels, l)
	}
	sort.Slice(pb.Labels, func(i, j int) bool {
		return pb.Labels[i].Key < pb.Labels[j].Key
	})

	if len(gl.NumericLabels) > cap(pb.NumericLabels) {
		pb.NumericLabels = make([]*aggregationpb.NumericLabel, 0, len(gl.NumericLabels))
	}
	for k, v := range gl.NumericLabels {
		l := aggregationpb.NumericLabelFromVTPool()
		l.Key = k
		l.Value = v.Value
		l.Values = v.Values

		pb.NumericLabels = append(pb.NumericLabels, l)
	}
	sort.Slice(pb.NumericLabels, func(i, j int) bool {
		return pb.NumericLabels[i].Key < pb.NumericLabels[j].Key
	})

	return pb
}

// FromProto converts protobuf representation to GlobalLabels.
func (gl *GlobalLabels) FromProto(pb *aggregationpb.GlobalLabels) {
	gl.Labels = make(modelpb.Labels, len(pb.Labels))
	for _, l := range pb.Labels {
		gl.Labels[l.Key] = &modelpb.LabelValue{Value: l.Value, Values: l.Values, Global: true}
	}
	gl.NumericLabels = make(modelpb.NumericLabels, len(pb.NumericLabels))
	for _, l := range pb.NumericLabels {
		gl.NumericLabels[l.Key] = &modelpb.NumericLabelValue{Value: l.Value, Values: l.Values, Global: true}
	}
}

// MarshalBinary marshals GlobalLabels to binary using protobuf.
func (gl *GlobalLabels) MarshalBinary() ([]byte, error) {
	if gl.Labels == nil && gl.NumericLabels == nil {
		return nil, nil
	}
	pb := gl.ToProto()
	defer pb.ReturnToVTPool()
	return pb.MarshalVT()
}

// MarshalString marshals GlobalLabels to string from binary using protobuf.
func (gl *GlobalLabels) MarshalString() (string, error) {
	b, err := gl.MarshalBinary()
	return string(b), err
}

// UnmarshalBinary unmarshals binary protobuf to GlobalLabels.
func (gl *GlobalLabels) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		gl.Labels = nil
		gl.NumericLabels = nil
		return nil
	}
	pb := aggregationpb.GlobalLabelsFromVTPool()
	defer pb.ReturnToVTPool()
	if err := pb.UnmarshalVT(data); err != nil {
		return err
	}
	gl.FromProto(pb)
	return nil
}

// UnmarshalString unmarshals string of binary protobuf to GlobalLabels.
func (gl *GlobalLabels) UnmarshalString(data string) error {
	return gl.UnmarshalBinary([]byte(data))
}

func hllBytes(estimator *hyperloglog.Sketch) []byte {
	if estimator == nil {
		return nil
	}
	// Ignoring error here since error will always be nil
	b, _ := estimator.MarshalBinary()
	return b
}

func hllSketch(estimator []byte) *hyperloglog.Sketch {
	if len(estimator) == 0 {
		return nil
	}
	var sketch hyperloglog.Sketch
	// Ignoring returned error here since the error is only returned if
	// the precision is set outside bounds which is not possible for our case.
	sketch.UnmarshalBinary(estimator)
	return &sketch
}
