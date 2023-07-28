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
func (m *combinedMetrics) ToProto() *aggregationpb.CombinedMetrics {
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
	if m.OverflowServiceInstancesEstimator != nil {
		pb.OverflowServices = m.OverflowServices.ToProto()
		pb.OverflowServiceInstancesEstimator = hllBytes(m.OverflowServiceInstancesEstimator)
	}
	pb.EventsTotal = m.EventsTotal
	pb.YoungestEventTimestamp = m.YoungestEventTimestamp
	return pb
}

// ToProto converts ServiceAggregationKey to its protobuf representation.
func (k *serviceAggregationKey) ToProto() *aggregationpb.ServiceAggregationKey {
	pb := aggregationpb.ServiceAggregationKeyFromVTPool()
	pb.Timestamp = timestamppb.TimeToPBTimestamp(k.Timestamp)
	pb.ServiceName = k.ServiceName
	pb.ServiceEnvironment = k.ServiceEnvironment
	pb.ServiceLanguageName = k.ServiceLanguageName
	pb.AgentName = k.AgentName
	return pb
}

// FromProto converts protobuf representation to ServiceAggregationKey.
func (k *serviceAggregationKey) FromProto(pb *aggregationpb.ServiceAggregationKey) {
	k.Timestamp = timestamppb.PBTimestampToTime(pb.Timestamp)
	k.ServiceName = pb.ServiceName
	k.ServiceEnvironment = pb.ServiceEnvironment
	k.ServiceLanguageName = pb.ServiceLanguageName
	k.AgentName = pb.AgentName
}

// ToProto converts ServiceMetrics to its protobuf representation.
func (m *serviceMetrics) ToProto() *aggregationpb.ServiceMetrics {
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

// ToProto converts ServiceInstanceAggregationKey to its protobuf representation.
func (k *serviceInstanceAggregationKey) ToProto() *aggregationpb.ServiceInstanceAggregationKey {
	pb := aggregationpb.ServiceInstanceAggregationKeyFromVTPool()
	pb.GlobalLabelsStr = []byte(k.GlobalLabelsStr)
	return pb
}

// FromProto converts protobuf representation to ServiceInstanceAggregationKey.
func (k *serviceInstanceAggregationKey) FromProto(pb *aggregationpb.ServiceInstanceAggregationKey) {
	k.GlobalLabelsStr = string(pb.GlobalLabelsStr)
}

// ToProto converts ServiceInstanceMetrics to its protobuf representation.
func (m *serviceInstanceMetrics) ToProto() *aggregationpb.ServiceInstanceMetrics {
	pb := aggregationpb.ServiceInstanceMetricsFromVTPool()
	if len(m.TransactionGroups) > cap(pb.TransactionMetrics) {
		pb.TransactionMetrics = make([]*aggregationpb.KeyedTransactionMetrics, 0, len(m.TransactionGroups))
	}
	for _, m := range m.TransactionGroups {
		pb.TransactionMetrics = append(pb.TransactionMetrics, m)
	}
	if len(m.ServiceTransactionGroups) > cap(pb.ServiceTransactionMetrics) {
		pb.ServiceTransactionMetrics = make([]*aggregationpb.KeyedServiceTransactionMetrics, 0, len(m.ServiceTransactionGroups))
	}
	for _, m := range m.ServiceTransactionGroups {
		pb.ServiceTransactionMetrics = append(pb.ServiceTransactionMetrics, m)
	}
	if len(m.SpanGroups) > cap(pb.SpanMetrics) {
		pb.SpanMetrics = make([]*aggregationpb.KeyedSpanMetrics, 0, len(m.SpanGroups))
	}
	for _, m := range m.SpanGroups {
		pb.SpanMetrics = append(pb.SpanMetrics, m)
	}
	return pb
}

// ToProto converts TransactionAggregationKey to its protobuf representation.
func (k *transactionAggregationKey) ToProto() *aggregationpb.TransactionAggregationKey {
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
func (k *transactionAggregationKey) FromProto(pb *aggregationpb.TransactionAggregationKey) {
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

	k.FAASColdstart = nullable.Bool(pb.FaasColdstart)
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

// ToProto converts ServiceTransactionAggregationKey to its protobuf representation.
func (k *serviceTransactionAggregationKey) ToProto() *aggregationpb.ServiceTransactionAggregationKey {
	pb := aggregationpb.ServiceTransactionAggregationKeyFromVTPool()
	pb.TransactionType = k.TransactionType
	return pb
}

// FromProto converts protobuf representation to ServiceTransactionAggregationKey.
func (k *serviceTransactionAggregationKey) FromProto(pb *aggregationpb.ServiceTransactionAggregationKey) {
	k.TransactionType = pb.TransactionType
}

// ToProto converts SpanAggregationKey to its protobuf representation.
func (k *spanAggregationKey) ToProto() *aggregationpb.SpanAggregationKey {
	pb := aggregationpb.SpanAggregationKeyFromVTPool()
	pb.SpanName = k.SpanName
	pb.Outcome = k.Outcome

	pb.TargetType = k.TargetType
	pb.TargetName = k.TargetName

	pb.Resource = k.Resource
	return pb
}

// FromProto converts protobuf representation to SpanAggregationKey.
func (k *spanAggregationKey) FromProto(pb *aggregationpb.SpanAggregationKey) {
	k.SpanName = pb.SpanName
	k.Outcome = pb.Outcome

	k.TargetType = pb.TargetType
	k.TargetName = pb.TargetName

	k.Resource = pb.Resource
}

// ToProto converts Overflow to its protobuf representation.
func (o *overflow) ToProto() *aggregationpb.Overflow {
	pb := aggregationpb.OverflowFromVTPool()
	if !o.OverflowTransaction.Empty() {
		pb.OverflowTransactions = o.OverflowTransaction.Metrics
		pb.OverflowTransactionsEstimator = hllBytes(o.OverflowTransaction.Estimator)
	}
	if !o.OverflowServiceTransaction.Empty() {
		pb.OverflowServiceTransactions = o.OverflowServiceTransaction.Metrics
		pb.OverflowServiceTransactionsEstimator = hllBytes(o.OverflowServiceTransaction.Estimator)
	}
	if !o.OverflowSpan.Empty() {
		pb.OverflowSpans = o.OverflowSpan.Metrics
		pb.OverflowSpansEstimator = hllBytes(o.OverflowSpan.Estimator)
	}
	return pb
}

// FromProto converts protobuf representation to Overflow.
func (o *overflow) FromProto(pb *aggregationpb.Overflow) {
	if pb.OverflowTransactions != nil {
		o.OverflowTransaction.Estimator = hllSketch(pb.OverflowTransactionsEstimator)
		o.OverflowTransaction.Metrics = pb.OverflowTransactions
		pb.OverflowTransactions = nil
	}
	if pb.OverflowServiceTransactions != nil {
		o.OverflowServiceTransaction.Estimator = hllSketch(pb.OverflowServiceTransactionsEstimator)
		o.OverflowServiceTransaction.Metrics = pb.OverflowServiceTransactions
		pb.OverflowServiceTransactions = nil
	}
	if pb.OverflowSpans != nil {
		o.OverflowSpan.Estimator = hllSketch(pb.OverflowSpansEstimator)
		o.OverflowSpan.Metrics = pb.OverflowSpans
		pb.OverflowSpans = nil
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

func histogramFromProto(h *hdrhistogram.HistogramRepresentation, pb *aggregationpb.HDRHistogram) {
	if pb == nil {
		return
	}
	h.LowestTrackableValue = pb.LowestTrackableValue
	h.HighestTrackableValue = pb.HighestTrackableValue
	h.SignificantFigures = pb.SignificantFigures
	h.CountsRep.Reset()

	for i := 0; i < len(pb.Bars); i++ {
		bar := pb.Bars[i]
		h.CountsRep.Add(bar.Bucket, bar.Counts)
	}
}

func histogramToProto(h *hdrhistogram.HistogramRepresentation) *aggregationpb.HDRHistogram {
	if h == nil {
		return nil
	}
	pb := aggregationpb.HDRHistogramFromVTPool()
	pb.LowestTrackableValue = h.LowestTrackableValue
	pb.HighestTrackableValue = h.HighestTrackableValue
	pb.SignificantFigures = h.SignificantFigures
	countsLen := h.CountsRep.Len()
	if countsLen > cap(pb.Bars) {
		pb.Bars = make([]*aggregationpb.Bar, 0, countsLen)
	}
	h.CountsRep.ForEach(func(bucket int32, value int64) {
		bar := aggregationpb.BarFromVTPool()
		bar.Bucket = bucket
		bar.Counts = value
		pb.Bars = append(pb.Bars, bar)
	})
	return pb
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
