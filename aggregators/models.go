// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"encoding/binary"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"

	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-aggregation/aggregators/nullable"
	"github.com/elastic/apm-data/model/modelpb"
)

// Limits define the aggregation limits. Once the limits are reached
// the metrics will overflow into dedicated overflow buckets.
type Limits struct {
	// MaxServices is the limit on the total number of unique services.
	// A unique service is identified by a unique ServiceAggregationKey.
	// This limit is shared across all aggregation metrics.
	MaxServices int

	// MaxServiceInstanceGroupsPerService is the limit on the total number
	// of unique service instance groups within a service.
	// A unique service instance group within a service is identified by a
	// unique ServiceInstanceAggregationKey.
	MaxServiceInstanceGroupsPerService int

	// MaxSpanGroups is the limit on total number of unique span groups
	// across all services.
	// A unique span group is identified by a unique
	// ServiceAggregationKey + ServiceInstanceAggregationKey + SpanAggregationKey.
	MaxSpanGroups int

	// MaxSpanGroupsPerService is the limit on the total number of unique
	// span groups within a service.
	// A unique span group within a service is identified by a unique
	// SpanAggregationKey.
	MaxSpanGroupsPerService int

	// MaxTransactionGroups is the limit on total number of unique
	// transaction groups across all services.
	// A unique transaction group is identified by a unique
	// ServiceAggregationKey + ServiceInstanceAggregationKey + TransactionAggregationKey.
	MaxTransactionGroups int

	// MaxTransactionGroupsPerService is the limit on the number of unique
	// transaction groups within a service.
	// A unique transaction group within a service is identified by a unique
	// TransactionAggregationKey.
	MaxTransactionGroupsPerService int

	// MaxServiceTransactionGroups is the limit on total number of unique
	// service transaction groups across all services.
	// A unique service transaction group is identified by a unique
	// ServiceAggregationKey + ServiceInstanceAggregationKey + ServiceTransactionAggregationKey.
	MaxServiceTransactionGroups int

	// MaxServiceTransactionGroupsPerService is the limit on the number
	// of unique service transaction groups within a service.
	// A unique service transaction group within a service is identified
	// by a unique ServiceTransactionAggregationKey.
	MaxServiceTransactionGroupsPerService int
}

// CombinedMetricsKey models the key to store the data in LSM tree.
// Each key-value pair represents a set of unique metric for a combined metrics ID.
// The processing time used in the key should be rounded to the
// duration of aggregation since the zero time.
type CombinedMetricsKey struct {
	Interval       time.Duration
	ProcessingTime time.Time
	PartitionID    uint16
	ID             [16]byte
}

// CombinedMetrics models the value to store the data in LSM tree.
// Each unique combined metrics ID stores a combined metrics per aggregation
// interval. CombinedMetrics encapsulates the aggregated metrics
// as well as the overflow metrics.
type CombinedMetrics struct {
	Services map[ServiceAggregationKey]ServiceMetrics

	// OverflowServices provides a dedicated bucket for collecting
	// aggregate metrics for all the aggregation groups for all services
	// that overflowed due to max services limit being reached.
	OverflowServices Overflow

	// OverflowServiceInstancesEstimator estimates the number of unique service
	// instance aggregation keys that overflowed due to max services limit or
	// max service instances per service limit.
	OverflowServiceInstancesEstimator *hyperloglog.Sketch

	// eventsTotal is the total number of individual events, including
	// all overflows, that were aggregated for this combined metrics. It
	// is used for internal monitoring purposes and is approximated when
	// partitioning is enabled.
	eventsTotal float64

	// youngestEventTimestamp is the youngest event that was aggregated
	// in the combined metrics based on the received timestamp.
	youngestEventTimestamp time.Time
}

// ServiceAggregationKey models the key used to store service specific
// aggregation metrics.
type ServiceAggregationKey struct {
	Timestamp           time.Time
	ServiceName         string
	ServiceEnvironment  string
	ServiceLanguageName string
	AgentName           string
}

// Hash returns a xxhash.Digest after hashing the aggregation key on top of h.
func (k ServiceAggregationKey) Hash(h xxhash.Digest) xxhash.Digest {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(k.Timestamp.UnixNano()))
	h.Write(buf[:])

	h.WriteString(k.ServiceName)
	h.WriteString(k.ServiceEnvironment)
	h.WriteString(k.ServiceLanguageName)
	h.WriteString(k.AgentName)
	return h
}

// ServiceMetrics models the value to store all the aggregated metrics
// for a specific service aggregation key.
type ServiceMetrics struct {
	ServiceInstanceGroups map[ServiceInstanceAggregationKey]ServiceInstanceMetrics
	OverflowGroups        Overflow
}

// ServiceInstanceAggregationKey models the key used to store service instance specific
// aggregation metrics.
type ServiceInstanceAggregationKey struct {
	GlobalLabelsStr string
}

// Hash returns a xxhash.Digest after hashing the aggregation key on top of h.
func (k ServiceInstanceAggregationKey) Hash(h xxhash.Digest) xxhash.Digest {
	h.WriteString(k.GlobalLabelsStr)
	return h
}

// ServiceInstanceMetrics models the value to store all the aggregated metrics
// for a specific service instance aggregation key.
type ServiceInstanceMetrics struct {
	TransactionGroups        map[TransactionAggregationKey]TransactionMetrics
	ServiceTransactionGroups map[ServiceTransactionAggregationKey]ServiceTransactionMetrics
	SpanGroups               map[SpanAggregationKey]SpanMetrics
}

func insertHash(estimator **hyperloglog.Sketch, hash uint64) {
	if *estimator == nil {
		*estimator = hyperloglog.New14()
	}
	(*estimator).InsertHash(hash)
}

func mergeEstimator(to **hyperloglog.Sketch, from *hyperloglog.Sketch) {
	if *to == nil {
		*to = hyperloglog.New14()
	}
	(*to).Merge(from)
}

type OverflowTransaction struct {
	Metrics   TransactionMetrics
	Estimator *hyperloglog.Sketch
}

func (o *OverflowTransaction) Merge(from *TransactionMetrics, hash uint64) {
	o.Metrics.Merge(from)
	insertHash(&o.Estimator, hash)
}

func (o *OverflowTransaction) MergeOverflow(from *OverflowTransaction) {
	if from.Estimator != nil {
		o.Metrics.Merge(&from.Metrics)
		mergeEstimator(&o.Estimator, from.Estimator)
	}
}

func (o *OverflowTransaction) Empty() bool {
	return o.Estimator == nil
}

type OverflowServiceTransaction struct {
	Metrics   ServiceTransactionMetrics
	Estimator *hyperloglog.Sketch
}

func (o *OverflowServiceTransaction) Merge(from *ServiceTransactionMetrics, hash uint64) {
	o.Metrics.Merge(from)
	insertHash(&o.Estimator, hash)
}

func (o *OverflowServiceTransaction) MergeOverflow(from *OverflowServiceTransaction) {
	if from.Estimator != nil {
		o.Metrics.Merge(&from.Metrics)
		mergeEstimator(&o.Estimator, from.Estimator)
	}
}

func (o *OverflowServiceTransaction) Empty() bool {
	return o.Estimator == nil
}

type OverflowSpan struct {
	Metrics   SpanMetrics
	Estimator *hyperloglog.Sketch
}

func (o *OverflowSpan) Merge(from *SpanMetrics, hash uint64) {
	o.Metrics.Merge(from)
	insertHash(&o.Estimator, hash)
}

func (o *OverflowSpan) MergeOverflow(from *OverflowSpan) {
	if from.Estimator != nil {
		o.Metrics.Merge(&from.Metrics)
		mergeEstimator(&o.Estimator, from.Estimator)
	}
}

func (o *OverflowSpan) Empty() bool {
	return o.Estimator == nil
}

// Overflow contains transaction and spans overflow metrics and cardinality
// estimators for the aggregation group for overflow buckets.
type Overflow struct {
	OverflowTransaction        OverflowTransaction
	OverflowServiceTransaction OverflowServiceTransaction
	OverflowSpan               OverflowSpan
}

// TransactionAggregationKey models the key used to store transaction
// aggregation metrics.
type TransactionAggregationKey struct {
	TraceRoot bool

	ContainerID       string
	KubernetesPodName string

	ServiceVersion  string
	ServiceNodeName string

	ServiceRuntimeName     string
	ServiceRuntimeVersion  string
	ServiceLanguageVersion string

	HostHostname   string
	HostName       string
	HostOSPlatform string

	EventOutcome string

	TransactionName   string
	TransactionType   string
	TransactionResult string

	FAASColdstart   nullable.NullableBool
	FAASID          string
	FAASName        string
	FAASVersion     string
	FAASTriggerType string

	CloudProvider         string
	CloudRegion           string
	CloudAvailabilityZone string
	CloudServiceName      string
	CloudAccountID        string
	CloudAccountName      string
	CloudMachineType      string
	CloudProjectID        string
	CloudProjectName      string
}

// Hash returns a xxhash.Digest after hashing the aggregation key on top of h.
func (k TransactionAggregationKey) Hash(h xxhash.Digest) xxhash.Digest {
	if k.TraceRoot {
		h.WriteString("1")
	}

	h.WriteString(k.ContainerID)
	h.WriteString(k.KubernetesPodName)

	h.WriteString(k.ServiceVersion)
	h.WriteString(k.ServiceNodeName)

	h.WriteString(k.ServiceRuntimeName)
	h.WriteString(k.ServiceRuntimeVersion)
	h.WriteString(k.ServiceLanguageVersion)

	h.WriteString(k.HostHostname)
	h.WriteString(k.HostName)
	h.WriteString(k.HostOSPlatform)

	h.WriteString(k.EventOutcome)

	h.WriteString(k.TransactionName)
	h.WriteString(k.TransactionType)
	h.WriteString(k.TransactionResult)

	if k.FAASColdstart == nullable.True {
		h.WriteString("1")
	}
	h.WriteString(k.FAASID)
	h.WriteString(k.FAASName)
	h.WriteString(k.FAASVersion)
	h.WriteString(k.FAASTriggerType)

	h.WriteString(k.CloudProvider)
	h.WriteString(k.CloudRegion)
	h.WriteString(k.CloudAvailabilityZone)
	h.WriteString(k.CloudServiceName)
	h.WriteString(k.CloudAccountID)
	h.WriteString(k.CloudAccountName)
	h.WriteString(k.CloudMachineType)
	h.WriteString(k.CloudProjectID)
	h.WriteString(k.CloudProjectName)
	return h
}

// TransactionMetrics models the aggregated metric for each unique
// transaction metrics key. TransactionMetrics is designed to use
// two different data structures depending on the number of transactions
// getting aggregated. For lower number of transactions (< 255), a slice
// is used. The slice is promoted to a histogram if the number of entries
// exceed the limit for the slice data structure.
type TransactionMetrics struct {
	Histogram *hdrhistogram.HistogramRepresentation
}

func (m *TransactionMetrics) Merge(from *TransactionMetrics) {
	mergeTransactionMetrics(m, from)
}

// SpanAggregationKey models the key used to store span aggregation metrics.
type SpanAggregationKey struct {
	SpanName string
	Outcome  string

	TargetType string
	TargetName string

	Resource string
}

// Hash returns a xxhash.Digest after hashing the aggregation key on top of h.
func (k SpanAggregationKey) Hash(h xxhash.Digest) xxhash.Digest {
	h.WriteString(k.SpanName)
	h.WriteString(k.Outcome)

	h.WriteString(k.TargetType)
	h.WriteString(k.TargetName)

	h.WriteString(k.Resource)
	return h
}

// SpanMetrics models the aggregated metric for each unique span metrics key.
type SpanMetrics struct {
	Count float64
	Sum   float64
}

func (m *SpanMetrics) Merge(from *SpanMetrics) {
	mergeSpanMetrics(m, from)
}

// ServiceTransactionAggregationKey models the key used to store
// service transaction aggregation metrics.
type ServiceTransactionAggregationKey struct {
	TransactionType string
}

// Hash returns a xxhash.Digest after hashing the aggregation key on top of h.
func (k ServiceTransactionAggregationKey) Hash(h xxhash.Digest) xxhash.Digest {
	h.WriteString(k.TransactionType)
	return h
}

// ServiceTransactionMetrics models the value to store all the aggregated metrics
// for a specific service transaction aggregation key.
type ServiceTransactionMetrics struct {
	Histogram    *hdrhistogram.HistogramRepresentation
	FailureCount float64
	SuccessCount float64
}

func (m *ServiceTransactionMetrics) Merge(from *ServiceTransactionMetrics) {
	mergeServiceTransactionMetrics(m, from)
}

// GlobalLabels is an intermediate struct used to marshal/unmarshal the provided
// global labels into a comparable format. The format is used by pebble db to
// compare service aggregation keys.
type GlobalLabels struct {
	Labels        modelpb.Labels
	NumericLabels modelpb.NumericLabels
}
