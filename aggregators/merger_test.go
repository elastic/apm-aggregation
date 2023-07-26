// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/elastic/apm-aggregation/aggregationpb"
)

func TestMerge(t *testing.T) {
	ts := time.Unix(0, 0).UTC()
	for _, tc := range []struct {
		name     string
		limits   Limits
		to       func() CombinedMetrics
		from     func() *aggregationpb.CombinedMetrics
		expected func() CombinedMetrics
	}{
		{
			name: "no_overflow_with_count_values",
			limits: Limits{
				MaxSpanGroups:                         2,
				MaxSpanGroupsPerService:               2,
				MaxTransactionGroups:                  2,
				MaxTransactionGroupsPerService:        2,
				MaxServiceTransactionGroups:           2,
				MaxServiceTransactionGroupsPerService: 2,
				MaxServices:                           2,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(10)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(5)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(5)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(5)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(4)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(2)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(2)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(2)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(14)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
		},
		{
			name: "no_overflow_with_histograms_in_to",
			limits: Limits{
				MaxSpanGroups:                         2,
				MaxSpanGroupsPerService:               2,
				MaxTransactionGroups:                  2,
				MaxTransactionGroupsPerService:        2,
				MaxServiceTransactionGroups:           2,
				MaxServiceTransactionGroupsPerService: 2,
				MaxServices:                           2,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1000)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(500)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(500)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(500)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(4)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(2)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(2)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(2)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1004)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(502)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(502)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(502)).
					Get()
			},
		},
		{
			name: "no_overflow_with_histogram_in_from",
			limits: Limits{
				MaxSpanGroups:                         2,
				MaxSpanGroupsPerService:               2,
				MaxTransactionGroups:                  2,
				MaxTransactionGroupsPerService:        2,
				MaxServiceTransactionGroups:           2,
				MaxServiceTransactionGroupsPerService: 2,
				MaxServices:                           2,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(4)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(2)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(2)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(2)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1000)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(500)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(500)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(500)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1004)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(502)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(502)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(502)).
					Get()
			},
		},
		{
			name: "no_overflow_with_histogram_in_both",
			limits: Limits{
				MaxSpanGroups:                         2,
				MaxSpanGroupsPerService:               2,
				MaxTransactionGroups:                  2,
				MaxTransactionGroupsPerService:        2,
				MaxServiceTransactionGroups:           2,
				MaxServiceTransactionGroupsPerService: 2,
				MaxServices:                           2,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1400)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(700)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(700)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(700)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1000)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(500)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(500)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(500)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(2400)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(1200)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(1200)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(1200)).
					Get()
			},
		},
		{
			name: "overflow_due_to_merge",
			limits: Limits{
				MaxSpanGroups:                         1,
				MaxSpanGroupsPerService:               1,
				MaxTransactionGroups:                  1,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           1,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(14)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(10)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span2"}, WithSpanCount(5)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(24)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					// no merge as span, transaction, and service transaction will overflow
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					// all span, transaction, and service transaction from _from_ will overflow
					AddSpanOverflow(SpanAggregationKey{SpanName: ""}, WithSpanCount(5)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					Get()
			},
		},
		{
			name: "to_overflowed_before_merge",
			limits: Limits{
				MaxSpanGroups:                         1,
				MaxSpanGroupsPerService:               1,
				MaxTransactionGroups:                  1,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           1,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(34)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					AddSpanOverflow(SpanAggregationKey{SpanName: ""}, WithSpanCount(10)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(10)).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(10)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(10)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span2"}, WithSpanCount(5)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(44)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					AddSpanOverflow(SpanAggregationKey{SpanName: ""}, WithSpanCount(15)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(15),
					).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(15),
					).
					Get()
			},
		},
		{
			name: "from_overflowed_before_merge",
			limits: Limits{
				MaxSpanGroups:                         1,
				MaxSpanGroupsPerService:               1,
				MaxTransactionGroups:                  1,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           1,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(14)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(26)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span2"}, WithSpanCount(5)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					AddSpanOverflow(SpanAggregationKey{SpanName: ""}, WithSpanCount(8)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type3"},
						WithTransactionCount(8)).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn3", TransactionType: "type3"},
						WithTransactionCount(8)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(40)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					AddSpanOverflow(SpanAggregationKey{SpanName: ""}, WithSpanCount(13)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type3"},
						WithTransactionCount(8)).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn3", TransactionType: "type3"},
						WithTransactionCount(8)).
					Get()
			},
		},
		{
			name: "svc_overflow",
			limits: Limits{
				MaxSpanGroups:                         1,
				MaxSpanGroupsPerService:               1,
				MaxTransactionGroups:                  1,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           1,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(14)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(10)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(5)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(5)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(5)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(24)).
					AddGlobalTransactionOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"},
						ServiceInstanceAggregationKey{},
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(5)).
					AddGlobalServiceTransactionOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"},
						ServiceInstanceAggregationKey{},
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(5)).
					AddGlobalSpanOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"},
						ServiceInstanceAggregationKey{},
						SpanAggregationKey{SpanName: "span1"},
						WithSpanCount(5)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
		},
		{
			name: "svc_overflow_only",
			limits: Limits{
				MaxSpanGroups:                         1,
				MaxSpanGroupsPerService:               1,
				MaxTransactionGroups:                  1,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           1,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(111)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(222)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(333)).
					AddGlobalServiceInstanceOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"},
						ServiceInstanceAggregationKey{},
					).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					Get()
			},
		},
		{
			name: "per_svc_overflow_known_svc",
			limits: Limits{
				MaxSpanGroups:                         100,
				MaxSpanGroupsPerService:               1,
				MaxTransactionGroups:                  100,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           100,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(14)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(10)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span2"}, WithSpanCount(5)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(24)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddSpan(SpanAggregationKey{SpanName: "span1"}, WithSpanCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					AddSpanOverflow(SpanAggregationKey{SpanName: ""}, WithSpanCount(5)).
					AddServiceTransactionOverflow(
						ServiceTransactionAggregationKey{TransactionType: "type2"},
						WithTransactionCount(5)).
					AddTransactionOverflow(
						TransactionAggregationKey{TransactionName: "txn2", TransactionType: "type2"},
						WithTransactionCount(5)).
					Get()
			},
		},
		{
			name: "service_instance_no_overflow",
			limits: Limits{
				MaxSpanGroups:                         0,
				MaxSpanGroupsPerService:               0,
				MaxTransactionGroups:                  0,
				MaxTransactionGroupsPerService:        0,
				MaxServiceTransactionGroups:           0,
				MaxServiceTransactionGroupsPerService: 0,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    2,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(2)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
					GetProto()
			},
			expected: func() CombinedMetrics {
				tcm := NewTestCombinedMetrics(WithEventsTotal(3))
				sm := tcm.AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"})
				sm.AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"})
				sm.AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "2"})
				return tcm.Get()
			},
		},
		{
			name: "service_instance_overflow_per_svc",
			limits: Limits{
				MaxSpanGroups:                         0,
				MaxSpanGroupsPerService:               0,
				MaxTransactionGroups:                  0,
				MaxTransactionGroupsPerService:        0,
				MaxServiceTransactionGroups:           0,
				MaxServiceTransactionGroupsPerService: 0,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(2)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(3)).
					AddGlobalServiceInstanceOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"},
						ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
					Get()
			},
		},
		{
			name: "service_instance_overflow_global",
			limits: Limits{
				MaxSpanGroups:                         0,
				MaxSpanGroupsPerService:               0,
				MaxTransactionGroups:                  0,
				MaxTransactionGroupsPerService:        0,
				MaxServiceTransactionGroups:           0,
				MaxServiceTransactionGroupsPerService: 0,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(2)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
					GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(3)).
					AddGlobalServiceInstanceOverflow(
						ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"},
						ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
					Get()
			},
		},
		// {
		// 	name: "service_instance_overflow_per_svc_on_metrics",
		// 	limits: Limits{
		// 		MaxSpanGroups:                         100,
		// 		MaxSpanGroupsPerService:               100,
		// 		MaxTransactionGroups:                  100,
		// 		MaxTransactionGroupsPerService:        100,
		// 		MaxServiceTransactionGroups:           100,
		// 		MaxServiceTransactionGroupsPerService: 100,
		// 		MaxServices:                           1,
		// 		MaxServiceInstanceGroupsPerService:    1,
		// 	},
		// 	to: func() CombinedMetrics {
		// 		return NewTestCombinedMetrics(WithEventsTotal(1)).
		// 			AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
		// 			AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
		// 			AddTransaction(
		// 				TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
		// 				WithTransactionCount(1)).
		// 			Get()
		// 	},
		// 	from: func() *aggregationpb.CombinedMetrics {
		// 		return NewTestCombinedMetrics(WithEventsTotal(2)).
		// 			AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
		// 			AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
		// 			AddTransaction(
		// 				TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
		// 				WithTransactionCount(2)).
		// 			GetProto()
		// 	},
		// 	expected: func() CombinedMetrics {
		// 		tcm := NewTestCombinedMetrics(WithEventsTotal(3))
		// 		tsm := tcm.
		// 			AddGlobalServiceInstanceOverflow(
		// 				ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"},
		// 				ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
		// 			AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"})
		// 		tsm.
		// 			AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "1"}).
		// 			AddTransaction(TransactionAggregationKey{
		// 				TransactionName: "txn1",
		// 				TransactionType: "type1",
		// 			}, WithTransactionCount(1))
		// 		tsm.
		// 			AddServiceInstanceMetrics(ServiceInstanceAggregationKey{GlobalLabelsStr: "2"}).
		// 			AddTransactionOverflow(TransactionAggregationKey{
		// 				TransactionName: "txn1",
		// 				TransactionType: "type1",
		// 			}, WithTransactionCount(2))
		// 		return tcm.Get()
		// 	},
		// },
		//{
		//	name: "service_instance_overflow_global_merge",
		//	limits: Limits{
		//		MaxSpanGroups:                         100,
		//		MaxSpanGroupsPerService:               100,
		//		MaxTransactionGroups:                  100,
		//		MaxTransactionGroupsPerService:        100,
		//		MaxServiceTransactionGroups:           100,
		//		MaxServiceTransactionGroupsPerService: 100,
		//		MaxServices:                           1,
		//		MaxServiceInstanceGroupsPerService:    1,
		//	},
		//	to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1)).
		//		addTransaction(ts, "svc1", "1", testTransaction{txnName: "txn1", txnType: "type1", count: 1}).
		//		addGlobalServiceOverflowServiceInstance(ts, "svc1", "2").
		//		addGlobalServiceOverflowServiceInstance(ts, "svc3", "3"),
		//	),
		//	from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2)).
		//		addTransaction(ts, "svc2", "2", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
		//		addGlobalServiceOverflowServiceInstance(ts, "svc2", "3").
		//		addGlobalServiceOverflowServiceInstance(ts, "svc3", "3"),
		//	),
		//	expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(3)).
		//		addTransaction(ts, "svc1", "1", testTransaction{txnName: "txn1", txnType: "type1", count: 1}).
		//		addGlobalServiceOverflowTransaction(ts, "svc2", "2", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
		//		addGlobalServiceOverflowServiceInstance(ts, "svc2", "2").
		//		addGlobalServiceOverflowServiceInstance(ts, "svc2", "3").
		//		addGlobalServiceOverflowServiceInstance(ts, "svc1", "2").
		//		addGlobalServiceOverflowServiceInstance(ts, "svc3", "3"),
		//	),
		//},
		{
			name: "merge_with_empty_combined_metrics",
			limits: Limits{
				MaxSpanGroups:                         100,
				MaxSpanGroupsPerService:               10,
				MaxTransactionGroups:                  100,
				MaxTransactionGroupsPerService:        1,
				MaxServiceTransactionGroups:           100,
				MaxServiceTransactionGroupsPerService: 1,
				MaxServices:                           1,
			},
			to: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(7)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
			from: func() *aggregationpb.CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(1)).GetProto()
			},
			expected: func() CombinedMetrics {
				return NewTestCombinedMetrics(WithEventsTotal(8)).
					AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
					AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
					AddTransaction(
						TransactionAggregationKey{TransactionName: "txn1", TransactionType: "type1"},
						WithTransactionCount(7)).
					AddServiceTransaction(
						ServiceTransactionAggregationKey{TransactionType: "type1"},
						WithTransactionCount(7)).
					Get()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmm := combinedMetricsMerger{
				limits:  tc.limits,
				metrics: tc.to(),
			}
			cmm.merge(tc.from())
			assert.Empty(t, cmp.Diff(
				tc.expected(), cmm.metrics,
				protocmp.Transform(),
				cmp.Exporter(func(reflect.Type) bool { return true }),
			))
		})
	}
}

func TestCardinalityEstimationOnSubKeyCollision(t *testing.T) {
	limits := Limits{
		MaxSpanGroups:                         100,
		MaxSpanGroupsPerService:               100,
		MaxTransactionGroups:                  100,
		MaxTransactionGroupsPerService:        100,
		MaxServiceTransactionGroups:           100,
		MaxServiceTransactionGroupsPerService: 100,
		MaxServices:                           1,
	}
	ts := time.Time{}
	to := NewTestCombinedMetrics(WithEventsTotal(0)).
		AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc1"}).
		AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
		Get()
	from1 := NewTestCombinedMetrics(WithEventsTotal(10)).
		AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc2"}).
		AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
		AddSpan(SpanAggregationKey{}, WithSpanCount(5)).
		AddTransaction(TransactionAggregationKey{
			TransactionName: "txn1",
			TransactionType: "type1",
		}, WithTransactionCount(5)).
		AddServiceTransaction(ServiceTransactionAggregationKey{
			TransactionType: "type1",
		}, WithTransactionCount(5)).
		GetProto()
	from2 := NewTestCombinedMetrics(WithEventsTotal(10)).
		AddServiceMetrics(ServiceAggregationKey{Timestamp: ts, ServiceName: "svc3"}).
		AddServiceInstanceMetrics(ServiceInstanceAggregationKey{}).
		AddSpan(SpanAggregationKey{}, WithSpanCount(5)).
		AddTransaction(TransactionAggregationKey{
			TransactionName: "txn1",
			TransactionType: "type1",
		}, WithTransactionCount(5)).
		AddServiceTransaction(ServiceTransactionAggregationKey{
			TransactionType: "type1",
		}, WithTransactionCount(5)).
		GetProto()
	cmm := combinedMetricsMerger{
		limits:  limits,
		metrics: to,
	}
	cmm.merge(from1)
	cmm.merge(from2)
	assert.Equal(t, uint64(2), cmm.metrics.OverflowServices.OverflowTransaction.Estimator.Estimate())
	assert.Equal(t, uint64(2), cmm.metrics.OverflowServices.OverflowServiceTransaction.Estimator.Estimate())
	assert.Equal(t, uint64(2), cmm.metrics.OverflowServices.OverflowSpan.Estimator.Estimate())
}
