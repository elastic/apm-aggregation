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

	"github.com/elastic/apm-aggregation/aggregators/internal/hdrhistogram"
	"github.com/elastic/apm-data/model/modelpb"
)

func TestMerge(t *testing.T) {
	ts := time.Time{}
	for _, tc := range []struct {
		name     string
		limits   Limits
		to       CombinedMetrics
		from     CombinedMetrics
		expected CombinedMetrics
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 5}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 5}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 5}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(4)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 2}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 2}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(14)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1000)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 500}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 500}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 500}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(4)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 2}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 2}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1004)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 502}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 502}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 502}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(4)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 2}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 2}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1000)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 500}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 500}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 500}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1004)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 502}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 502}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 502}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1400)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 700}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 700}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 700}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1000)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 500}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 500}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 500}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2400)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 1200}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 1200}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 1200}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(14)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span2", count: 5}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(24)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).                   // no merge as transactions will overflow
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).                      // no merge as service transactions will overflow
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}).                                                 // no merge as spans will overflow
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}). // all transactions in from will overflow
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).    // all service transactions in from will overflow
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 5}),                                    // all spans will overflow but span.name dropped
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(34)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}).
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 10}).
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 10}).
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 10}), // since max span groups per svc limit is 1, span.name will be dropped
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span2", count: 5}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(44)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}).
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 15}).
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 15}).
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 15}), // all spans will overflow but span.name dropped
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(14)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(26)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span2", count: 5}).
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn3", txnType: "type3", count: 8}).
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type3", count: 8}).
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 8}), // since max span groups per svc limit is 1, span.name will be dropped
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(40)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}).
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}).
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 5}).
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn3", txnType: "type3", count: 8}).
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type3", count: 8}).
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 8}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(14)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
				addTransaction(ts, "svc2", "", testTransaction{txnName: "txn1", txnType: "type1", count: 5}).
				addServiceTransaction(ts, "svc2", "", testServiceTransaction{txnType: "type1", count: 5}).
				addSpan(ts, "svc2", "", testSpan{spanName: "span1", count: 5}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(24)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}).
				addGlobalServiceOverflowTransaction(ts, "svc2", "", testTransaction{txnName: "txn1", txnType: "type1", count: 5}).
				addGlobalServiceOverflowServiceTransaction(ts, "svc2", "", testServiceTransaction{txnType: "type1", count: 5}).
				addGlobalServiceOverflowSpan(ts, "svc2", "", testSpan{spanName: "span1", count: 5}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(111)).
				addServiceInstance(ts, "svc1", ""),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(222)).
				addServiceInstance(ts, "svc2", ""),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(333)).
				addServiceInstance(ts, "svc1", "").
				addGlobalServiceOverflowServiceInstance(ts, "svc2", ""),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(14)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span2", count: 5}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(24)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}).
				addSpan(ts, "svc1", "", testSpan{spanName: "span1", count: 7}).
				addPerServiceOverflowTransaction(ts, "svc1", "", testTransaction{txnName: "txn2", txnType: "type2", count: 5}).
				addPerServiceOverflowServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type2", count: 5}).
				addPerServiceOverflowSpan(ts, "svc1", "", testSpan{spanName: "", count: 5}),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1)).
				addServiceInstance(ts, "svc1", "1"),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2)).
				addServiceInstance(ts, "svc1", "2"),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(3)).
				addServiceInstance(ts, "svc1", "1").
				addServiceInstance(ts, "svc1", "2"),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1)).
				addServiceInstance(ts, "svc1", "1"),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2)).
				addServiceInstance(ts, "svc1", "2"),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(3)).
				addServiceInstance(ts, "svc1", "1").
				addGlobalServiceOverflowServiceInstance(ts, "svc1", "2"),
			),
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1)).
				addServiceInstance(ts, "svc1", "1"),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2)).
				addServiceInstance(ts, "svc2", "2"),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(3)).
				addServiceInstance(ts, "svc1", "1").
				addGlobalServiceOverflowServiceInstance(ts, "svc2", "2"),
			),
		},
		{
			name: "service_instance_overflow_per_svc_on_metrics",
			limits: Limits{
				MaxSpanGroups:                         100,
				MaxSpanGroupsPerService:               100,
				MaxTransactionGroups:                  100,
				MaxTransactionGroupsPerService:        100,
				MaxServiceTransactionGroups:           100,
				MaxServiceTransactionGroupsPerService: 100,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1)).
				addTransaction(ts, "svc1", "1", testTransaction{txnName: "txn1", txnType: "type1", count: 1}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2)).
				addTransaction(ts, "svc1", "2", testTransaction{txnName: "txn1", txnType: "type1", count: 2}),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(3)).
				addTransaction(ts, "svc1", "1", testTransaction{txnName: "txn1", txnType: "type1", count: 1}).
				addPerServiceOverflowTransaction(ts, "svc1", "2", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
				addGlobalServiceOverflowServiceInstance(ts, "svc1", "2"),
			),
		},
		{
			name: "service_instance_overflow_global_merge",
			limits: Limits{
				MaxSpanGroups:                         100,
				MaxSpanGroupsPerService:               100,
				MaxTransactionGroups:                  100,
				MaxTransactionGroupsPerService:        100,
				MaxServiceTransactionGroups:           100,
				MaxServiceTransactionGroupsPerService: 100,
				MaxServices:                           1,
				MaxServiceInstanceGroupsPerService:    1,
			},
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1)).
				addTransaction(ts, "svc1", "1", testTransaction{txnName: "txn1", txnType: "type1", count: 1}).
				addGlobalServiceOverflowServiceInstance(ts, "svc1", "2").
				addGlobalServiceOverflowServiceInstance(ts, "svc3", "3"),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(2)).
				addTransaction(ts, "svc2", "2", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
				addGlobalServiceOverflowServiceInstance(ts, "svc2", "3").
				addGlobalServiceOverflowServiceInstance(ts, "svc3", "3"),
			),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(3)).
				addTransaction(ts, "svc1", "1", testTransaction{txnName: "txn1", txnType: "type1", count: 1}).
				addGlobalServiceOverflowTransaction(ts, "svc2", "2", testTransaction{txnName: "txn1", txnType: "type1", count: 2}).
				addGlobalServiceOverflowServiceInstance(ts, "svc2", "2").
				addGlobalServiceOverflowServiceInstance(ts, "svc2", "3").
				addGlobalServiceOverflowServiceInstance(ts, "svc1", "2").
				addGlobalServiceOverflowServiceInstance(ts, "svc3", "3"),
			),
		},
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
			to: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(7)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}),
			),
			from: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(1))),
			expected: CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(8)).
				addTransaction(ts, "svc1", "", testTransaction{txnName: "txn1", txnType: "type1", count: 7}).
				addServiceTransaction(ts, "svc1", "", testServiceTransaction{txnType: "type1", count: 7}),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merge(&tc.to, &tc.from, tc.limits)
			assert.Empty(t, cmp.Diff(
				tc.expected, tc.to,
				cmp.Exporter(func(reflect.Type) bool { return true }),
				cmp.Comparer(func(a, b hdrhistogram.HybridCountsRep) bool {
					return a.Equal(&b)
				}),
			))
		})
	}
}

type testCombinedMetricsCfg struct {
	eventsTotal            int64
	youngestEventTimestamp time.Time
}

type testCombinedMetricsOpt func(cfg testCombinedMetricsCfg) testCombinedMetricsCfg

func withEventsTotal(total int64) testCombinedMetricsOpt {
	return func(cfg testCombinedMetricsCfg) testCombinedMetricsCfg {
		cfg.eventsTotal = total
		return cfg
	}
}

func withYoungestEventTimestamp(ts time.Time) testCombinedMetricsOpt {
	return func(cfg testCombinedMetricsCfg) testCombinedMetricsCfg {
		cfg.youngestEventTimestamp = ts
		return cfg
	}
}

type TestCombinedMetrics CombinedMetrics

func createTestCombinedMetrics(opts ...testCombinedMetricsOpt) *TestCombinedMetrics {
	var cfg testCombinedMetricsCfg
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	return &TestCombinedMetrics{
		eventsTotal:            cfg.eventsTotal,
		youngestEventTimestamp: cfg.youngestEventTimestamp,
	}
}

type testTransaction struct {
	txnName      string
	txnType      string
	eventOutcome string
	faas         *modelpb.Faas
	count        int
}

type testServiceTransaction struct {
	txnType string
	count   int
}

type testSpan struct {
	spanName            string
	destinationResource string
	targetName          string
	count               int
}

func txnKeyFromTestTxn(txn testTransaction) TransactionAggregationKey {
	tk := TransactionAggregationKey{
		TransactionName: txn.txnName,
		TransactionType: txn.txnType,
		EventOutcome:    txn.eventOutcome,
	}
	if txn.faas != nil {
		tk.FAASID = txn.faas.Id
		tk.FAASName = txn.faas.Name
		tk.FAASVersion = txn.faas.Version
		tk.FAASTriggerType = txn.faas.TriggerType
		tk.FAASColdstart.ParseBoolPtr(txn.faas.ColdStart)
	}
	return tk
}

func spanKeyFromTestSpan(span testSpan) SpanAggregationKey {
	return SpanAggregationKey{
		SpanName:   span.spanName,
		TargetName: span.targetName,
		Resource:   span.destinationResource,
	}
}

func (m *TestCombinedMetrics) addTransaction(timestamp time.Time, serviceName, globalLabelsStr string, txn testTransaction) *TestCombinedMetrics {
	upsertSIM(m, timestamp, serviceName, globalLabelsStr, func(sim *ServiceInstanceMetrics) {
		tk := txnKeyFromTestTxn(txn)
		tm, ok := sim.TransactionGroups[tk]
		if !ok {
			tm = newTransactionMetrics()
		}
		for i := 0; i < txn.count; i++ {
			tm.Histogram.RecordDuration(time.Second, 1)
		}
		sim.TransactionGroups[tk] = tm
	})
	return m
}

func (m *TestCombinedMetrics) addPerServiceOverflowTransaction(timestamp time.Time, serviceName, globalLabelsStr string, txn testTransaction) *TestCombinedMetrics {
	sk := ServiceAggregationKey{
		Timestamp:   timestamp,
		ServiceName: serviceName,
	}
	upsertPerServiceOverflow(m, timestamp, serviceName, func(overflow *Overflow) {
		sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
		tk := txnKeyFromTestTxn(txn)
		tm := newTransactionMetrics()
		for i := 0; i < txn.count; i++ {
			tm.Histogram.RecordDuration(time.Second, 1)
		}
		overflow.OverflowTransaction.Merge(&tm, Hasher{}.Chain(sk).Chain(sik).Chain(tk).Sum())
	})
	return m
}

func (m *TestCombinedMetrics) addGlobalServiceOverflowTransaction(timestamp time.Time, serviceName, globalLabelsStr string, txn testTransaction) *TestCombinedMetrics {
	upsertGlobalServiceOverflow(m, timestamp, serviceName, globalLabelsStr, func(overflow *Overflow) {
		sk := ServiceAggregationKey{
			Timestamp:   timestamp,
			ServiceName: serviceName,
		}
		sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
		tk := txnKeyFromTestTxn(txn)
		tm := newTransactionMetrics()
		for i := 0; i < txn.count; i++ {
			tm.Histogram.RecordDuration(time.Second, 1)
		}
		overflow.OverflowTransaction.Merge(&tm, Hasher{}.Chain(sk).Chain(sik).Chain(tk).Sum())
	})
	return m
}

func (m *TestCombinedMetrics) addServiceTransaction(timestamp time.Time, serviceName, globalLabelsStr string, svcTxn testServiceTransaction) *TestCombinedMetrics {
	upsertSIM(m, timestamp, serviceName, globalLabelsStr, func(sim *ServiceInstanceMetrics) {
		stk := ServiceTransactionAggregationKey{
			TransactionType: svcTxn.txnType,
		}
		stm, ok := sim.ServiceTransactionGroups[stk]
		if !ok {
			stm = newServiceTransactionMetrics()
		}
		for i := 0; i < svcTxn.count; i++ {
			stm.Histogram.RecordDuration(time.Second, 1)
			stm.FailureCount += 0.0
			stm.SuccessCount += 1.0
		}
		sim.ServiceTransactionGroups[stk] = stm
	})
	return m
}

func (m *TestCombinedMetrics) addPerServiceOverflowServiceTransaction(timestamp time.Time, serviceName, globalLabelsStr string, svcTxn testServiceTransaction) *TestCombinedMetrics {
	sk := ServiceAggregationKey{
		Timestamp:   timestamp,
		ServiceName: serviceName,
	}
	upsertPerServiceOverflow(m, timestamp, serviceName, func(overflow *Overflow) {
		sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
		stk := ServiceTransactionAggregationKey{
			TransactionType: svcTxn.txnType,
		}
		stm := newServiceTransactionMetrics()
		for i := 0; i < svcTxn.count; i++ {
			stm.Histogram.RecordDuration(time.Second, 1)
			stm.FailureCount += 0.0
			stm.SuccessCount += 1.0
		}
		overflow.OverflowServiceTransaction.Merge(&stm, Hasher{}.Chain(sk).Chain(sik).Chain(stk).Sum())
	})
	return m
}

func (m *TestCombinedMetrics) addGlobalServiceOverflowServiceTransaction(timestamp time.Time, serviceName, globalLabelsStr string, svcTxn testServiceTransaction) *TestCombinedMetrics {
	upsertGlobalServiceOverflow(m, timestamp, serviceName, globalLabelsStr, func(overflow *Overflow) {
		sk := ServiceAggregationKey{
			Timestamp:   timestamp,
			ServiceName: serviceName,
		}
		sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
		stk := ServiceTransactionAggregationKey{
			TransactionType: svcTxn.txnType,
		}
		stm := newServiceTransactionMetrics()
		for i := 0; i < svcTxn.count; i++ {
			stm.Histogram.RecordDuration(time.Second, 1)
			stm.FailureCount += 0.0
			stm.SuccessCount += 1.0
		}
		overflow.OverflowServiceTransaction.Merge(&stm, Hasher{}.Chain(sk).Chain(sik).Chain(stk).Sum())
	})
	return m
}

func (m *TestCombinedMetrics) addSpan(timestamp time.Time, serviceName, globalLabelsStr string, span testSpan) *TestCombinedMetrics {
	upsertSIM(m, timestamp, serviceName, globalLabelsStr, func(sim *ServiceInstanceMetrics) {
		spk := spanKeyFromTestSpan(span)
		spm := sim.SpanGroups[spk]
		for i := 0; i < span.count; i++ {
			spm.Count++
			spm.Sum++
		}
		sim.SpanGroups[spk] = spm
	})
	return m
}

func (m *TestCombinedMetrics) addPerServiceOverflowSpan(timestamp time.Time, serviceName, globalLabelsStr string, span testSpan) *TestCombinedMetrics {
	sk := ServiceAggregationKey{
		Timestamp:   timestamp,
		ServiceName: serviceName,
	}
	upsertPerServiceOverflow(m, timestamp, serviceName, func(overflow *Overflow) {
		sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
		spk := spanKeyFromTestSpan(span)
		spm := SpanMetrics{}
		for i := 0; i < span.count; i++ {
			spm.Count++
			spm.Sum++
		}
		overflow.OverflowSpan.Merge(&spm, Hasher{}.Chain(sk).Chain(sik).Chain(spk).Sum())
	})
	return m
}

func (m *TestCombinedMetrics) addGlobalServiceOverflowSpan(timestamp time.Time, serviceName, globalLabelsStr string, span testSpan) *TestCombinedMetrics {
	upsertGlobalServiceOverflow(m, timestamp, serviceName, globalLabelsStr, func(overflow *Overflow) {
		sk := ServiceAggregationKey{
			Timestamp:   timestamp,
			ServiceName: serviceName,
		}
		sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
		spk := spanKeyFromTestSpan(span)
		spm := SpanMetrics{}
		for i := 0; i < span.count; i++ {
			spm.Count++
			spm.Sum++
		}
		overflow.OverflowSpan.Merge(&spm, Hasher{}.Chain(sk).Chain(sik).Chain(spk).Sum())
	})
	return m
}

func (m *TestCombinedMetrics) addServiceInstance(timestamp time.Time, serviceName, globalLabelsStr string) *TestCombinedMetrics {
	upsertSIM(m, timestamp, serviceName, globalLabelsStr, func(_ *ServiceInstanceMetrics) {})
	return m
}

func (m *TestCombinedMetrics) addGlobalServiceOverflowServiceInstance(timestamp time.Time, serviceName, globalLabelsStr string) *TestCombinedMetrics {
	upsertGlobalServiceOverflow(m, timestamp, serviceName, globalLabelsStr, func(_ *Overflow) {})
	return m
}

func upsertSIM(cm *TestCombinedMetrics, timestamp time.Time, serviceName, globalLabelsStr string, updater func(sim *ServiceInstanceMetrics)) {
	sk := ServiceAggregationKey{
		Timestamp:   timestamp,
		ServiceName: serviceName,
	}
	sm, ok := cm.Services[sk]
	if !ok {
		sm = newServiceMetrics()
	}
	sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
	sim, ok := sm.ServiceInstanceGroups[sik]
	if !ok {
		sim = newServiceInstanceMetrics()
	}
	updater(&sim)
	sm.ServiceInstanceGroups[sik] = sim
	if cm.Services == nil {
		cm.Services = make(map[ServiceAggregationKey]ServiceMetrics)
	}
	cm.Services[sk] = sm
}

func upsertPerServiceOverflow(cm *TestCombinedMetrics, timestamp time.Time, serviceName string, updater func(overflow *Overflow)) {
	sk := ServiceAggregationKey{
		Timestamp:   timestamp,
		ServiceName: serviceName,
	}
	sm, ok := cm.Services[sk]
	if !ok {
		sm = newServiceMetrics()
	}
	updater(&sm.OverflowGroups)
	if cm.Services == nil {
		cm.Services = make(map[ServiceAggregationKey]ServiceMetrics)
	}
	cm.Services[sk] = sm
}

func upsertGlobalServiceOverflow(cm *TestCombinedMetrics, timestamp time.Time, serviceName, globalLabelsStr string, updater func(overflow *Overflow)) {
	sk := ServiceAggregationKey{
		Timestamp:   timestamp,
		ServiceName: serviceName,
	}
	sik := ServiceInstanceAggregationKey{GlobalLabelsStr: globalLabelsStr}
	updater(&cm.OverflowServices)
	insertHash(&cm.OverflowServiceInstancesEstimator, Hasher{}.Chain(sk).Chain(sik).Sum())
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
	to := CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(0)).
		addServiceInstance(ts, "svc1", ""))
	from1 := CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
		addTransaction(ts, "svc2", "", testTransaction{txnName: "txn1", txnType: "type1", count: 5}).
		addServiceTransaction(ts, "svc2", "", testServiceTransaction{txnType: "type1", count: 5}).
		addSpan(ts, "svc2", "", testSpan{spanName: "", count: 5}),
	)
	from2 := CombinedMetrics(*createTestCombinedMetrics(withEventsTotal(10)).
		addTransaction(ts, "svc3", "", testTransaction{txnName: "txn1", txnType: "type1", count: 5}).
		addServiceTransaction(ts, "svc3", "", testServiceTransaction{txnType: "type1", count: 5}).
		addSpan(ts, "svc3", "", testSpan{spanName: "", count: 5}),
	)
	merge(&to, &from1, limits)
	merge(&to, &from2, limits)
	assert.Equal(t, uint64(2), to.OverflowServices.OverflowTransaction.Estimator.Estimate())
	assert.Equal(t, uint64(2), to.OverflowServices.OverflowServiceTransaction.Estimator.Estimate())
	assert.Equal(t, uint64(2), to.OverflowServices.OverflowSpan.Estimator.Estimate())
}
