// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
)

func TestConfig(t *testing.T) {
	custom := metric.NewMeterProvider()
	for _, tt := range []struct {
		name     string
		options  []Option
		expected func() *config
	}{
		{
			name:    "empty_config",
			options: nil,
			expected: func() *config {
				mp := otel.GetMeterProvider()
				return &config{
					Meter:         mp.Meter(instrumentationName),
					MeterProvider: mp,
				}
			},
		},
		{
			name:    "config_with_custom_meter_provider",
			options: []Option{WithMeterProvider(custom)},
			expected: func() *config {
				return &config{
					Meter:         custom.Meter(instrumentationName),
					MeterProvider: custom,
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newConfig(tt.options...)

			assert.Equal(t, tt.expected(), cfg)
		})
	}
}
