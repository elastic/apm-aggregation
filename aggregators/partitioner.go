// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package aggregators

// HashPartitioner is a hash based partitioner for pebble keys. The paritioner
// partitions the keys into buckets with number of buckets limited by the passed
// argument.
type HashPartitioner struct {
	maxPartitions uint16
}

// NewHashPartitioner creates a new instance of the HashPartitioner.
func NewHashPartitioner(maxPartitions uint16) *HashPartitioner {
	return &HashPartitioner{
		maxPartitions: maxPartitions,
	}
}

// Partition generates an ID to be used as partition ID given a hash of the key.
func (p *HashPartitioner) Partition(h uint64) uint16 {
	return uint16(h % uint64(p.maxPartitions))
}

// NoPartitioner disables partitions by producing constant partition IDs.
type NoPartitioner struct{}

// NewNoPartitioner returns a partitioner which disables partitioning.
func NewNoPartitioner() *NoPartitioner {
	return new(NoPartitioner)
}

// Partition generates a constant partition ID to disable partitioning.
func (p *NoPartitioner) Partition(h uint64) uint16 {
	return 0
}
