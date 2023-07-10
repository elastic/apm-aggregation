// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package timestamppb

import "time"

// TimeToPBTimestamp encodes a time.Time to Unix epoch nanos in uint64 for protobuf.
func TimeToPBTimestamp(t time.Time) uint64 {
	return uint64(t.UnixNano())
}

// PBTimestampToTime decodes a uint64 of Unix epoch nanos to a time.Time for protobuf.
func PBTimestampToTime(timestamp uint64) time.Time {
	return time.Unix(0, int64(timestamp)).UTC()
}
