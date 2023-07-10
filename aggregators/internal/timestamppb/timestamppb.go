// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package timestamppb

import "time"

func TimeToPBTimestamp(t time.Time) uint64 {
	return uint64(t.UnixNano())
}

func PBTimestampToTime(timestamp uint64) time.Time {
	return time.Unix(0, int64(timestamp)).UTC()
}
