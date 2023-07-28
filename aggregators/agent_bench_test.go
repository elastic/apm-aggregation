package aggregators

import (
	"bufio"
	"context"
	"io"
	"math"
	"os"
	"testing"

	"github.com/elastic/apm-data/input/elasticapm"
	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-data/model/modelprocessor"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ndjsonFileToBatch(reader io.Reader) *modelpb.Batch {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	elasticapmProcessor := elasticapm.NewProcessor(elasticapm.Config{
		Logger:       logger,
		MaxEventSize: 1024 * 1024, // 1MiB
		Semaphore:    semaphore.NewWeighted(1),
	})
	baseEvent := modelpb.APMEvent{
		Event: &modelpb.Event{
			Received: timestamppb.Now(),
		},
	}
	var batch modelpb.Batch
	processor := modelprocessor.Chained{
		modelprocessor.SetHostHostname{},
		modelprocessor.SetServiceNodeName{},
		modelprocessor.SetGroupingKey{},
		modelprocessor.SetErrorMessage{},
		modelpb.ProcessBatchFunc(func(ctx context.Context, b *modelpb.Batch) error {
			batch = make(modelpb.Batch, len(*b))
			copy(batch, *b)
			return nil
		}),
	}

	var elasticapmResult elasticapm.Result
	if err := elasticapmProcessor.HandleStream(
		context.TODO(),
		false, // async
		&baseEvent,
		reader,
		math.MaxInt32, // batch size
		processor,
		&elasticapmResult,
	); err != nil {
		logger.Error("stream error", zap.Error(err))
	}
	return &batch
}

func BenchmarkNDJSON(b *testing.B) {
	for _, tc := range []struct {
		name     string
		filename string
	}{
		{
			name:     "AgentNodeJS",
			filename: "nodejs-3.29.0.ndjson",
		},
		{
			name:     "AgentPython",
			filename: "python-6.7.2.ndjson",
		},
		{
			name:     "AgentRuby",
			filename: "ruby-4.5.0.ndjson",
		},
		{
			name:     "AgentGo",
			filename: "go-2.0.0.ndjson",
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dirFS := os.DirFS("testdata")
			f, err := dirFS.Open(tc.filename)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()

			var batch *modelpb.Batch
			batch = ndjsonFileToBatch(bufio.NewReader(f))

			agg := newTestAggregator(b)
			cmID := EncodeToCombinedMetricsKeyID(b, "ab01")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := agg.AggregateBatch(context.Background(), cmID, batch); err != nil {
					b.Fatal(err)
				}
			}
			flushTestAggregator(b, agg)
		})
	}

}
