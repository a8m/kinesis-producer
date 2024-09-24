package producer

import (
	"context"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// Constants and default configuration take from:
// github.com/awslabs/amazon-kinesis-producer/.../KinesisProducerConfiguration.java
const (
	maxRecordSize          = 1 << 20 // 1MiB
	maxRequestSize         = 5 << 20 // 5MiB
	maxRecordsPerRequest   = 500
	maxAggregationSize     = 1048576 // 1MiB
	maxAggregationCount    = 4294967295
	defaultAggregationSize = 51200 // 50k
	defaultMaxConnections  = 24
	defaultFlushInterval   = 5 * time.Second
	partitionKeyIndexSize  = 8
)

// Putter is the interface that wraps the KinesisAPI.PutRecords method.
type Putter interface {
	PutRecords(context.Context, *k.PutRecordsInput, ...func(*k.Options)) (*k.PutRecordsOutput, error)
}

// Config is the Producer configuration.
type Config struct {

	// StreamARN is the ARN of the stream.
	StreamARN *string

	// StreamName is the Kinesis stream.
	StreamName *string

	// FlushInterval is a regular interval for flushing the buffer. Defaults to 5s.
	FlushInterval time.Duration

	// BatchCount determine the maximum number of items to pack in batch.
	// Must not exceed length. Defaults to 500.
	BatchCount int

	// BatchSize determine the maximum number of bytes to send with a PutRecords request.
	// Must not exceed 5MiB; Default to 5MiB.
	BatchSize int

	// AggregateBatchCount determine the maximum number of items to pack into an aggregated record.
	AggregateBatchCount int

	// AggregationBatchSize determine the maximum number of bytes to pack into an aggregated record. User records larger
	// than this will bypass aggregation.
	AggregateBatchSize int

	// BacklogCount determines the channel capacity before Put() will begin blocking. Default to `BatchCount`.
	BacklogCount int

	// Number of requests to sent concurrently. Default to 24.
	MaxConnections int

	// Logger is the logger used. Default to producer.Logger.
	Logger Logger

	// Enabling verbose logging. Default to false.
	Verbose bool

	// Client is the Putter interface implementation.
	Client Putter
}

// defaults for configuration
func (c *Config) defaults() {
	if c.Logger == nil {
		c.Logger = &StdLogger{log.New(os.Stdout, "", log.LstdFlags)}
	}
	if c.BatchCount == 0 {
		c.BatchCount = maxRecordsPerRequest
	}
	falseOrPanic(c.BatchCount > maxRecordsPerRequest, "kinesis: BatchCount exceeds 500")
	if c.BatchSize == 0 {
		c.BatchSize = maxRequestSize
	}
	falseOrPanic(c.BatchSize > maxRequestSize, "kinesis: BatchSize exceeds 5MiB")
	if c.BacklogCount == 0 {
		c.BacklogCount = maxRecordsPerRequest
	}
	if c.AggregateBatchCount == 0 {
		c.AggregateBatchCount = maxAggregationCount
	}
	falseOrPanic(c.AggregateBatchCount > maxAggregationCount, "kinesis: AggregateBatchCount exceeds 4294967295")
	if c.AggregateBatchSize == 0 {
		c.AggregateBatchSize = defaultAggregationSize
	}
	falseOrPanic(c.AggregateBatchSize > maxAggregationSize, "kinesis: AggregateBatchSize exceeds 50KB")
	if c.MaxConnections == 0 {
		c.MaxConnections = defaultMaxConnections
	}
	falseOrPanic(c.MaxConnections < 1 || c.MaxConnections > 256, "kinesis: MaxConnections must be between 1 and 256")
	if c.FlushInterval == 0 {
		c.FlushInterval = defaultFlushInterval
	}
	if c.StreamARN != nil {
		// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html#Streams-PutRecords-request-StreamARN
		if !matchRegex(`^arn:aws.*:kinesis:.*:\d{12}:stream/\S+$`, aws.ToString(c.StreamARN)) {
			panic(`kinesis: StreamARN must match pattern "arn:aws.*:kinesis:.*:\d{12}:stream/\S+"` + " current StreamARN: " + aws.ToString(c.StreamARN))
		}
		if c.StreamName != nil && !strings.HasSuffix(aws.ToString(c.StreamARN), "/"+aws.ToString(c.StreamName)) {
			panic(`kinesis: StreamName must match the StreamArn` + " current StreamARN: " + aws.ToString(c.StreamARN) + " current StreamName: " + aws.ToString(c.StreamName))
		}
	} else if c.StreamName != nil {
		// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html#Streams-PutRecords-request-StreamName
		if !matchRegex(`^[a-zA-Z0-9_.-]+$`, aws.ToString(c.StreamName)) {
			panic(`kinesis: StreamName must match pattern "[a-zA-Z0-9_.-]+"` + " current StreamName: " + aws.ToString(c.StreamName))
		}
	} else {
		// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html#API_PutRecords
		panic("kinesis: either StreamARN or StreamName must be set, recommended use StreamARN")
	}
}

func falseOrPanic(p bool, msg string) {
	if p {
		panic(msg)
	}
}

func matchRegex(pattern string, str string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	match := re.MatchString(str)
	return match
}
