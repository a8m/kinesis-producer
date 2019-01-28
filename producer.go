// Amazon kinesis producer
// A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK
// and using the same aggregation format that KPL use.
//
// Note: this project start as a fork of `tj/go-kinesis`. if you are not intersting in the
// KPL aggregation logic, you probably want to check it out.
package producer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jpillora/backoff"
)

// Errors
var (
	ErrStoppedProducer     = errors.New("Unable to Put record. Producer is already stopped")
	ErrIllegalPartitionKey = errors.New("Invalid parition key. Length must be at least 1 and at most 256")
	ErrRecordSizeExceeded  = errors.New("Data must be less than or equal to 1MB in size")
)

// Flush reasons
var (
	FlushReasonBatchSize   = "batch_size"
	FlushReasonBatchLength = "batch_length"
	FlushReasonDrain       = "drain"
	FlushReasonInterval    = "interval"
	FlushReasonRetry       = "interval"
)

// Producer batches records.
type Producer struct {
	sync.RWMutex
	*Config
	aggregator *Aggregator
	semaphore  semaphore
	batches    chan *AggregatedBatch
	failure    chan *FailureRecord
	done       chan struct{}
	hooks      Hooks

	// Current state of the Producer
	// notify set to true after calling to `NotifyFailures`
	notify bool
	// stopped set to true after `Stop`ing the Producer.
	// This will prevent from user to `Put` any new data.
	stopped bool
}

// New creates new producer with the given config.
func New(config *Config, hooks Hooks) *Producer {
	config.defaults()
	if hooks == nil {
		hooks = &noopHooks{}
	}
	return &Producer{
		Config:     config,
		done:       make(chan struct{}),
		hooks:      hooks,
		batches:    make(chan *AggregatedBatch, config.BacklogCount),
		semaphore:  make(chan struct{}, config.MaxConnections),
		aggregator: new(Aggregator),
	}
}

// Put `data` using `partitionKey` asynchronously. This method is thread-safe.
//
// Under the covers, the Producer will automatically re-attempt puts in case of
// transient errors.
// When unrecoverable error has detected(e.g: trying to put to in a stream that
// doesn't exist), the message will returned by the Producer.
// Add a listener with `Producer.NotifyFailures` to handle undeliverable messages.
func (p *Producer) Put(data []byte, partitionKey string) error {
	p.RLock()
	stopped := p.stopped
	p.RUnlock()
	if stopped {
		return ErrStoppedProducer
	}
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}
	if l := len(partitionKey); l < 1 || l > 256 {
		return ErrIllegalPartitionKey
	}
	nbytes := len(data) + len([]byte(partitionKey))
	// if the record size is bigger than aggregation size
	// handle it as a simple kinesis record
	if nbytes > p.AggregateBatchSize {
		p.batches <- &AggregatedBatch{
			Records: &kinesis.PutRecordsRequestEntry{
				Data:         data,
				PartitionKey: &partitionKey,
			},
			Count: 1,
			Size:  nbytes,
		}
	} else {
		p.RLock()
		needToDrain := nbytes+p.aggregator.Size() > p.AggregateBatchSize || p.aggregator.Count() >= p.AggregateBatchCount
		p.RUnlock()
		var (
			batch *AggregatedBatch
			err   error
		)
		p.Lock()
		if needToDrain {
			if batch, err = p.aggregator.Drain(); err != nil {
				p.Logger.Error("drain aggregator", err)
			}
		}
		p.aggregator.Put(data, partitionKey)
		p.Unlock()
		// release the lock and then pipe the record to the records channel
		// we did it, because the "send" operation blocks when the backlog is full
		// and this can cause deadlock(when we never release the lock)
		if needToDrain && batch != nil {
			p.batches <- batch
		}
	}
	return nil
}

// Failure record type
type FailureRecord struct {
	error
	Data         []byte
	PartitionKey string
}

// NotifyFailures registers and return listener to handle undeliverable messages.
// The incoming struct has a copy of the Data and the PartitionKey along with some
// error information about why the publishing failed.
func (p *Producer) NotifyFailures() <-chan *FailureRecord {
	p.Lock()
	defer p.Unlock()
	if !p.notify {
		p.notify = true
		p.failure = make(chan *FailureRecord, p.BacklogCount)
	}
	return p.failure
}

// Start the producer
func (p *Producer) Start() {
	p.Logger.Info("starting producer", LogValue{"stream", p.StreamName})
	go p.loop()
}

// Stop the producer gracefully. Flushes any in-flight data.
func (p *Producer) Stop() {
	p.Lock()
	p.stopped = true
	p.Unlock()
	p.Logger.Info("stopping producer", LogValue{"backlog batches", len(p.batches)})

	// drain
	if batch, ok := p.drainIfNeed(); ok {
		p.batches <- batch
	}
	p.done <- struct{}{}
	close(p.batches)

	// wait
	<-p.done
	p.semaphore.wait()

	// close the failures channel if we notify
	p.RLock()
	if p.notify {
		close(p.failure)
	}
	p.RUnlock()
	p.Logger.Info("stopped producer")
}

// loop and flush at the configured interval, or when the buffer is exceeded.
func (p *Producer) loop() {
	size := 0
	count := 0
	drain := false
	buf := make([]*kinesis.PutRecordsRequestEntry, 0, p.BatchCount)
	tick := time.NewTicker(p.FlushInterval)

	flush := func(msg string) {
		p.semaphore.acquire()
		go p.flush(buf, count, msg)
		buf = nil
		size = 0
		count = 0
	}

	batchAppend := func(batch *AggregatedBatch) {
		// the record size limit applies to the total size of the
		// partition key and data blob.
		p.hooks.OnDrain(int64(batch.Size), int64(batch.Count))

		if size+batch.Size > p.BatchSize {
			flush(FlushReasonBatchSize)
		}
		size += batch.Size
		count += batch.Count
		buf = append(buf, batch.Records)
		if len(buf) >= p.BatchCount {
			flush(FlushReasonBatchLength)
		}
	}

	defer tick.Stop()
	defer close(p.done)

	for {
		select {
		case batch, ok := <-p.batches:
			if drain && !ok {
				if size > 0 {
					flush(FlushReasonDrain)
				}
				p.Logger.Info("backlog drained")
				return
			}
			batchAppend(batch)
		case <-tick.C:
			if batch, ok := p.drainIfNeed(); ok {
				batchAppend(batch)
			}
			// if the buffer is still containing records
			if size > 0 {
				flush(FlushReasonInterval)
			}
		case <-p.done:
			drain = true
		}
	}
}

func (p *Producer) drainIfNeed() (*AggregatedBatch, bool) {
	p.RLock()
	needToDrain := p.aggregator.Size() > 0
	p.RUnlock()
	if needToDrain {
		p.Lock()
		batch, err := p.aggregator.Drain()
		p.Unlock()
		if err != nil {
			p.Logger.Error("drain aggregator", err)
		} else {
			return batch, true
		}
	}
	return nil, false
}

// flush records and retry failures if necessary.
// for example: when we get "ProvisionedThroughputExceededException"
func (p *Producer) flush(records []*kinesis.PutRecordsRequestEntry, count int, reason string) {
	b := &backoff.Backoff{
		Jitter: true,
	}

	defer p.semaphore.release()

	for {
		startTime := time.Now()
		p.Logger.Info("flushing records", LogValue{"reason", reason}, LogValue{"records", len(records)})
		out, err := p.Client.PutRecords(&kinesis.PutRecordsInput{
			StreamName: &p.StreamName,
			Records:    records,
		})

		if err != nil {
			p.Logger.Error("flush", err)
			p.RLock()
			notify := p.notify
			p.RUnlock()
			if notify {
				p.dispatchFailures(records, err)
			}
			return
		}

		putLatencyMs := int64(time.Since(startTime) / time.Millisecond)
		p.hooks.OnPutRecords(int64(len(records)), int64(count), putLatencyMs, reason)

		for i, r := range out.Records {
			if r.ErrorCode != nil {
				p.hooks.OnPutErr(*r.ErrorCode)
				p.Logger.Info(
					fmt.Sprintf("Result[%d]", i),
					LogValue{"ErrorCode", *r.ErrorCode},
					LogValue{"ErrorMessage", *r.ErrorMessage},
				)
			} else if p.Verbose {
				p.Logger.Info(
					fmt.Sprintf("Result[%d]", i),
					LogValue{"ShardId", *r.ShardId},
					LogValue{"SequenceNumber", *r.SequenceNumber},
				)
			}
		}

		failed := *out.FailedRecordCount
		if failed == 0 {
			return
		}

		duration := b.Duration()

		p.Logger.Info(
			"put failures",
			LogValue{"failures", failed},
			LogValue{"backoff", duration.String()},
		)
		time.Sleep(duration)

		// change the logging state for the next itertion
		reason = FlushReasonRetry
		records = failures(records, out.Records)
	}
}

// dispatchFailures gets batch of records, extract them, and push them
// into the failure channel
func (p *Producer) dispatchFailures(records []*kinesis.PutRecordsRequestEntry, err error) {
	for _, r := range records {
		if isAggregated(r) {
			p.dispatchFailures(extractRecords(r), err)
		} else {
			p.failure <- &FailureRecord{err, r.Data, *r.PartitionKey}
		}
	}
}

// failures returns the failed records as indicated in the response.
func failures(records []*kinesis.PutRecordsRequestEntry,
	response []*kinesis.PutRecordsResultEntry) (out []*kinesis.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}
