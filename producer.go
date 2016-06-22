package producer

import (
	"errors"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	k "github.com/aws/aws-sdk-go/service/kinesis"
)

// Errors
var (
	ErrStoppedProducer     = errors.New("Unable to Put record. Producer is already stopped")
	ErrIllegalPartitionKey = errors.New("Invalid parition key. Length must be at least 1 and at most 256")
	ErrRecordSizeExceeded  = errors.New("Data must be less than or equal to 1MB in size")
)

// Producer batches records.
type Producer struct {
	sync.Mutex
	*Config
	aggregator *Aggregator
	taskPool   *TaskPool
	records    chan *k.PutRecordsRequestEntry
	failure    chan *FailureRecord
	done       chan struct{}

	// Current state of the Producer
	// notify set to true after calling to `NotifyFailures`
	notify bool
	// stopped set to true after `Stop`ing the Producer.
	// This will prevent from user to `Put` any new data.
	stopped bool
}

// New creates new producer with the given config.
func New(config *Config) *Producer {
	config.defaults()
	return &Producer{
		Config:     config,
		records:    make(chan *k.PutRecordsRequestEntry, config.BacklogCount),
		done:       make(chan struct{}),
		aggregator: new(Aggregator),
		taskPool:   newPool(config.MaxConnections),
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
	p.Lock()
	defer p.Unlock()
	if p.stopped {
		return ErrStoppedProducer
	}
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}
	if l := len(partitionKey); l < 1 || l > 256 {
		return ErrIllegalPartitionKey
	}
	nbytes := len(data) + len([]byte(partitionKey))
	switch {
	case nbytes > p.AggregateBatchSize:
		p.records <- &k.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &partitionKey,
		}
	case nbytes+p.aggregator.Size() > p.AggregateBatchSize ||
		p.aggregator.Count() >= p.AggregateBatchCount:
		p.drainIfNeed()
		fallthrough
	default:
		p.aggregator.Put(data, partitionKey)
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
	p.Logger.WithField("stream", p.StreamName).Info("starting producer")
	p.taskPool.Start()
	go p.loop()
}

// Stop the producer gracefully. Flushes any in-flight data.
func (p *Producer) Stop() {
	p.Lock()
	p.stopped = true
	p.Unlock()
	p.Logger.WithField("backlog", len(p.records)).Info("stopping producer")

	// drain
	p.drainIfNeed()
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done
	p.taskPool.Stop()
	p.Logger.Info("stopped producer")
}

// loop and flush at the configured interval, or when the buffer is exceeded.
func (p *Producer) loop() {
	size := 0
	drain := false
	buf := make([]*k.PutRecordsRequestEntry, 0, p.BatchCount)
	tick := time.NewTicker(p.FlushInterval)

	flush := func(msg string) {
		b := buf
		p.taskPool.Put(func() {
			p.flush(b, msg)
		})
		buf = nil
		size = 0
	}

	defer tick.Stop()
	defer close(p.done)
	for {
		select {
		case record, ok := <-p.records:
			if drain && !ok {
				if size > 0 {
					flush("drain")
				}
				p.Logger.Info("backlog drained")
				return
			}
			// The record size limit applies to the total size of the
			// partition key and data blob.
			rsize := len(record.Data) + len([]byte(*record.PartitionKey))
			if size+rsize > p.BatchSize {
				flush("batch size")
			}
			size += rsize
			buf = append(buf, record)
			if len(buf) >= p.BatchCount {
				flush("batch length")
			}
		case <-tick.C:
			p.drainIfNeed()
			if size > 0 {
				flush("interval")
			}
		case <-p.done:
			drain = true
		}
	}
}

func (p *Producer) drainIfNeed() {
	if p.aggregator.Size() > 0 {
		record, err := p.aggregator.Drain()
		if err != nil {
			p.Logger.WithError(err).Error("drain aggregator")
		} else {
			p.records <- record
		}
	}
}

// flush records and retry failures if necessary.
// for example: when we get "ProvisionedThroughputExceededException"
func (p *Producer) flush(records []*k.PutRecordsRequestEntry, reason string) {
	p.Logger.WithField("reason", reason).Infof("flush %v records", len(records))
	out, err := p.Client.PutRecords(&k.PutRecordsInput{
		StreamName: &p.StreamName,
		Records:    records,
	})

	if err != nil {
		p.Logger.WithError(err).Error("flush")
		p.Lock()
		p.Backoff.Reset()
		if p.notify {
			p.dispatchFailures(records, err)
		}
		p.Unlock()
		return
	}

	if p.Verbose {
		for i, r := range out.Records {
			fields := make(logrus.Fields)
			if r.ErrorCode != nil {
				fields["ErrorCode"] = *r.ErrorCode
				fields["ErrorMessage"] = *r.ErrorMessage
			} else {
				fields["ShardId"] = *r.ShardId
				fields["SequenceNumber"] = *r.SequenceNumber
			}
			p.Logger.WithFields(fields).Infof("Result[%d]", i)
		}
	}

	failed := *out.FailedRecordCount
	if failed == 0 {
		p.Lock()
		p.Backoff.Reset()
		p.Unlock()
		return
	}

	// TODO(a8m): we've too many locks here.
	// add thread-safe backoff implementation.
	p.Lock()
	backoff := p.Backoff.Duration()
	p.Unlock()

	p.Logger.WithFields(logrus.Fields{
		"failures": failed,
		"backoff":  backoff,
	}).Warn("put failures")

	time.Sleep(backoff)

	p.flush(failures(records, out.Records), "retry")
}

// dispatchFailures gets batch of records, extract them, and push them
// into the failure channel
func (p *Producer) dispatchFailures(records []*k.PutRecordsRequestEntry, err error) {
	for _, r := range records {
		if isAggregated(r) {
			p.dispatchFailures(extractRecords(r), err)
		} else {
			p.failure <- &FailureRecord{err, r.Data, *r.PartitionKey}
		}
	}
}

// failures returns the failed records as indicated in the response.
func failures(records []*k.PutRecordsRequestEntry, response []*k.PutRecordsResultEntry) (out []*k.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}
