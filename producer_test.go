package producer

import (
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	k "github.com/aws/aws-sdk-go/service/kinesis"
)

type responseMock struct {
	Response *k.PutRecordsOutput
	Error    error
}

type clientMock struct {
	calls     int
	responses []responseMock
	incoming  map[int][]string
}

func (c *clientMock) PutRecords(input *k.PutRecordsInput) (*k.PutRecordsOutput, error) {
	res := c.responses[c.calls]
	for _, r := range input.Records {
		c.incoming[c.calls] = append(c.incoming[c.calls], *r.PartitionKey)
	}
	c.calls++
	if res.Error != nil {
		return nil, res.Error
	}
	return res.Response, nil
}

type testCase struct {
	// configuration
	name    string      // test name
	config  *Config     // test config
	records []string    // all outgoing records(partition keys and data too)
	putter  *clientMock // mocked client

	// expectations
	outgoing map[int][]string // [call number][partition keys]
}

func genBulk(n int, s string) (ret []string) {
	for i := 0; i < n; i++ {
		ret = append(ret, s)
	}
	return
}

var testCases = []testCase{
	{
		"one record with batch count 1",
		&Config{BatchCount: 1},
		[]string{"hello"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello"},
		},
	},
	{
		"two records with batch count 1",
		&Config{BatchCount: 1, AggregateBatchCount: 1},
		[]string{"hello", "world"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello"},
			1: []string{"world"},
		},
	},
	{
		"two records with batch count 2, simulating retries",
		&Config{BatchCount: 2, AggregateBatchCount: 1},
		[]string{"hello", "world"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(1),
						Records: []*k.PutRecordsResultEntry{
							{SequenceNumber: aws.String("3"), ShardId: aws.String("1")},
							{ErrorCode: aws.String("400")},
						},
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello", "world"},
			1: []string{"world"},
		},
	},
	{
		"2 bulks of 10 records",
		&Config{BatchCount: 10, AggregateBatchCount: 2},
		genBulk(20, "foo"),
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int64(0),
					},
				},
			}},
		map[int][]string{
			0: genBulk(10, "foo"),
			1: genBulk(10, "foo"),
		},
	},
}

func TestProducer(t *testing.T) {
	for _, test := range testCases {
		test.config.Client = test.putter
		p := New(test.config)
		p.Start()
		var wg sync.WaitGroup
		wg.Add(len(test.records))
		for _, r := range test.records {
			go func(s string) {
				p.Put([]byte(s), s)
				wg.Done()
			}(r)
		}
		wg.Wait()
		p.Stop()
		for k, v := range test.putter.incoming {
			if len(v) != len(test.outgoing[k]) {
				t.Errorf("failed complete test: %s\n\texcpeted:%v\n\tactual:%v", test.name,
					test.outgoing, test.putter.incoming)
			}
		}
	}
}
