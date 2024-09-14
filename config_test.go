package producer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestConfig_ValidStreamArn(t *testing.T) {
	c := &Config{
		StreamARN: aws.String("arn:aws:kinesis:us-east-1:012345678901:stream/test-stream-name"),
	}
	c.defaults()
	t.Logf("TestConfig_ValidStreamArn success")
}

func TestConfig_InvalidStreamArn(t *testing.T) {
	c := &Config{
		StreamARN: aws.String("test-stream-name"),
	}
	defer func() {
		r := recover()
		if r != `kinesis: StreamARN must match pattern "arn:aws.*:kinesis:.*:\d{12}:stream/\S+"`+" current StreamARN: test-stream-name" {
			t.Errorf("unexpected panic: %v", r)
		} else {
			t.Logf("TestConfig_InvalidStreamArn success")
		}
	}()
	c.defaults()
}

func TestConfig_ValidStreamName(t *testing.T) {
	c := &Config{
		StreamName: aws.String("test-stream-name"),
	}
	c.defaults()
	t.Logf("TestConfig_ValidStreamName success")
}

func TestConfig_InvalidStreamName(t *testing.T) {
	c := &Config{
		StreamName: aws.String("test`-stream/name"),
	}
	defer func() {
		r := recover()
		if r != `kinesis: StreamName must match pattern "[a-zA-Z0-9_.-]+"`+" current StreamName: test`-stream/name" {
			t.Errorf("unexpected panic: %v", r)
		} else {
			t.Logf("TestConfig_InvalidStreamName success")
		}
	}()
	c.defaults()
}

func TestConfig_BothStreamArnAndName(t *testing.T) {
	c := &Config{
		StreamARN:  aws.String("arn:aws:kinesis:us-east-1:012345678901:stream/test-stream-name"),
		StreamName: aws.String("test-stream-name"),
	}
	c.defaults()
	t.Logf("TestConfig_BothStreamArnAndName success")
}

func TestConfig_EmptyStreamArnAndName(t *testing.T) {
	c := &Config{}
	defer func() {
		r := recover()
		if r != `kinesis: either StreamARN or StreamName must be set, recommended use StreamARN` {
			t.Errorf("unexpected panic: %v", r)
		} else {
			t.Logf("TestConfig_EmptyStreamArnAndName success")
		}
	}()
	c.defaults()
}

func TestConfig_NoMatchStreamArnAndName(t *testing.T) {
	c := &Config{
		StreamARN:  aws.String("arn:aws:kinesis:us-east-1:012345678901:stream/test-stream-name"),
		StreamName: aws.String("test-stream-name-2"),
	}
	defer func() {
		r := recover()
		if r != `kinesis: StreamName must match the StreamArn current StreamARN: arn:aws:kinesis:us-east-1:012345678901:stream/test-stream-name current StreamName: test-stream-name-2` {
			t.Errorf("unexpected panic: %v", r)
		} else {
			t.Logf("TestConfig_NoMatchStreamArnAndName success")
		}
	}()
	c.defaults()
}
