package producer

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	k "github.com/aws/aws-sdk-go/service/kinesis"
)

func ExampleSimple() {
	log := logrus.New()
	client := k.New(session.New(aws.NewConfig()))
	pr := New(&Config{
		StreamName:   "test",
		BacklogCount: 2000,
		Client:       client,
		Logger:       log,
	})

	pr.Start()

	// Handle failures
	go func() {
		for _ = range pr.NotifyFailure() {
			// r contains `Data`, `PartitionKey` and `Error()`
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			err := pr.Put([]byte("foo"), "bar")
			if err != nil {
				log.WithError(err).Fatal("error producing")
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
