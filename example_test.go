package producer

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/a8m/kinesis.producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func ExampleSimple() {
	log := logrus.New()
	client := k.New(session.New(aws.NewConfig()))
	pr := producer.New(producer.Config{
		StreamName:  "test",
		BacklogSize: 2000,
		Client:      client,
		Logger:      log,
	})

	pr.Start()

	// Handle failures
	go func() {
		for r := range pr.NotifyFailure() {
			// r contains `Data`, `PartitionKey` and `Error()`
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			err := pr.Put("foo", "bar")
			if err != nil {
				log.WithError(err).Fatal("error producing")
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
