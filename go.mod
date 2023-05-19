module github.com/a8m/kinesis-producer

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.18.0
	github.com/aws/aws-sdk-go-v2/config v1.18.25
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.17.12
	github.com/golang/protobuf v1.3.2
	github.com/jpillora/backoff v0.0.0-20180909062703-3050d21c67d7
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.4.2
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/sys v0.0.0-20210423082822-04245dca01da // indirect
)
