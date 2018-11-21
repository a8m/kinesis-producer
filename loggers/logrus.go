package loggers

import (
	producer "github.com/a8m/kinesis-producer"
	"github.com/sirupsen/logrus"
)

// Logrus implements a logurs.Logger for kinesis-producer
type Logrus struct {
	Logger *logrus.Logger
}

// Info logs a message
func (l *Logrus) Info(msg string, args ...producer.LogValue) {
	l.Logger.WithFields(l.valuesToFields(args...)).Info(msg)
}

// Error logs an error
func (l *Logrus) Error(msg string, err error, args ...producer.LogValue) {
	l.Logger.WithError(err).WithFields(l.valuesToFields(args...)).Error(msg)
}

func (l *Logrus) valuesToFields(values ...producer.LogValue) logrus.Fields {
	fields := logrus.Fields{}
	for _, v := range values {
		fields[v.Name] = v.Value
	}
	return fields
}
