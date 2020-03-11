package kplogrus

import (
	producer "github.com/ouzi-dev/kinesis-producer"
	"github.com/sirupsen/logrus"
)

// Logger implements a logurs.Logger logger for kinesis-producer
type Logger struct {
	Logger *logrus.Logger
}

// Info logs a message
func (l *Logger) Info(msg string, args ...producer.LogValue) {
	l.Logger.WithFields(l.valuesToFields(args...)).Info(msg)
}

// Error logs an error
func (l *Logger) Error(msg string, err error, args ...producer.LogValue) {
	l.Logger.WithError(err).WithFields(l.valuesToFields(args...)).Error(msg)
}

func (l *Logger) valuesToFields(values ...producer.LogValue) logrus.Fields {
	fields := logrus.Fields{}
	for _, v := range values {
		fields[v.Name] = v.Value
	}
	return fields
}
