package loggers

import (
	"go.uber.org/zap"

	producer "github.com/a8m/kinesis-producer"
)

// Zap implements a logurs.Logger for kinesis-producer
type Zap struct {
	Logger *zap.Logger
}

// Info logs a message
func (l *Zap) Info(msg string, values ...producer.LogValue) {
	l.Logger.Info(msg, l.valuesToFields(values)...)
}

// Error logs an error
func (l *Zap) Error(msg string, err error, values ...producer.LogValue) {
	fields := l.valuesToFields(values)
	fields = append(fields, zap.Error(err))
	l.Logger.Info(msg, fields...)
}

func (l *Zap) valuesToFields(values []producer.LogValue) []zap.Field {
	fields := make([]zap.Field, len(values))
	for i, v := range values {
		fields[i] = zap.Any(v.Name, v.Value)
	}
	return fields
}
