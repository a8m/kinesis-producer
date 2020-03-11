package kpzap

import (
	"go.uber.org/zap"

	producer "github.com/ouzi-dev/kinesis-producer"
)

// Logger implements a zap.Logger logger for kinesis-producer
type Logger struct {
	Logger *zap.Logger
}

// Info logs a message
func (l *Logger) Info(msg string, values ...producer.LogValue) {
	l.Logger.Info(msg, l.valuesToFields(values)...)
}

// Error logs an error
func (l *Logger) Error(msg string, err error, values ...producer.LogValue) {
	fields := l.valuesToFields(values)
	fields = append(fields, zap.Error(err))
	l.Logger.Info(msg, fields...)
}

func (l *Logger) valuesToFields(values []producer.LogValue) []zap.Field {
	fields := make([]zap.Field, len(values))
	for i, v := range values {
		fields[i] = zap.Any(v.Name, v.Value)
	}
	return fields
}
