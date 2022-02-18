package producer

import (
	"fmt"
	"log"
	"strings"
)

// Logger represents a simple interface used by kinesis-producer to handle logging
type Logger interface {
	Info(msg string, values ...LogValue)
	Error(msg string, err error, values ...LogValue)
}

// LogValue represents a key:value pair used by the Logger interface
type LogValue struct {
	Name  string
	Value interface{}
}

func (v LogValue) String() string {
	return fmt.Sprintf(" %s=%v", v.Name, v.Value)
}

// StdLogger implements the Logger interface using standard library loggers
type StdLogger struct {
	Logger *log.Logger
}

// Info prints log message
func (l *StdLogger) Info(msg string, values ...LogValue) {
	l.Logger.Print(msg, l.valuesToString(values...))
}

// Error prints log message
func (l *StdLogger) Error(msg string, err error, values ...LogValue) {
	l.Logger.Print(msg, l.valuesToString(values...), err)
}

func (l *StdLogger) valuesToString(values ...LogValue) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ", ")
}
