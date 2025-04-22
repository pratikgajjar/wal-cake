// Package logger provides a structured logging implementation using zerolog with logfmt format
package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Logger is a wrapper around zerolog.Logger that provides a consistent logging interface
type Logger struct {
	logger zerolog.Logger
}

// New creates a new Logger instance with logfmt format
func New(serviceName string) *Logger {
	return NewWithOutput(serviceName, os.Stdout)
}

// NewWithOutput creates a new Logger with a custom output
func NewWithOutput(serviceName string, w io.Writer) *Logger {
	// Configure zerolog
	zerolog.TimeFieldFormat = time.RFC3339
	
	// Use logfmt format as required by user rules
	output := zerolog.ConsoleWriter{
		Out:        w,
		TimeFormat: time.RFC3339,
		NoColor:    true,
		FormatLevel: func(i interface{}) string {
			return "level=" + fmt.Sprintf("%v", i)
		},
		FormatTimestamp: func(i interface{}) string {
			return "time=" + i.(string)
		},
		FormatMessage: func(i interface{}) string {
			if i == nil {
				return ""
			}
			return "msg=" + fmt.Sprintf("%v", i)
		},
		FormatFieldName: func(i interface{}) string {
			return fmt.Sprintf("%v", i) + "="
		},
		FormatFieldValue: func(i interface{}) string {
			switch v := i.(type) {
			case string:
				return v
			case json.Number:
				return v.String()
			case nil:
				return "null"
			default:
				return fmt.Sprintf("%v", v)
			}
		},
	}

	logger := zerolog.New(output).
		With().
		Timestamp().
		Str("service", serviceName).
		Logger()

	return &Logger{
		logger: logger,
	}
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, fields ...map[string]interface{}) {
	event := l.logger.Debug()
	for _, field := range fields {
		for k, v := range field {
			event = event.Interface(k, v)
		}
	}
	event.Msg(msg)
}

// Info logs an info message
func (l *Logger) Info(msg string, fields ...map[string]interface{}) {
	event := l.logger.Info()
	for _, field := range fields {
		for k, v := range field {
			event = event.Interface(k, v)
		}
	}
	event.Msg(msg)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, fields ...map[string]interface{}) {
	event := l.logger.Warn()
	for _, field := range fields {
		for k, v := range field {
			event = event.Interface(k, v)
		}
	}
	event.Msg(msg)
}

// Error logs an error message
func (l *Logger) Error(msg string, err error, fields ...map[string]interface{}) {
	event := l.logger.Error()
	if err != nil {
		event = event.Err(err)
	}
	for _, field := range fields {
		for k, v := range field {
			event = event.Interface(k, v)
		}
	}
	event.Msg(msg)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string, err error, fields ...map[string]interface{}) {
	event := l.logger.Fatal()
	if err != nil {
		event = event.Err(err)
	}
	for _, field := range fields {
		for k, v := range field {
			event = event.Interface(k, v)
		}
	}
	event.Msg(msg)
}

// With returns a new Logger with the given fields added
func (l *Logger) With(fields map[string]interface{}) *Logger {
	loggerWith := l.logger.With()
	for k, v := range fields {
		loggerWith = loggerWith.Interface(k, v)
	}
	return &Logger{
		logger: loggerWith.Logger(),
	}
}
