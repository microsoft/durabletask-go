package api

import (
	"fmt"
	"log"
	"log/slog"
)

// Logger receives diagnostic messages from DTS clients and workers.
type Logger interface {
	// Debug logs a message at level Debug.
	Debug(v ...any)
	// Debugf logs a message at level Debug.
	Debugf(format string, v ...any)
	// Info logs a message at level Info.
	Info(v ...any)
	// Infof logs a message at level Info.
	Infof(format string, v ...any)
	// Warn logs a message at level Warn.
	Warn(v ...any)
	// Warnf logs a message at level Warn.
	Warnf(format string, v ...any)
	// Error logs a message at level Error.
	Error(v ...any)
	// Errorf logs a message at level Error.
	Errorf(format string, v ...any)
}

type logger struct {
	debugLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger
}

var defaultLogger = &logger{
	infoLogger:    log.New(log.Writer(), "INFO: ", log.Flags()),
	warningLogger: log.New(log.Writer(), "WARNING: ", log.Flags()),
	errorLogger:   log.New(log.Writer(), "ERROR: ", log.Flags()),
	debugLogger:   log.New(log.Writer(), "DEBUG: ", log.Flags()),
}

func (l *logger) Debug(v ...any) {
	l.debugLogger.Print(v...)
}

func (l *logger) Debugf(format string, v ...any) {
	l.debugLogger.Printf(format, v...)
}

func (l *logger) Error(v ...any) {
	l.errorLogger.Print(v...)
}

func (l *logger) Errorf(format string, v ...any) {
	l.errorLogger.Printf(format, v...)
}

func (l *logger) Info(v ...any) {
	l.infoLogger.Print(v...)
}

func (l *logger) Infof(format string, v ...any) {
	l.infoLogger.Printf(format, v...)
}

func (l *logger) Warn(v ...any) {
	l.warningLogger.Print(v...)
}

func (l *logger) Warnf(format string, v ...any) {
	l.warningLogger.Printf(format, v...)
}

// DefaultLogger returns the process-wide logger used when a caller does not
// supply one.
func DefaultLogger() Logger {
	return defaultLogger
}

// slogLogger adapts a *slog.Logger to the Logger interface.
type slogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a Logger that delegates to the provided *slog.Logger.
// This allows consumers to integrate durabletask logging with their existing
// slog-based logging infrastructure.
func NewSlogLogger(l *slog.Logger) Logger {
	return &slogLogger{logger: l}
}

func (s *slogLogger) Debug(v ...any) {
	s.logger.Debug(fmt.Sprint(v...))
}

func (s *slogLogger) Debugf(format string, v ...any) {
	s.logger.Debug(fmt.Sprintf(format, v...))
}

func (s *slogLogger) Info(v ...any) {
	s.logger.Info(fmt.Sprint(v...))
}

func (s *slogLogger) Infof(format string, v ...any) {
	s.logger.Info(fmt.Sprintf(format, v...))
}

func (s *slogLogger) Warn(v ...any) {
	s.logger.Warn(fmt.Sprint(v...))
}

func (s *slogLogger) Warnf(format string, v ...any) {
	s.logger.Warn(fmt.Sprintf(format, v...))
}

func (s *slogLogger) Error(v ...any) {
	s.logger.Error(fmt.Sprint(v...))
}

func (s *slogLogger) Errorf(format string, v ...any) {
	s.logger.Error(fmt.Sprintf(format, v...))
}
