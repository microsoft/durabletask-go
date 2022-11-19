package backend

import "log"

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

// Debug implements Logger
func (log *logger) Debug(v ...any) {
	log.debugLogger.Print(v...)
}

// Debugf implements Logger
func (log *logger) Debugf(format string, v ...any) {
	log.debugLogger.Printf(format, v...)
}

// Error implements Logger
func (log *logger) Error(v ...any) {
	log.errorLogger.Print(v...)
}

// Errorf implements Logger
func (log *logger) Errorf(format string, v ...any) {
	log.errorLogger.Printf(format, v...)
}

// Info implements Logger
func (log *logger) Info(v ...any) {
	log.infoLogger.Print(v...)
}

// Infof implements Logger
func (log *logger) Infof(format string, v ...any) {
	log.infoLogger.Printf(format, v...)
}

// Warn implements Logger
func (log *logger) Warn(v ...any) {
	log.warningLogger.Print(v...)
}

// Warnf implements Logger
func (log *logger) Warnf(format string, v ...any) {
	log.warningLogger.Printf(format, v...)
}

func DefaultLogger() Logger {
	return defaultLogger
}
