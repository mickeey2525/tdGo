package tdGo

import (
	"fmt"
	"log"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

func (l LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL", "PANIC"}[l]
}

type LevelLogger struct {
	*log.Logger
	level LogLevel
}

func NewLevelLogger(level LogLevel, logger *log.Logger) *LevelLogger {
	return &LevelLogger{
		Logger: logger,
		level:  level,
	}
}

func (l *LevelLogger) log(level LogLevel, args ...interface{}) {
	if level >= l.level {
		err := l.Output(3, fmt.Sprintf("[%s] %s", level, fmt.Sprint(args...)))
		if err != nil {
			return
		}
	}
}

func (l *LevelLogger) logf(level LogLevel, format string, args ...interface{}) {
	if level >= l.level {
		err := l.Output(3, fmt.Sprintf("[%s] %s", level, fmt.Sprintf(format, args...)))
		if err != nil {
			return
		}
	}
}

// Implement the Logger interface with level checks
func (l *LevelLogger) Debug(v ...interface{}) {
	l.log(DEBUG, v...)
}

func (l *LevelLogger) Debugf(format string, v ...interface{}) {
	l.logf(DEBUG, format, v...)
}

func (l *LevelLogger) Info(v ...interface{}) {
	l.log(INFO, v...)
}

func (l *LevelLogger) Infof(format string, v ...interface{}) {
	l.logf(INFO, format, v...)
}

func (l *LevelLogger) Warn(v ...interface{}) {
	l.log(WARNING, v...)
}

func (l *LevelLogger) Warnf(format string, v ...interface{}) {
	l.logf(WARNING, format, v...)
}

func (l *LevelLogger) Error(v ...interface{}) {
	l.log(ERROR, v...)
}

func (l *LevelLogger) Errorf(format string, v ...interface{}) {
	l.logf(ERROR, format, v...)
}

func (l *LevelLogger) Fatal(v ...interface{}) {
	l.log(FATAL, v...)
}

func (l *LevelLogger) Fatalf(format string, v ...interface{}) {
	l.logf(FATAL, format, v...)
}
