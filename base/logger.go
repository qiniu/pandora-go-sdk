package base

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type LogLevelType uint

func LogLevel(l LogLevelType) LogLevelType {
	return l
}

func (l LogLevelType) AtMost(v LogLevelType) bool {
	return l <= v
}

const (
	LogDebug LogLevelType = iota
	LogInfo
	LogWarn
	LogError
	LogPanic
	LogFatal
	LogOff
)

type Logger interface {
	SetLoggerLevel(LogLevelType)

	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warn(v ...interface{})
	Warnf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

func SetLogger(l Logger) { pipelineLogger = l }

var (
	defaultLogger  = &DefaultLogger{Logger: log.New(os.Stderr, "Pandora ", log.LstdFlags)}
	discardLogger  = &DefaultLogger{Logger: log.New(ioutil.Discard, "", 0)}
	pipelineLogger = Logger(defaultLogger)
)

const (
	calldepth = 2
)

func NewDefaultLogger() *DefaultLogger {
	return defaultLogger
}

type DefaultLogger struct {
	*log.Logger
	level LogLevelType
}

func (l *DefaultLogger) SetLoggerLevel(level LogLevelType) {
	l.level = level
}

func (l *DefaultLogger) EnableTimestamps() {
	l.SetFlags(l.Flags() | log.Ldate | log.Ltime)
}

func (l *DefaultLogger) Debug(v ...interface{}) {
	if l.level.AtMost(LogDebug) {
		l.Output(calldepth, header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.level.AtMost(LogDebug) {
		l.Output(calldepth, header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Info(v ...interface{}) {
	if l.level.AtMost(LogInfo) {
		l.Output(calldepth, header("INFO", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	if l.level.AtMost(LogInfo) {
		l.Output(calldepth, header("INFO", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Warn(v ...interface{}) {
	if l.level.AtMost(LogWarn) {
		l.Output(calldepth, header("WARN", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Warnf(format string, v ...interface{}) {
	if l.level.AtMost(LogWarn) {
		l.Output(calldepth, header("WARN", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Error(v ...interface{}) {
	if l.level.AtMost(LogError) {
		l.Output(calldepth, header("ERROR", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	if l.level.AtMost(LogError) {
		l.Output(calldepth, header("ERROR", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Panic(v ...interface{}) {
	if l.level.AtMost(LogPanic) {
		l.Logger.Panic(v)
	}
}

func (l *DefaultLogger) Panicf(format string, v ...interface{}) {
	if l.level.AtMost(LogPanic) {
		l.Logger.Panicf(format, v...)
	}
}

func (l *DefaultLogger) Fatal(v ...interface{}) {
	if l.level.AtMost(LogFatal) {
		l.Output(calldepth, header("FATAL", fmt.Sprint(v...)))
		os.Exit(1)
	}
}

func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	if l.level.AtMost(LogFatal) {
		l.Output(calldepth, header("FATAL", fmt.Sprintf(format, v...)))
		os.Exit(1)
	}
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}
