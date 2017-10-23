package base

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestDefaultLogger(t *testing.T) {
	log := NewDefaultLogger()
	for level := 0; level < 6; level++ {
		log.SetLoggerLevel(LogLevelType(level))
		assert.Equal(t, LogLevelType(level), log.LogLevel())

		log.Debug("aaa")
		log.Info("aaa")
		log.Warn("aaa")
		log.Error("aaa")
	}
}
