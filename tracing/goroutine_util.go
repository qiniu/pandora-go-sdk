package tracing

import (
	"github.com/petermattis/goid"
)

// current go version in our compiling environment is 1.10+.
// the g structure in go1.10+ is the same as go1.9, meanwhile go1.9 is supported by petermattis/goid.
// in the future, if the g structure changes as go updates, use goid.ExtractGID() instead of goid.Get()
// or we can submit pr to github.com/petermattis/goid to support higher go versions.
func CurGoroutineID() uint64 {
	return uint64(goid.Get())
}