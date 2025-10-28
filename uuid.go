package fluxaorm

import (
	"sync/atomic"
	"time"
)

var codeStartTime = uint64(time.Now().Unix())
var uuidServerID = uint64(0)
var uuidCounter = uint64(0)

func uuid() uint64 {
	return (uint64(uuidServerID)&255)<<56 + (codeStartTime << 24) + atomic.AddUint64(&uuidCounter, 1)
}
