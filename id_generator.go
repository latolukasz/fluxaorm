package fluxaorm

import (
	"sync"
	"time"
)

// Snowflake-style id layout (63 usable bits, always positive in a uint64):
//
//	timestamp (41 bits, ms since idEpochMillis) | node (11 bits) | sequence (11 bits)
//
// IDs are generated in-process with no I/O and never error. Uniqueness across
// processes relies on each running process having a distinct node id (assigned
// once at startup — see Engine.SetNodeID).
const (
	idEpochMillis int64 = 1704067200000 // 2024-01-01T00:00:00Z, fixed forever
	idNodeBits    uint8 = 11
	idSeqBits     uint8 = 11
	idMaxNode     int64 = -1 ^ (-1 << idNodeBits) // 2047
	idMaxSeq      int64 = -1 ^ (-1 << idSeqBits)  // 2047
	idTimeShift         = idNodeBits + idSeqBits  // 22
	idNodeShift         = idSeqBits               // 11
)

type snowflakeGenerator struct {
	mu       sync.Mutex
	node     int64
	lastTime int64
	seq      int64
}

func newSnowflakeGenerator(node int64) *snowflakeGenerator {
	g := &snowflakeGenerator{}
	g.setNode(node)
	return g
}

func (g *snowflakeGenerator) setNode(node int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.node = node & idMaxNode // clamp defensively; never panic
}

func (g *snowflakeGenerator) next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixMilli()
	if now < g.lastTime {
		// Clock moved backwards: keep issuing monotonic ids off lastTime
		// rather than blocking or returning a smaller id.
		now = g.lastTime
	}
	if now == g.lastTime {
		g.seq = (g.seq + 1) & idMaxSeq
		if g.seq == 0 {
			// Sequence exhausted this millisecond: roll forward.
			now++
			g.lastTime = now
		}
	} else {
		g.seq = 0
		g.lastTime = now
	}
	return uint64(((now - idEpochMillis) << idTimeShift) | (g.node << idNodeShift) | g.seq)
}
