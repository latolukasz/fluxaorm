package fluxaorm

type Entity interface {
	PrivateFlush() error
	PrivateFlushed()
	PrivateFlushEvent() (uint8, map[string]any)
	GetID() uint64
}
