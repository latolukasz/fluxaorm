package fluxaorm

type Entity interface {
	PrivateFlush() error
	PrivateFlushed()
	PrivateFlushEvent() (uint8, map[string]any)
	PrivateGetDatabaseBind() map[string]any
	PrivateIsNew() bool
	PrivateReload() (found bool, err error)
	GetID() uint64
}
