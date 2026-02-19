package fluxaorm

type Entity interface {
	PrivateFlush() error
	PrivateFlushed()
	GetID() uint64
}
