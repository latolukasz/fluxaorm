package fluxaorm

import "strconv"

// NotFoundError is returned by GetByID, SearchOne, SearchOneInRedis and
// reference getters when no row matches. ID is 0 for search and unset-reference misses.
type NotFoundError struct {
	Entity string
	ID     uint64
}

func (e *NotFoundError) Error() string {
	if e.ID == 0 {
		return e.Entity + " not found"
	}
	return e.Entity + " " + strconv.FormatUint(e.ID, 10) + " not found"
}
