package fluxaorm

import (
	"crypto/sha256"
	"fmt"
)

const cacheNilValue = ""

func hashString(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}
