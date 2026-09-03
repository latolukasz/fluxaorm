// Package jobtasks deliberately shares its name with the other fixture package
// so the generator's import aliasing has something to disambiguate.
package jobtasks

import "github.com/latolukasz/fluxaorm/v2"

type TranscodeClip struct {
	Path string
}

func (TranscodeClip) Queue() fluxaorm.Queue { return "media" }
