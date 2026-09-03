// Package jobtasks holds task fixtures. A task struct is plain data plus, at
// most, its queue - which is what keeps the generated package free to import it.
package jobtasks

import "github.com/latolukasz/fluxaorm/v2"

// SendWelcomeEmail declares its queue on the value receiver.
type SendWelcomeEmail struct {
	UserID uint64
	Locale string
}

func (SendWelcomeEmail) Queue() fluxaorm.Queue { return "emails" }

// SendReceipt declares its queue on the pointer receiver, so the optional-
// interface probe has to find both forms.
type SendReceipt struct {
	OrderID uint64
}

func (*SendReceipt) Queue() fluxaorm.Queue { return "emails" }

// SendPasswordReset declares no queue at all, so it falls through to
// fluxaorm.DefaultQueue.
type SendPasswordReset struct {
	UserID uint64
}
