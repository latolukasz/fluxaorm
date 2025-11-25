package fluxaorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBTransaction(t *testing.T) {
	orm := PrepareTables(t, NewRegistry())
	db := orm.Engine().DB(DefaultPoolCode)
	tx, err := db.Begin(orm)
	assert.NoError(t, err)
	assert.NotNil(t, tx)
	err = tx.Commit(orm)
	assert.NoError(t, err)
}
