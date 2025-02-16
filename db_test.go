package orm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDBTransaction(t *testing.T) {
	orm := PrepareTables(t, NewRegistry())
	db := orm.Engine().DB(DefaultPoolCode)
	tx := db.Begin(orm)
	assert.NotNil(t, tx)
	tx.Commit(orm)
}
