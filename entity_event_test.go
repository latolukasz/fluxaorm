package fluxaorm

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildDispatchPreservesLargeIDs guards the UseNumber decode in
// BuildDispatch: snapshot values are uint64 snowflake IDs that exceed
// float64's 2^53 exact range, so a plain json.Unmarshal would round them.
func TestBuildDispatchPreservesLargeIDs(t *testing.T) {
	const productID = uint64(323046535625160701)

	after := map[string]any{"ID": uint64(999), "Product": productID}
	data, err := json.Marshal(&DirtyEvent[map[string]any]{
		Op:    DirtyInsert,
		ID:    12,
		After: &after,
	})
	require.NoError(t, err)

	var got *DirtyEvent[map[string]any]
	dispatch := BuildDispatch(func(_ Context, ev *DirtyEvent[map[string]any]) error {
		got = ev

		return nil
	})

	require.NoError(t, dispatch(nil, &NatsMessage{Data: data}))
	require.NotNil(t, got)
	require.NotNil(t, got.After)

	raw := (*got.After)["Product"]
	num, ok := raw.(json.Number)
	require.Truef(t, ok, "expected json.Number, got %T (%v)", raw, raw)

	parsed, err := strconv.ParseUint(num.String(), 10, 64)
	require.NoError(t, err)
	require.Equal(t, productID, parsed)
}
