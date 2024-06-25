package tuple_test

import (
	"fisi/elenadb/pkg/storage/table/tuple"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTupleSerializeDeserialize(t *testing.T) {
	// disk := bytes.NewBuffer(nil)

	// Serialize
	tp := tuple.Empty()

	assert.NotNil(t, tp)

	// tp.Data = []byte("Some random tuple data")
	// tp.SerializeTo(disk)

	// // Deserialize
	// tp2 := tuple.Empty()
	// tp2.DeserializeFrom(bytes.NewReader(disk.Bytes()))

	// if string(tp.Data) != string(tp2.Data) {
	// 	t.Fatalf("Data mismatch")
	// }
}
