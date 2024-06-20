package tuple

type TupleMeta struct {
	timestamp int64
	isDeleted bool
}

func EmptyTupleMeta() *TupleMeta {
	return &TupleMeta{
		timestamp: -1,
		isDeleted: false,
	}
}
