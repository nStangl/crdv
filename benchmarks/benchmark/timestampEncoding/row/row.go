package row

type Clock []int64

type Row struct {
	K   int64 // key
	V   int64 // value
	Lts Clock // timestamp
}

func MergeClocks(v1, v2 Clock) {
	for i := 0; i < len(v1); i++ {
		v1[i] = max(v1[i], v2[i])
	}
}
