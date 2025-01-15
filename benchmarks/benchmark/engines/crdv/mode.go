package crdv

type mode int

const (
	Mvr mode = iota
	Lww
	Aw
	Rw
	RwMvr
	AwMvr
	AwLww
)
