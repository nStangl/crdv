package engine

type Counter interface {
	Get(id string) (int64, error)
	Inc(id string, delta int) error
	Dec(id string, delta int) error
	GetAll() (map[string]int64, error)
	GetMultiple(ids []string) (map[string]int64, error)
}
