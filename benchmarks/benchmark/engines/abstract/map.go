package engine

type Map interface {
	Get(id string) (map[string]string, error)
	Value(id string, key string) (string, error)
	Contains(id string, key string) (bool, error)
	Add(id string, key string, value string) error
	Rmv(id string, key string) error
	Clear(id string) error
}
