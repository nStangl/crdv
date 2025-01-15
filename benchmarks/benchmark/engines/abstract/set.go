package engine

type Set interface {
	Get(id string) ([]string, error)
	Contains(id string, value string) (bool, error)
	Add(id string, value string) error
	Rmv(id string, value string) error
	Clear(id string) error
}
