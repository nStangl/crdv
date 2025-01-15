package engine

type List interface {
	Get(id string) ([]string, error)
	GetAt(id string, index int) (string, error)
	Add(id string, index int, value string) error
	Append(id string, value string) error
	Prepend(id string, value string) error
	Rmv(id string, index int) error
	Clear(id string) error
}
