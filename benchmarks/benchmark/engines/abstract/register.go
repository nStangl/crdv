package engine

type Register interface {
	Get(id string) (string, error)
	Set(id string, value string) error
}
