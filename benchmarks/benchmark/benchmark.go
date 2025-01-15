package benchmark

type Benchmark interface {
	// Called once at the start of the run, to setup any resources required
	Setup(connections []any)
	// Populates (and cleans if needed) the databases (receives the list of different connections)
	Populate(connections []any)
	// Prepares the statements and returns the list of operations (called for each worker)
	Prepare(connection any) map[string]func() error
	// Returns the benchmark-specific configurations
	GetConfigs() map[string]string
	// Returns the benchmark-specific metrics
	GetMetrics(connection any) map[string]string
	// Called once at the end of the run, to close any resources required
	Finalize(connections []any)
}
