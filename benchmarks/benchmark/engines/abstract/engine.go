package engine

type Engine interface {
	// Setup the engine
	Setup(connections []any)
	// Cleans the database(s)
	Cleanup(connections []any)
	// Cleans and populates the database(s)
	Populate(connections []any, typesToPopulate []string, itemsPerStructure int, opsPerItem int, valueLength int)
	// Prepares a connection
	Prepare(connection any)
	// Retrieves this engine's register manager
	GetRegister() Register
	// Retrieves this engine's counter manager
	GetCounter() Counter
	// Retrieves this engine's set manager
	GetSet() Set
	// Retrieves this engine's map manager
	GetMap() Map
	// Retrieves this engine's list manager
	GetList() List
	// Returns the engine-specific configurations
	GetConfigs() map[string]string
	// Returns the engine-specific metrics
	GetMetrics(connection any) map[string]string
	// Cleanup any resources
	Finalize(connections []any)
}
