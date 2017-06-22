package service

type CreateRepoInput struct {
	RepoName       string
	Region         string            `json:"region"`
	Schema         []RepoSchemaEntry `json:"schema"`
	Retention      string            `json:"retention"`
	TSDBseriesName string            `json:"series"`
}

type CreateRepoInputDSL struct {
	RepoName  string
	Region    string `json:"region"`
	Schema    string `json:"schema"`
	Retention string `json:"retention"`
}

type RepoSchemaEntry struct {
	Key         string
	ValueType   string
	ElementType string
	IsTag       bool
	Schemas     []RepoSchemaEntry
}
