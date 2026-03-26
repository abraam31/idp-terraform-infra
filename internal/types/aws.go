package types

// RootSpec is intentionally loose for MVP: accept any resources under "aws".
// Later, replace `map[string]any` with typed structs per service and versioned API.
type RootSpec struct {
	Aws map[string]any `yaml:"aws" json:"aws"`
}

