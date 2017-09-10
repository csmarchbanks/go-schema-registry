package schemaregistry

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// SchemaRegistryError holds more detailed information about errors coming back from schema registry
type SchemaRegistryError struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (e *SchemaRegistryError) Error() string {
	return fmt.Sprintf("%d - %s", e.ErrorCode, e.Message)
}

func newSchemaRegistryError(resp *http.Response) *SchemaRegistryError {
	err := &SchemaRegistryError{}
	parsingErr := json.NewDecoder(resp.Body).Decode(&err)
	if parsingErr != nil {
		return &SchemaRegistryError{resp.StatusCode, "Unrecognized error found"}
	}
	return err
}
