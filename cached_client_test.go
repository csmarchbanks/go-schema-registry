package schemaregistry

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCachedGetSchema(t *testing.T) {
	codec, _ := createTestCodec()
	schema := codec.Schema()
	count := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", contentType)
		escapedSchema := strings.Replace(schema, "\"", "\\\"", -1)
		fmt.Fprintf(w, `{"schema": "%s"}`, escapedSchema)
	}))
	client := NewCachedClient([]string{mockServer.URL})
	client.GetSchema(1)
	responseCodec, err := client.GetSchema(1)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if responseCodec.Schema() != codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", codec.Schema(), responseCodec.Schema())
	}
	if count > 1 {
		t.Errorf("Expected call count of 1, got %d", count)
	}
}

func TestCachedCreateSchema(t *testing.T) {
	client := NewCachedClient([]string{"http://localhost:8081"})
	testCreateSubjectInternal(t, client)
}

func TestCachedDeleteSubject(t *testing.T) {
	client := NewCachedClient([]string{"http://localhost:8081"})
	testDeleteSubjectInternal(t, client)
}

func TestCachedVersions(t *testing.T) {
	client := NewCachedClient([]string{"http://localhost:8081"})
	testVersionsInternal(t, client)
}
