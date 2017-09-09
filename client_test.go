package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/karrick/goavro"
	uuid "github.com/satori/go.uuid"
)

func contains(array []string, value string) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func createTestCodec() (*goavro.Codec, error) {
	return goavro.NewCodec(`
        {
          "type": "record",
          "name": "test",
          "fields" : [
            {"name": "val", "type": "int"}
          ]
        }`)
}

func testCreateSubjectInternal(t *testing.T, client Client) {
	codec, err := createTestCodec()
	if err != nil {
		t.Errorf("Could not create codec %v", err)
	}
	schemaName := string(uuid.NewV4().String())
	id, err := client.CreateSubject(schemaName, codec)
	if nil != err {
		t.Errorf("error creating schema %v", err)
	}
	subjects, err := client.GetSubjects()
	if !contains(subjects, schemaName) {
		t.Errorf("Could not find subject in list of subjects after creating it")
	}
	schema, err := client.GetSchema(id)
	if nil != err {
		t.Errorf("Did not get schema back %v", err)
	}
	if nil == schema {
		t.Errorf("Something went wrong")
	}
	client.DeleteSubject(schemaName)
}

func TestCreateSubject(t *testing.T) {
	httpClient := NewHTTPClient([]string{"http://localhost:8081"})
	testCreateSubjectInternal(t, httpClient)
}

func testDeleteSubjectInternal(t *testing.T, client Client) {
	codec, err := createTestCodec()
	if err != nil {
		t.Errorf("Could not create codec %v", err)
	}
	schemaName := string(uuid.NewV4().String())
	_, err = client.CreateSubject(schemaName, codec)
	if nil != err {
		t.Errorf("error creating schema %v", err)
	}
	subjects, err := client.GetSubjects()
	if !contains(subjects, schemaName) {
		t.Errorf("Could not find subject in list of subjects after creating it")
	}
	err = client.DeleteSubject(schemaName)
	if nil != err {
		t.Errorf("Error deleting subject: %v", err)
	}
	subjects, err = client.GetSubjects()
	if contains(subjects, schemaName) {
		t.Errorf("Did not successfully delete subject")
	}
	client.DeleteSubject(schemaName)
}

func TestDeleteSubject(t *testing.T) {
	httpClient := NewHTTPClient([]string{"http://localhost:8081"})
	testDeleteSubjectInternal(t, httpClient)
}

func verifyCodecs(t *testing.T, codec1, codec2 *goavro.Codec) {
	t.Helper()
	if codec1.Schema() != codec2.Schema() {
		t.Fatalf("Schema does not match, expected: %s, returned schema: %s", codec1.Schema(), codec2.Schema())
	}
}

func testVersionsInternal(t *testing.T, client Client) {
	codec, _ := createTestCodec()
	schemaName := string(uuid.NewV4().String())
	id, _ := client.CreateSubject(schemaName, codec)
	schemaString := `
        {
          "type": "record",
          "name": "test",
          "fields" : [
            {"name": "val", "type": "int"},
			{"name": "val2", "type": ["string", "null"], "default": "null"} 
          ]
        }`
	codec2, _ := goavro.NewCodec(schemaString)
	client.CreateSubject(schemaName, codec2)
	versions, err := client.GetVersions(schemaName)
	if err != nil {
		t.Fatalf("Error getting versions: %v", err)
	}
	if !reflect.DeepEqual(versions, []int{1, 2}) {
		t.Fatalf("Versions were not 1 and 2, got: %v", versions)
	}
	responseCodec, err := client.GetSchemaByVersion(schemaName, 1)
	if err != nil {
		t.Fatalf("Error getting schema by version: %v", err)
	}
	verifyCodecs(t, codec, responseCodec)
	responseCodec, err = client.GetSchemaByVersion(schemaName, 2)
	if err != nil {
		t.Fatalf("Error getting schema by version: %v", err)
	}
	verifyCodecs(t, codec2, responseCodec)

	idResponse, err := client.IsSchemaRegistered(schemaName, codec)
	if err != nil {
		t.Fatalf("Error testing IsSchemaRegistered: %v", err)
	}
	if id != idResponse {
		t.Fatalf("Ids did not match, expected: %d, got: %d", id, idResponse)
	}
	client.DeleteVersion(schemaName, 1)
	responseCodec, err = client.GetSchemaByVersion(schemaName, 1)
	if nil != responseCodec || err.Error() != "40402 - Version not found." {
		t.Fatalf("Found deleted version responseCodec: %v, error: %v", responseCodec, err)
	}

	client.DeleteSubject(schemaName)
}

func TestVersions(t *testing.T) {
	httpClient := NewHTTPClient([]string{"http://localhost:8081"})
	testVersionsInternal(t, httpClient)
}

func TestRetries(t *testing.T) {
	count := 0
	response := []string{"test"}
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", contentType)
		if count < 3 {
			http.Error(w, `{"error_code": 500, "message": "Error in the backend datastore"}`, 500)
		} else {
			str, _ := json.Marshal(response)
			fmt.Fprintf(w, string(str))
		}
	}))
	httpClient := NewHTTPClientWithRetries([]string{mockServer.URL}, 2)
	subjects, err := httpClient.GetSubjects()
	if err != nil {
		t.Errorf("Found error %s", err)
	}
	if !reflect.DeepEqual(subjects, response) {
		t.Errorf("Subjects did not match expected %s, got %s", response, subjects)
	}
	expectedCallCount := 3
	if count != expectedCallCount {
		t.Errorf("Expected error count to be %d, got %d", expectedCallCount, count)
	}
}
