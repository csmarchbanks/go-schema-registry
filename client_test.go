package main

import (
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

func TestCreateSubject(t *testing.T) {
	codec, err := createTestCodec()
	if err != nil {
		t.Errorf("Could not create codec %v", err)
	}
	httpClient := NewHTTPClient("http://localhost:8081")
	schemaName := string(uuid.NewV4().String())
	id, err := httpClient.CreateSubject(schemaName, codec)
	if nil != err {
		t.Errorf("error creating schema %v", err)
	}
	subjects, err := httpClient.GetSubjects()
	if !contains(subjects, schemaName) {
		t.Errorf("Could not find subject in list of subjects after creating it")
	}
	schema, err := httpClient.GetSchema(id)
	if nil != err {
		t.Errorf("Did not get schema back %v", err)
	}
	if nil == schema {
		t.Errorf("Something went wrong")
	}
	httpClient.DeleteSubject(schemaName)
}

func TestDeleteSubject(t *testing.T) {
	codec, err := createTestCodec()
	if err != nil {
		t.Errorf("Could not create codec %v", err)
	}
	httpClient := NewHTTPClient("http://localhost:8081")
	schemaName := string(uuid.NewV4().String())
	_, err = httpClient.CreateSubject(schemaName, codec)
	if nil != err {
		t.Errorf("error creating schema %v", err)
	}
	subjects, err := httpClient.GetSubjects()
	if !contains(subjects, schemaName) {
		t.Errorf("Could not find subject in list of subjects after creating it")
	}
	err = httpClient.DeleteSubject(schemaName)
	if nil != err {
		t.Errorf("Error deleting subject: %v", err)
	}
	subjects, err = httpClient.GetSubjects()
	if contains(subjects, schemaName) {
		t.Errorf("Did not successfully delete subject")
	}
	httpClient.DeleteSubject(schemaName)
}

func verifyCodecs(t *testing.T, codec1, codec2 *goavro.Codec) {
	t.Helper()
	if codec1.Schema() != codec2.Schema() {
		t.Fatalf("Schema does not match, expected: %s, returned schema: %s", codec1.Schema(), codec2.Schema())
	}
}

func TestVersions(t *testing.T) {
	codec, _ := createTestCodec()
	httpClient := NewHTTPClient("http://localhost:8081")
	schemaName := string(uuid.NewV4().String())
	httpClient.CreateSubject(schemaName, codec)
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
	httpClient.CreateSubject(schemaName, codec2)
	versions, err := httpClient.GetVersions(schemaName)
	if err != nil {
		t.Fatalf("Error getting versions: %v", err)
	}
	if !reflect.DeepEqual(versions, []int{1, 2}) {
		t.Fatalf("Versions were not 1 and 2, got: %v", versions)
	}
	responseCodec, err := httpClient.GetSchemaByVersion(schemaName, 1)
	if err != nil {
		t.Fatalf("Error getting schema by version: %v", err)
	}
	verifyCodecs(t, codec, responseCodec)
	responseCodec, err = httpClient.GetSchemaByVersion(schemaName, 2)
	if err != nil {
		t.Fatalf("Error getting schema by version: %v", err)
	}
	verifyCodecs(t, codec2, responseCodec)

	httpClient.DeleteSubject(schemaName)
}
