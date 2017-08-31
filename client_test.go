package main

import (
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

func TestCreateSubject(t *testing.T) {
	codec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "test",
          "fields" : [
            {"name": "val", "type": "int"}
          ]
        }`)
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
	err = httpClient.DeleteSubject(schemaName)
	if nil != err {
		t.Errorf("Error deleting subject: %v", err)
	}
	subjects, err = httpClient.GetSubjects()
	if contains(subjects, schemaName) {
		t.Errorf("Did not successfully delete subject")
	}
}

func TestDeleteSubject(t *testing.T) {
	codec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "test",
          "fields" : [
            {"name": "val", "type": "int"}
          ]
        }`)
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
}
