package main

import (
	"testing"

	"github.com/karrick/goavro"
)

func TestCreateSchema(t *testing.T) {
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
	httpClient := NewHttpClient("http://localhost:8081")
	id, err := httpClient.CreateSchema("test", codec)
	if nil != err {
		t.Errorf("error creating schema %v", err)
	}
	schema, err := httpClient.GetSchema(id)
	if nil != err {
		t.Errorf("Did not get schema back %v", err)
	}
	if nil == schema {
		t.Errorf("Something went wrong")
	}
}
