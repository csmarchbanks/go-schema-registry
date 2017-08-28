package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/karrick/goavro"
)

type SchemaRegistryClient interface {
	GetSchema(id int) (*goavro.Codec, error)
	CreateSchema(subject string, codec *goavro.Codec) (int, error)
	GetSubjects() ([]string, error)
	GetVersions(subject string) ([]int, error)
	GetLatestSubject(subject string) ([]int, error)
	GetSubject(subject string, id int) ([]int, error)
	DeleteSubject(subject string) error
}

type HttpClient struct {
	SchemaRegistryConnect string
	httpClient            *http.Client
}

type Schema struct {
	Schema string `json:"schema"`
}

type idResponse struct {
	Id int `json:"id"`
}

const (
	SCHEMA_BY_ID       = "/schemas/ids/%d"
	SUBJECTS           = "/subjects"
	SUBJECT_VERSIONS   = "/subjects/%s/versions"
	DELETE_SUBJECT     = "/subjects/%s"
	SUBJECT_BY_VERSION = "/subjects/%s/versions/%v"

	LATEST_VERSION = "latest"

	CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
)

func NewHttpClient(connect string) HttpClient {
	return HttpClient{connect, http.DefaultClient}
}

func (client *HttpClient) GetSchema(id int) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(SCHEMA_BY_ID, id), nil)
	if nil != err {
		return nil, err
	}
	schema, err := parseSchema(resp)
	if nil != err {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

func (client *HttpClient) CreateSchema(subject string, codec *goavro.Codec) (int, error) {
	schema := Schema{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(SUBJECT_VERSIONS, subject), payload)
	if err != nil {
		return 0, err
	} else if !ok(resp) {
		return 0, fmt.Errorf("non-ok return code found: %s", resp.Status)
	}
	return parseId(resp)
}

func ok(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func parseSchema(resp *http.Response) (*Schema, error) {
	var schema = new(Schema)
	err := json.NewDecoder(resp.Body).Decode(&schema)
	return schema, err
}

func parseId(resp *http.Response) (int, error) {
	var id = new(idResponse)
	str, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(str, id)
	return id.Id, err
}

func (client HttpClient) httpCall(method, uri string, payload io.Reader) (*http.Response, error) {
	url := fmt.Sprintf("%s%s", client.SchemaRegistryConnect, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", CONTENT_TYPE)
	req.Header.Set("Accept", CONTENT_TYPE)
	resp, err := client.httpClient.Do(req)
	return resp, err
}
