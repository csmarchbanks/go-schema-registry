package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/linkedin/goavro"
)

type Client interface {
	GetSchema(int) (*goavro.Codec, error)
	GetSubjects() ([]string, error)
	GetVersions(string) ([]int, error)
	GetSchemaByVersion(string, int) (*goavro.Codec, error)
	CreateSubject(string, *goavro.Codec) (int, error)
	IsSchemaRegistered(string, *goavro.Codec) (int, error)
	DeleteSubject(string) error
	DeleteVersion(string, int) error
}

// HTTPClient is a basic http client to interact with schema registry
type HTTPClient struct {
	SchemaRegistryConnect []string
	httpClient            *http.Client
	retries               int
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

type schemaVersionResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	ID      int    `json:"id"`
}

type idResponse struct {
	ID int `json:"id"`
}

const (
	schemaByID       = "/schemas/ids/%d"
	subjects         = "/subjects"
	subjectVersions  = "/subjects/%s/versions"
	deleteSubject    = "/subjects/%s"
	subjectByVersion = "/subjects/%s/versions/%v"

	latestVersion = "latest"

	contentType = "application/vnd.schemaregistry.v1+json"
)

// NewHTTPClient creates a client to talk with the schema registry at the connect string
func NewHTTPClient(connect []string) *HTTPClient {
	return &HTTPClient{connect, http.DefaultClient, 0}
}

// NewHTTPClientWithRetries creates an http client with a configurable amount of retries on 5XX responses
func NewHTTPClientWithRetries(connect []string, retries int) *HTTPClient {
	return &HTTPClient{connect, http.DefaultClient, retries}
}

// GetSchema returns a goavro.Codec by unique id
func (client *HTTPClient) GetSchema(id int) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(schemaByID, id), nil)
	if nil != err {
		return nil, err
	}
	schema, err := parseSchema(resp)
	if nil != err {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

// GetSubjects returns a list of all subjects in the schema registry
func (client *HTTPClient) GetSubjects() ([]string, error) {
	resp, err := client.httpCall("GET", subjects, nil)
	if nil != err {
		return []string{}, err
	}
	var result = []string{}
	err = json.Unmarshal(resp, &result)
	return result, err
}

// GetVersions returns a list of the versions of a subject
func (client *HTTPClient) GetVersions(subject string) ([]int, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectVersions, subject), nil)
	if nil != err {
		return []int{}, err
	}
	var result = []int{}
	err = json.Unmarshal(resp, &result)
	return result, err
}

// GetSchemaByVersion returns a goavro.Codec for the version of the subject
func (client *HTTPClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectByVersion, subject, version), nil)
	if nil != err {
		return nil, err
	}
	var schema = new(schemaVersionResponse)
	err = json.Unmarshal(resp, &schema)
	if nil != err {
		return nil, err
	}

	return goavro.NewCodec(schema.Schema)
}

// CreateSubject adds a schema to the subject
func (client *HTTPClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

// IsSchemaRegistered tests if the schema is registered, if so it returns the unique id of that schema
func (client *HTTPClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(deleteSubject, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

// DeleteSubject deletes a subject. It should only be used in development
func (client *HTTPClient) DeleteSubject(subject string) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(deleteSubject, subject), nil)
	return err
}

// DeleteVersion deletes a subject. It should only be used in development
func (client *HTTPClient) DeleteVersion(subject string, version int) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(subjectByVersion, subject, version), nil)
	return err
}

func parseSchema(str []byte) (*schemaResponse, error) {
	var schema = new(schemaResponse)
	err := json.Unmarshal(str, &schema)
	return schema, err
}

func parseID(str []byte) (int, error) {
	var id = new(idResponse)
	err := json.Unmarshal(str, &id)
	return id.ID, err
}

func (client *HTTPClient) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	nServers := len(client.SchemaRegistryConnect)
	offset := rand.Intn(nServers)
	for i := 0; ; i++ {
		url := fmt.Sprintf("%s%s", client.SchemaRegistryConnect[(i+offset)%nServers], uri)
		req, err := http.NewRequest(method, url, payload)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", contentType)
		resp, err := client.httpClient.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if i < client.retries && (err != nil || retriable(resp)) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !ok(resp) {
			return nil, newSchemaRegistryError(resp)
		}
		return ioutil.ReadAll(resp.Body)
	}
}

func retriable(resp *http.Response) bool {
	return resp.StatusCode >= 500 && resp.StatusCode < 600
}

func ok(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
