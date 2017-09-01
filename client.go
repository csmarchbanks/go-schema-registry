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

// HTTPClient is a basic http client to interact with schema registry
type HTTPClient struct {
	SchemaRegistryConnect string
	httpClient            *http.Client
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
func NewHTTPClient(connect string) HTTPClient {
	return HTTPClient{connect, http.DefaultClient}
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
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result, err
}

// GetVersions returns a list of the versions of a subject
func (client *HTTPClient) GetVersions(subject string) ([]int, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectVersions, subject), nil)
	if nil != err {
		return []int{}, err
	}
	var result = []int{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result, err
}

// GetSchemaByVersion returns a goavro.Codec for the version of the subject
func (client *HTTPClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectByVersion, subject, version), nil)
	if nil != err {
		return nil, err
	}
	bodyStr, _ := ioutil.ReadAll(resp.Body)
	var schema = new(schemaVersionResponse)
	err = json.Unmarshal(bodyStr, schema)
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
	} else if !ok(resp) {
		return 0, fmt.Errorf("non-ok return code found: %s", resp.Status)
	}
	return parseID(resp)
}

// DeleteSubject deletes a subject. It should only be used in development
func (client *HTTPClient) DeleteSubject(subject string) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(deleteSubject, subject), nil)
	return err
}

func ok(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func parseSchema(resp *http.Response) (*schemaResponse, error) {
	var schema = new(schemaResponse)
	err := json.NewDecoder(resp.Body).Decode(&schema)
	return schema, err
}

func parseID(resp *http.Response) (int, error) {
	var id = new(idResponse)
	str, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(str, id)
	return id.ID, err
}

func (client HTTPClient) httpCall(method, uri string, payload io.Reader) (*http.Response, error) {
	url := fmt.Sprintf("%s%s", client.SchemaRegistryConnect, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", contentType)
	resp, err := client.httpClient.Do(req)
	return resp, err
}
