package main

import (
	"github.com/karrick/goavro"
)

type CachedClient struct {
	httpClient  *HTTPClient
	schemaCache map[int]*goavro.Codec
}

func NewCachedClient(connect []string) *CachedClient {
	httpClient := NewHTTPClient(connect)
	return &CachedClient{httpClient, make(map[int]*goavro.Codec)}
}
func NewCachedClientWithRetries(connect []string, retries int) *CachedClient {
	httpClient := NewHTTPClientWithRetries(connect, retries)
	return &CachedClient{httpClient, make(map[int]*goavro.Codec)}
}

func (client *CachedClient) GetSchema(id int) (*goavro.Codec, error) {
	cachedResult := client.schemaCache[id]
	if nil != cachedResult {
		return cachedResult, nil
	}
	httpResult, err := client.httpClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	client.schemaCache[id] = httpResult
	return httpResult, nil
}

func (client *CachedClient) GetSubjects() ([]string, error) {
	return client.httpClient.GetSubjects()
}

func (client *CachedClient) GetVersions(subject string) ([]int, error) {
	return client.httpClient.GetVersions(subject)
}

func (client *CachedClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.httpClient.GetSchemaByVersion(subject, version)
}

func (client *CachedClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	return client.httpClient.CreateSubject(subject, codec)
}

func (client *CachedClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	return client.httpClient.IsSchemaRegistered(subject, codec)
}

func (client *CachedClient) DeleteSubject(subject string) error {
	return client.httpClient.DeleteSubject(subject)
}

func (client *CachedClient) DeleteVersion(subject string, version int) error {
	return client.httpClient.DeleteVersion(subject, version)
}
