package main

import (
	"sync"

	"github.com/linkedin/goavro"
)

// CachedClient is a schema registry client that will cache calls to GetSchema to improve performance
type CachedClient struct {
	httpClient      *HTTPClient
	schemaCache     map[int]*goavro.Codec
	schemaCacheLock sync.RWMutex
}

// NewCachedClient creates a cached client that will round robin requests to the servers specified
//
// By default it will retry failed requests (5XX responses and http errors) len(connect) number of times
func NewCachedClient(connect []string) *CachedClient {
	httpClient := NewHTTPClient(connect)
	return &CachedClient{httpClient: httpClient, schemaCache: make(map[int]*goavro.Codec)}
}

// NewCachedClientWithRetries creates a cached client that will round robin requests to the servers
// specified and will retry failed requests (5XX responses and http errors) retries number of times
func NewCachedClientWithRetries(connect []string, retries int) *CachedClient {
	httpClient := NewHTTPClientWithRetries(connect, retries)
	return &CachedClient{httpClient: httpClient, schemaCache: make(map[int]*goavro.Codec)}
}

// GetSchema will return and cache the codec with the given id
func (client *CachedClient) GetSchema(id int) (*goavro.Codec, error) {
	client.schemaCacheLock.RLock()
	cachedResult := client.schemaCache[id]
	client.schemaCacheLock.RUnlock()
	if nil != cachedResult {
		return cachedResult, nil
	}
	codec, err := client.httpClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	client.schemaCacheLock.Lock()
	client.schemaCache[id] = codec
	client.schemaCacheLock.Unlock()
	return codec, nil
}

// GetSubjects returns a list of subjects
// Will always make an http call
func (client *CachedClient) GetSubjects() ([]string, error) {
	return client.httpClient.GetSubjects()
}

// GetVersions returns a list of all versions of a subject. Will always make an http call
// Will always make an http call
func (client *CachedClient) GetVersions(subject string) ([]int, error) {
	return client.httpClient.GetVersions(subject)
}

// GetSchemaByVersion returns the codec for a specific version of a subject
func (client *CachedClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.httpClient.GetSchemaByVersion(subject, version)
}

// GetLatestSchema returns the highest version schema for a subject
// Will always make an http call
func (client *CachedClient) GetLatestSchema(subject string) (*goavro.Codec, error) {
	return client.httpClient.GetLatestSchema(subject)
}

// CreateSubject creates a new schema under the specified subject
// Will always make an http call
func (client *CachedClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	return client.httpClient.CreateSubject(subject, codec)
}

// IsSchemaRegistered checks if a specific codec is already registered to a subject
// Will always make an http call
func (client *CachedClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	return client.httpClient.IsSchemaRegistered(subject, codec)
}

// DeleteSubject deletes the subject, should only be used in development
// Will always make an http call
func (client *CachedClient) DeleteSubject(subject string) error {
	return client.httpClient.DeleteSubject(subject)
}

// DeleteVersion deletes the a specific version of a subject, should only be used in development.
// Will always make an http call
func (client *CachedClient) DeleteVersion(subject string, version int) error {
	return client.httpClient.DeleteVersion(subject, version)
}
