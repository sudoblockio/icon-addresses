package tests

import (
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// List test
func TestContractsEndpointList(t *testing.T) {
	assert := assert.New(t)

	addressesServiceURL := os.Getenv("ADDRESSES_SERVICE_URL")
	if addressesServiceURL == "" {
		addressesServiceURL = "http://localhost:8000"
	}
	addressesServiceRestPrefx := os.Getenv("ADDRESSES_SERVICE_REST_PREFIX")
	if addressesServiceRestPrefx == "" {
		addressesServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(addressesServiceURL + addressesServiceRestPrefx + "/addresses/contracts")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()
}
