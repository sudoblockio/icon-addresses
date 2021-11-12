package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// List from address test
func TestAddressesEndpointDetail(t *testing.T) {
	assert := assert.New(t)

	addressesServiceURL := os.Getenv("ADDRESSES_SERVICE_URL")
	if addressesServiceURL == "" {
		addressesServiceURL = "http://localhost:8000"
	}
	addressesServiceRestPrefx := os.Getenv("ADDRESSES_SERVICE_REST_PREFIX")
	if addressesServiceRestPrefx == "" {
		addressesServiceRestPrefx = "/api/v1"
	}

	// Get latest transaction
	resp, err := http.Get(addressesServiceURL + addressesServiceRestPrefx + "/addresses?limit=1")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	// Test headers
	assert.NotEqual("0", resp.Header.Get("X-TOTAL-COUNT"))

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable address
	addressPublicKey := bodyMap[0].(map[string]interface{})["public_key"].(string)

	// Test number
	resp, err = http.Get(addressesServiceURL + addressesServiceRestPrefx + "/addresses/details/" + addressPublicKey)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()
}
