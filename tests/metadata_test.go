package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadataEndpoint(t *testing.T) {
	assert := assert.New(t)

	addressesServiceURL := os.Getenv("ADDRESSES_SERVICE_URL")
	if addressesServiceURL == "" {
		addressesServiceURL = "http://localhost:8000"
	}

	resp, err := http.Get(addressesServiceURL + "/metadata")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make(map[string]interface{})
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)

	// Verify bodyMap
	assert.NotEqual(0, len(bodyMap["version"].(string)))
}
