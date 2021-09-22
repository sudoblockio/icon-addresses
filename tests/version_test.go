package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionEndpoint(t *testing.T) {
	assert := assert.New(t)

	logsServiceURL := os.Getenv("LOGS_SERVICE_URL")
	if logsServiceURL == "" {
		logsServiceURL = "http://localhost:8000"
	}

	resp, err := http.Get(logsServiceURL + "/version")
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
