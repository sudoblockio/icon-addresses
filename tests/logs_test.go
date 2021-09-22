package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogsEndpoint(t *testing.T) {
	assert := assert.New(t)

	logsServiceURL := os.Getenv("LOGS_SERVICE_URL")
	if logsServiceURL == "" {
		logsServiceURL = "http://localhost:8000"
	}
	logsServiceRestPrefx := os.Getenv("LOGS_SERVICE_REST_PREFIX")
	if logsServiceRestPrefx == "" {
		logsServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(logsServiceURL + logsServiceRestPrefx + "/logs")
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
}
