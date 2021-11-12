package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/geometry-labs/icon-addresses/config"
)

func IconNodeServiceGetBalanceOf(publicKey string) (string, error) {

	url := config.Config.IconNodeServiceURL
	method := "POST"
	payload := fmt.Sprintf(`{
    "jsonrpc": "2.0",
    "method": "icx_getBalance",
    "id": 1234,
    "params": {
        "address": "%s"
    }
	}`, publicKey)

	// Create http client
	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return "0x0", err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return "0x0", err
	}
	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "0x0", err
	}

	// Check status code
	if res.StatusCode != 200 {
		return "0x0", errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	body := map[string]interface{}{}
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		return "0x0", err
	}

	// Extract balance
	balance, ok := body["result"].(string)
	if ok == false {
		return "0x0", errors.New("Invalid response")
	}

	return balance, nil
}

func IconNodeServiceGetStakedBalanceOf(publicKey string) (string, error) {

	url := config.Config.IconNodeServiceURL
	method := "POST"
	payload := fmt.Sprintf(`{
    "jsonrpc": "2.0",
    "id": 1234,
    "method": "icx_call",
    "params": {
        "to": "cx0000000000000000000000000000000000000000",
        "dataType": "call",
        "data": {
            "method": "getStake",
            "params": {
                "address": "%s"
            }
        }
    }
	}`, publicKey)

	// Create http client
	client := &http.Client{}
	req, err := http.NewRequest(method, url, strings.NewReader(payload))
	if err != nil {
		return "0x0", err
	}

	// Execute request
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return "0x0", err
	}
	defer res.Body.Close()

	// Read body
	bodyString, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "0x0", err
	}

	// Check status code
	if res.StatusCode != 200 {
		return "0x0", errors.New(
			"StatusCode=" + strconv.Itoa(res.StatusCode) +
				",Request=" + payload +
				",Response=" + string(bodyString),
		)
	}

	// Parse body
	body := map[string]interface{}{}
	err = json.Unmarshal(bodyString, &body)
	if err != nil {
		return "0x0", err
	}

	// Extract balance
	resultMap, ok := body["result"].(map[string]interface{})
	if ok == false {
		return "0x0", errors.New("Invalid response")
	}

	balance, ok := resultMap["stake"].(string)
	if ok == false {
		return "0x0", errors.New("Invalid response")
	}

	return balance, nil
}
