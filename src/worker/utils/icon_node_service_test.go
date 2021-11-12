package utils

import (
	"testing"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/stretchr/testify/assert"
)

func init() {
	config.ReadEnvironment()
}

func TestIconNodeServiceGetBalanceOf(t *testing.T) {
	assert := assert.New(t)

	balance, err := IconNodeServiceGetBalanceOf("hx54f7853dc6481b670caf69c5a27c7c8fe5be8269")
	assert.Equal(nil, err)

	t.Log(balance)
}

func TestIconNodeServiceGetStakedBalanceOf(t *testing.T) {
	assert := assert.New(t)

	balance, err := IconNodeServiceGetStakedBalanceOf("hx9d9ad1bc19319bd5cdb5516773c0e376db83b644")
	assert.Equal(nil, err)

	t.Log(balance)
}
