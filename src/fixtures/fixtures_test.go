package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/logging"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestLoadAddressFixtures(t *testing.T) {
	assert := assert.New(t)

	addressFixtures := LoadAddressFixtures()

	assert.NotEqual(0, len(addressFixtures))
}
