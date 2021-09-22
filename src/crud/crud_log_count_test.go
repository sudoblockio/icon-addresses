//+build unit

package crud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-logs/config"
	"github.com/geometry-labs/icon-logs/logging"
	"github.com/geometry-labs/icon-logs/models"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestGetLogCountModel(t *testing.T) {
	assert := assert.New(t)

	logCountModel := GetLogCountModel()
	assert.NotEqual(nil, logCountModel)
}

func TestLogCountModelInsert(t *testing.T) {
	assert := assert.New(t)

	logCountModel := GetLogCountModel()
	assert.NotEqual(nil, logCountModel)

	logCountFixture := &models.LogCount{
		TransactionHash: "0xa",
		LogIndex:        10,
	}

	insertErr := logCountModel.Insert(logCountFixture)
	assert.Equal(nil, insertErr)
}

func TestLogCountModelSelect(t *testing.T) {
	assert := assert.New(t)

	logCountModel := GetLogCountModel()
	assert.NotEqual(nil, logCountModel)

	// Load fixture
	logCountFixture := &models.LogCount{
		TransactionHash: "0xb",
		LogIndex:        20,
	}

	insertErr := logCountModel.Insert(logCountFixture)
	assert.Equal(nil, insertErr)

	// Select LogCount
	result, err := logCountModel.SelectLargestCount()
	assert.NotEqual(result, 0)
	assert.Equal(nil, err)
}

func TestLogCountModelUpdate(t *testing.T) {
	assert := assert.New(t)

	logCountModel := GetLogCountModel()
	assert.NotEqual(nil, logCountModel)

	// Load fixture
	logCountFixture := &models.LogCount{
		TransactionHash: "0xc",
		LogIndex:        30,
	}

	insertErr := logCountModel.Insert(logCountFixture)
	assert.Equal(nil, insertErr)

	// Select LogCount
	resultOld, err := logCountModel.SelectLargestCount()
	assert.NotEqual(resultOld, 0)
	assert.Equal(nil, err)

	// Update LogCount
	logCountFixture = &models.LogCount{
		TransactionHash: "0xd",
		LogIndex:        40,
	}
	insertErr = logCountModel.Update(logCountFixture)
	assert.Equal(nil, insertErr)

	// Select LogCount
	resultNew, err := logCountModel.SelectLargestCount()
	assert.Equal(resultNew, resultOld+1)
	assert.Equal(nil, err)
}

func TestLogCountModelLoader(t *testing.T) {
	assert := assert.New(t)

	logCountModel := GetLogCountModel()
	assert.NotEqual(nil, logCountModel)

	// Load fixture
	logCountFixture := &models.LogCount{
		TransactionHash: "0xe",
		LogIndex:        50,
	}

	// Start loader
	StartLogCountLoader()

	// Write to loader channel
	go func() {
		i := 0
		for {
			logCountFixture.LogIndex += 1

			logCountModel.LoaderChannel <- logCountFixture
			time.Sleep(1)

			i++
			if i > 3 {
				break
			}
		}
	}()

	// Wait for inserts
	time.Sleep(5)
}
