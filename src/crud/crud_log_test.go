//+build unit

package crud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-logs/config"
	"github.com/geometry-labs/icon-logs/fixtures"
	"github.com/geometry-labs/icon-logs/logging"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestGetLogModel(t *testing.T) {
	assert := assert.New(t)

	logModel := GetLogModel()
	assert.NotEqual(nil, logModel)
}

func TestLogModelInsert(t *testing.T) {
	assert := assert.New(t)

	logModel := GetLogModel()
	assert.NotEqual(nil, logModel)

	// Load fixtures
	logFixtures := fixtures.LoadLogFixtures()

	for _, tx := range logFixtures {

		insertErr := logModel.Insert(tx)
		assert.Equal(nil, insertErr)

		// Clean up
		GetLogModel().db.Where(
			"transaction_hash= ? ",
			tx.TransactionHash,
		).Where(
			"log_index= ?",
			tx.LogIndex,
		).Delete(&tx)
	}
}

func TestLogModelSelectMany(t *testing.T) {
	assert := assert.New(t)

	logModel := GetLogModel()
	assert.NotEqual(nil, logModel)

	// Load fixtures
	logFixtures := fixtures.LoadLogFixtures()

	for _, tx := range logFixtures {

		insertErr := logModel.Insert(tx)
		assert.Equal(nil, insertErr)
	}

	// SelectMany all logs
	logs, _, err := logModel.SelectMany(len(logFixtures), 0, "", "")
	assert.Equal(len(logFixtures), len(*logs))
	assert.Equal(nil, err)

	// Test limit
	logs, _, err = logModel.SelectMany(1, 0, "", "")
	assert.Equal(1, len(*logs))
	assert.Equal(nil, err)

	// Test skip
	logs, _, err = logModel.SelectMany(1, 1, "", "")
	assert.Equal(1, len(*logs))
	assert.Equal(nil, err)

	// Test txHash
	logs, _, err = logModel.SelectMany(1, 1, "0xc34fc0c061a6ad5f6eef087f3dae7b633a40bac1b7697ee528eb3f5861daecbe", "")
	assert.Equal(1, len(*logs))
	assert.Equal(nil, err)

	// Test scoreAddr
	logs, _, err = logModel.SelectMany(1, 1, "", "cx38fd2687b202caf4bd1bda55223578f39dbb6561")
	assert.Equal(1, len(*logs))
	assert.Equal(nil, err)

	// Clean up
	for _, tx := range logFixtures {

		GetLogModel().db.Where(
			"transaction_hash= ? ",
			tx.TransactionHash,
		).Where(
			"log_index= ?",
			tx.LogIndex,
		).Delete(&tx)
	}
}

func TestLogModelLoader(t *testing.T) {
	assert := assert.New(t)

	logModel := GetLogModel()
	assert.NotEqual(nil, logModel)

	// Load fixtures
	logFixtures := fixtures.LoadLogFixtures()

	// Start loader
	StartLogLoader()

	// Write to loader channel
	go func() {
		for _, fixture := range logFixtures {
			logModel.LoaderChannel <- fixture
		}
	}()

	// Wait for inserts
	time.Sleep(5)

	// SelectMany all logs
	logs, _, err := logModel.SelectMany(len(logFixtures), 0, "", "")
	assert.Equal(len(logFixtures), len(*logs))
	assert.Equal(nil, err)

	// Clean up
	for _, tx := range logFixtures {

		GetLogModel().db.Where(
			"transaction_hash= ? ",
			tx.TransactionHash,
		).Where(
			"log_index= ?",
			tx.LogIndex,
		).Delete(&tx)
	}
}
