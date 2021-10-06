package builders

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/models"
)

// Table builder for balances
func StartBalanceBuilder() {

	// Tail builder
	go startBalanceBuilder(1)

	// Head builder
	// go startBalanceBuilder(1, 2)
}

func startBalanceBuilder(startBlockNumber uint32) {

	currentBlockNumber := startBlockNumber

	for {

		////////////////////////////////////
		// Get transaction and log counts //
		////////////////////////////////////
		currentBlock, err := curd.GetBlockModel().SelectOne(currentBlockNumber)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Block does not exist yet
			// Sleep and try again
			zap.S().Info("Builder=BalanceBuilder, BlockNumber=", currentBlockNumber, " - Block not seen yet. Sleeping 3 seconds...")

			time.Sleep(3 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		///////////////
		// Increment //
		///////////////
		currentBlockNumber++
	}
}
