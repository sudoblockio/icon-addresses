package builders

import (
	"errors"
	"fmt"
	"math/big"
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

func startBalanceBuilder(startBlockNumber uint64) {

	currentBlockNumber := startBlockNumber
	for {

		//////////////////////////
		// Check database state //
		//////////////////////////

		// check for block
		currentBlock, err := crud.GetBlockModel().SelectOne(uint32(currentBlockNumber))
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Block does not exist yet
			// Sleep and try again
			zap.S().Info(
				"Builder=BalanceBuilder,",
				"BlockNumber=", currentBlockNumber,
				" - Block not seen yet. Sleeping 3 seconds...",
			)

			time.Sleep(3 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		// Check for block transactions
		currentBlockTransactionCount, err := crud.GetTransactionCountByBlockNumberModel().SelectLargestCountByBlockNumber(currentBlockNumber)
		if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		} else if currentBlockTransactionCount != uint64(currentBlock.TransactionCount) {
			zap.S().Info(
				"Builder=BalanceBuilder,",
				"BlockNumber=", currentBlockNumber,
				"BlockTransactionCount=", currentBlockTransactionCount,
				"BlockTransactions=", currentBlock.TransactionCount,
				" - Transactions not yet seen. Sleeping 3 seconds...",
			)

			time.Sleep(3 * time.Second)
			continue
		}

		// Check for block logs
		currentBlockLogCount, err := crud.GetLogCountByBlockNumberModel().SelectLargestCountByBlockNumber(currentBlockNumber)
		if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		} else if currentBlockLogCount != uint64(currentBlock.LogCount) {
			zap.S().Info(
				"Builder=BalanceBuilder,",
				"BlockNumber=", currentBlockNumber,
				"BlockLogCount=", currentBlockLogCount,
				"BlockLogs=", currentBlock.LogCount,
				" - Logs not yet seen. Sleeping 3 seconds...",
			)

			time.Sleep(3 * time.Second)
			continue
		}

		//////////////////////
		// Compute Balances //
		//////////////////////
		currentBlockTransactions, err := crud.GetTransactionModel().SelectManyByBlockNumber(currentBlockNumber)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Transactions do not exist yet
			// Sleep and try again
			zap.S().Warn(
				"Builder=BalanceBuilder,",
				"BlockNumber=", currentBlockNumber,
				" - Transactions not seen in table. CHECK DATABASE. Sleeping 3 seconds...",
			)

			time.Sleep(3 * time.Second)
			continue
		} else if err != nil {
			// Postgres error
			zap.S().Fatal(err.Error())
		}

		// NOTE the transactions should be sorted by transaction_index, log_index
		for _, transaction := range *currentBlockTransactions {
			publicKeys := []string{transaction.FromAddress, transaction.ToAddress}

			for _, key := range publicKeys {

				curValue := "0x0"
				newValue := transaction.Value

				// Get current balance of public key
				curBalance, err := crud.GetBalanceModel().SelectOneByBlockNumber(
					key,
					transaction.BlockNumber,
				)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// No entry yet
					curValue = "0x0"
				} else if err != nil {
					// Postgres error
					zap.S().Fatal(err.Error())
				} else {
					curValue = curBalance.Value
				}

				// add new value
				curValueBigInt, _ := new(big.Int).SetString(curValue[2:], 16)
				newValueBigInt, _ := new(big.Int).SetString(newValue[2:], 16)

				if key == transaction.ToAddress {
					// Add
					newValueBigInt = newValueBigInt.Add(curValueBigInt, newValueBigInt)
				} else if key == transaction.FromAddress {
					// Subtract
					newValueBigInt = newValueBigInt.Sub(curValueBigInt, newValueBigInt)
				}

				newValue = fmt.Sprintf("0x%x", newValueBigInt)

				// Load to database
				crud.GetBalanceModel().LoaderChannel <- &models.Balance{
					BlockNumber:      transaction.BlockNumber,
					TransactionIndex: transaction.TransactionIndex,
					LogIndex:         transaction.LogIndex,
					PublicKey:        key,
					Value:            newValue,
					Timestamp:        transaction.BlockTimestamp,
				}
			}
		}

		///////////////
		// Increment //
		///////////////
		currentBlockNumber++
	}
}
