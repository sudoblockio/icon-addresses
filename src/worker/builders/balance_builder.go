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

	go startBalanceBuilder()
}

func startBalanceBuilder() {

	currentBlockNumber := uint64(0)

	// Hardcode genesis transaction in block#0
	// hx54f7853dc6481b670caf69c5a27c7c8fe5be8269
	if currentBlockNumber == 0 {

		// Load to database
		crud.GetBalanceModel().LoaderChannel <- &models.Balance{
			BlockNumber:      0,
			TransactionIndex: 0,
			LogIndex:         -1,
			PublicKey:        "hx54f7853dc6481b670caf69c5a27c7c8fe5be8269",
			Value:            "0x2961FFF8CA4A62327800000",
			ValueDecimal:     800460000,
			Timestamp:        0,
		}

		currentBlockNumber++
	}

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

		// NOTE the transactions should already be sorted by transaction_index, log_index in crud
		for _, transaction := range *currentBlockTransactions {
			publicKeys := []string{transaction.FromAddress, transaction.ToAddress}

			for _, key := range publicKeys {

				curValue := "0x0"
				newValue := transaction.Value
				newFee := transaction.TransactionFee

				// Get current balance of public key
				curBalance, err := crud.GetBalanceModel().SelectOneByBlockNumber(
					key,
					transaction.BlockNumber-1,
				)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					curValue = "0x0"
				} else if err != nil {
					// Postgres error
					zap.S().Fatal(err.Error())
				} else {
					curValue = curBalance.Value
				}

				// Hex -> big.Int
				curValueBigInt, _ := new(big.Int).SetString(curValue[2:], 16)
				newValueBigInt, _ := new(big.Int).SetString(newValue[2:], 16)
				newFeeBigInt, _ := new(big.Int).SetString(newFee[2:], 16)

				if key == transaction.ToAddress {
					// Add value
					newValueBigInt = newValueBigInt.Add(curValueBigInt, newValueBigInt)
				} else if key == transaction.FromAddress {
					// Subtract value
					newValueBigInt = newValueBigInt.Sub(curValueBigInt, newValueBigInt)

					// Subtact Fee
					newValueBigInt = newValueBigInt.Sub(newValueBigInt, newFeeBigInt)
				}

				newValue = fmt.Sprintf("0x%x", newValueBigInt)

				// Value -> ValueDecimal
				newValueDecimal := float64(0)

				baseBigFloat, _ := new(big.Float).SetString("1000000000000000000") // 10^18
				newValueBigFloat := new(big.Float).SetInt(newValueBigInt)

				// newValue / 10^18
				newValueBigFloat = newValueBigFloat.Quo(newValueBigFloat, baseBigFloat)

				newValueDecimal, _ = newValueBigFloat.Float64()

				// Load to database
				crud.GetBalanceModel().LoaderChannel <- &models.Balance{
					BlockNumber:      transaction.BlockNumber,
					TransactionIndex: transaction.TransactionIndex,
					LogIndex:         transaction.LogIndex,
					PublicKey:        key,
					Value:            newValue,
					ValueDecimal:     newValueDecimal,
					Timestamp:        transaction.BlockTimestamp,
				}

				// Wait until state is set
				for {
					latestBalance, err := crud.GetBalanceModel().SelectOneByBlockNumber(
						key,
						transaction.BlockNumber,
					)
					if errors.Is(err, gorm.ErrRecordNotFound) {
						// No record yet
						zap.S().Info(
							"Builder=BalanceBuilder,",
							"BlockNumber=", currentBlockNumber,
							" - Balance not set yet...",
						)
						time.Sleep(1 * time.Millisecond)
						continue
					} else if err != nil {
						// Postgres error
						zap.S().Fatal(err.Error())
					} else if latestBalance.Value != newValue {
						zap.S().Info(
							"Builder=BalanceBuilder,",
							"BlockNumber=", currentBlockNumber,
							" - Balance not set yet...",
						)
						time.Sleep(1 * time.Millisecond)
						continue
					}
					// Successful
					break
				}
			}
		}

		///////////////
		// Increment //
		///////////////
		zap.S().Debug(
			"Builder=BalanceBuilder,",
			"BlockNumber=", currentBlockNumber,
			" - Reading next block...",
		)
		currentBlockNumber++
	}
}
