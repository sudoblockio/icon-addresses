package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/models"
	"github.com/geometry-labs/icon-addresses/redis"
)

func StartTransactionCountByPublicKeyRoutine() {

	// routine every day
	go transactionCountByPublicKeyRoutine(3600 * time.Second)
}

func transactionCountByPublicKeyRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 1000
		for {
			transactionCountByPublicKeys, err := crud.GetTransactionCountByPublicKeyModel().SelectMany(limit, skip)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TransactionCountByPublicKey", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*transactionCountByPublicKeys) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TransactionCountByPublicKey", " - Processing ", len(*transactionCountByPublicKeys), " Public Keys...")
			for _, t := range *transactionCountByPublicKeys {

				///////////
				// Count //
				///////////
				count, err := crud.GetTransactionCountByPublicKeyIndexModel().CountByPublicKey(t.PublicKey)
				if err != nil {
					// Postgres error
					zap.S().Warn(err)
					continue
				}

				//////////////////
				// Update Redis //
				//////////////////
				countKey := "icon_addresses_transaction_count_by_address_" + t.PublicKey
				err = redis.GetRedisClient().SetCount(countKey, count)
				if err != nil {
					// Redis error
					zap.S().Warn(err)
					continue
				}

				/////////////////////
				// Update Postgres //
				/////////////////////
				transactionCountByPublicKey := &models.TransactionCountByPublicKey{
					PublicKey: t.PublicKey,
					Count:     uint64(count),
				}
				err = crud.GetTransactionCountByPublicKeyModel().UpsertOne(transactionCountByPublicKey)
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
