package routines

import (
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/models"
	"github.com/geometry-labs/icon-addresses/redis"
)

func StartAddressCountRoutine() {

	// routine every day
	go addressCountRoutine(3600 * time.Second)
}

func addressCountRoutine(duration time.Duration) {

	// Loop every duration
	for {

		/////////
		// All //
		/////////

		// Count
		count, err := crud.GetAddressModel().CountAll()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey := "icon_addresses_address_count_all"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		addressCount := &models.AddressCount{
			Type:  "all",
			Count: uint64(count),
		}
		err = crud.GetAddressCountModel().UpsertOne(addressCount)

		//////////////
		// Contract //
		//////////////

		// Count
		count, err := crud.GetAddressModel().CountContract()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey := "icon_addresses_address_count_contract"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		addressCount = &models.AddressCount{
			Type:  "contract",
			Count: uint64(count),
		}
		err = crud.GetAddressCountModel().UpsertOne(addressCount)

		///////////
		// Token //
		///////////

		// Count
		count, err := crud.GetAddressModel().CountToken()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey := "icon_addresses_address_count_token"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		addressCount := &models.AddressCount{
			Type:  "token",
			Count: uint64(count),
		}
		err = crud.GetAddressCountModel().UpsertOne(addressCount)

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
