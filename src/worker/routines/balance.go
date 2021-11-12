package routines

import (
	"errors"
	"time"

	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/metrics"
	"github.com/geometry-labs/icon-addresses/models"
	"github.com/geometry-labs/icon-addresses/worker/utils"
)

func StartBalanceRoutine() {

	// routine every day
	go balanceRoutine(3600 * time.Second)
}

func balanceRoutine(duration time.Duration) {

	// Init metrics
	metrics.BalanceRoutineNumRuns.Set(float64(0))
	metrics.BalanceRoutineNumAddressesComputed.Set(float64(0))

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 100
		for {
			addresses, err := crud.GetAddressModel().SelectMany(limit, skip)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*addresses) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=Balance", " - Processing ", len(*addresses), " addresses...")
			for _, a := range *addresses {

				/////////////
				// Balance //
				/////////////

				// Node call
				balance, err := utils.IconNodeServiceGetBalanceOf(a.PublicKey)
				if err != nil {
					// Icon node error
					zap.S().Warn("Routine=Balance, publicKey=", a.PublicKey, " - Error: ", err.Error())
					continue
				}

				// Hex -> float64
				a.Balance = utils.StringHexBase18ToFloat64(balance)

				////////////////////
				// Staked Balance //
				////////////////////
				stakedBalance, err := utils.IconNodeServiceGetStakedBalanceOf(a.PublicKey)
				if err != nil {
					// Icon node error
					zap.S().Warn("Routine=Balance, publicKey=", a.PublicKey, " - Error: ", err.Error())
					continue
				}

				// Hex -> float64
				a.Balance += utils.StringHexBase18ToFloat64(stakedBalance)

				// Copy struct for pointer conflicts
				addressCopy := &models.Address{}
				copier.Copy(addressCopy, &a)

				// Insert to database
				crud.GetAddressModel().LoaderChannel <- addressCopy
				zap.S().Info("PUBLICKEY=", a.PublicKey, ",BALANCE=", balanceDecimal)
				metrics.BalanceRoutineNumAddressesComputed.Inc()
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		metrics.BalanceRoutineNumRuns.Inc()
		metrics.BalanceRoutineNumAddressesComputed.Set(float64(0))
		time.Sleep(duration)
	}
}
