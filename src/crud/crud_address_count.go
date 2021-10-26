package crud

import (
	"errors"
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
	"github.com/geometry-labs/icon-addresses/redis"
)

// AddressCountModel - type for address table model
type AddressCountModel struct {
	db            *gorm.DB
	model         *models.AddressCount
	modelORM      *models.AddressCountORM
	LoaderChannel chan *models.AddressCount
}

var addressCountModel *AddressCountModel
var addressCountModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetAddressCountModel() *AddressCountModel {
	addressCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		addressCountModel = &AddressCountModel{
			db:            dbConn,
			model:         &models.AddressCount{},
			LoaderChannel: make(chan *models.AddressCount, 1),
		}

		err := addressCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartAddressCountLoader()
	})

	return addressCountModel
}

// Migrate - migrate addressCounts table
func (m *AddressCountModel) Migrate() error {
	// Only using AddressCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from addressCounts table
func (m *AddressCountModel) SelectOne(_type string) (*models.AddressCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.AddressCount{})

	// Address
	db = db.Where("type = ?", _type)

	addressCount := &models.AddressCount{}
	db = db.First(addressCount)

	return addressCount, db.Error
}

// Select - select from addressCounts table
func (m *AddressCountModel) SelectCount(_type string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.AddressCount{})

	// Address
	db = db.Where("type = ?", _type)

	addressCount := &models.AddressCount{}
	db = db.First(addressCount)

	count := uint64(0)
	if addressCount != nil {
		count = addressCount.Count
	}

	return count, db.Error
}

func (m *AddressCountModel) UpsertOne(
	addressCount *models.AddressCount,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*addressCount),
		reflect.TypeOf(*addressCount),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "type"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(addressCount)

	return db.Error
}

// StartAddressCountLoader starts loader
func StartAddressCountLoader() {
	go func() {
		postgresLoaderChan := GetAddressCountModel().LoaderChannel

		for {
			// Read address
			newAddressCount := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_addresses_address_count_" + newAddressCount.Type

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal(
					"Loader=AddressCount,",
					"PublicKey=", newAddressCount.PublicKey,
					" Type=", newAddressCount.Type,
					" - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curAddressCount, err := GetAddressCountModel().SelectOne(newAddressCount.Type)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=AddressCount,",
						"PublicKey=", newAddressCount.PublicKey,
						" Type=", newAddressCount.Type,
						" - Error: ", err.Error())
				} else {
					count = int64(curAddressCount.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal(
						"Loader=AddressCount,",
						"PublicKey=", newAddressCount.PublicKey,
						" Type=", newAddressCount.Type,
						" - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add address to indexed
			if newAddressCount.Type == "all" {
				newAddressCountIndex := &models.AddressCountIndex{
					PublicKey: newAddressCount.PublicKey,
				}
				err = GetAddressCountIndexModel().Insert(newAddressCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			} else if newAddressCount.Type == "contract" {
				newAddressContractCountIndex := &models.AddressContractCountIndex{
					PublicKey: newAddressCount.PublicKey,
				}
				err = GetAddressContractCountIndexModel().Insert(newAddressContractCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			} else if newAddressCount.Type == "token" {
				newAddressTokenCountIndex := &models.AddressTokenCountIndex{
					PublicKey: newAddressCount.PublicKey,
				}
				err = GetAddressTokenCountIndexModel().Insert(newAddressTokenCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal(
					"Loader=AddressCount,",
					"PublicKey=", newAddressCount.PublicKey,
					" Type=", newAddressCount.Type,
					" - Error: ", err.Error())
			}
			newAddressCount.Count = uint64(count)

			err = GetAddressCountModel().UpsertOne(newAddressCount)
			zap.S().Debug(
				"Loader=AddressCount,",
				"PublicKey=", newAddressCount.PublicKey,
				" Type=", newAddressCount.Type,
				" - Upsert")
			if err != nil {
				// Postgres error
				zap.S().Fatal(
					"Loader=AddressCount,",
					"PublicKey=", newAddressCount.PublicKey,
					" Type=", newAddressCount.Type,
					" - Error: ", err.Error())
			}
		}
	}()
}
