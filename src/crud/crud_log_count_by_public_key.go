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

// LogCountByPublicKeyModel - type for address table model
type LogCountByPublicKeyModel struct {
	db            *gorm.DB
	model         *models.LogCountByPublicKey
	modelORM      *models.LogCountByPublicKeyORM
	LoaderChannel chan *models.LogCountByPublicKey
}

var logCountByPublicKeyModel *LogCountByPublicKeyModel
var logCountByPublicKeyModelOnce sync.Once

// GetPublicKeyModel - create and/or return the addresss table model
func GetLogCountByPublicKeyModel() *LogCountByPublicKeyModel {
	logCountByPublicKeyModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logCountByPublicKeyModel = &LogCountByPublicKeyModel{
			db:            dbConn,
			model:         &models.LogCountByPublicKey{},
			LoaderChannel: make(chan *models.LogCountByPublicKey, 1),
		}

		err := logCountByPublicKeyModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogCountByPublicKeyModel: Unable migrate postgres table: ", err.Error())
		}

		StartLogCountByPublicKeyLoader()
	})

	return logCountByPublicKeyModel
}

// Migrate - migrate logCountByPublicKeys table
func (m *LogCountByPublicKeyModel) Migrate() error {
	// Only using LogCountByPublicKeyRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from logCountByPublicKeys table
func (m *LogCountByPublicKeyModel) SelectOne(publicKey string) (*models.LogCountByPublicKey, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByPublicKey{})

	// PublicKey
	db = db.Where("public_key = ?", publicKey)

	logCountByPublicKey := &models.LogCountByPublicKey{}
	db = db.First(logCountByPublicKey)

	return logCountByPublicKey, db.Error
}

// Select - select from logCountByPublicKeys table
func (m *LogCountByPublicKeyModel) SelectCount(publicKey string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByPublicKey{})

	// PublicKey
	db = db.Where("public_key = ?", publicKey)

	logCountByPublicKey := &models.LogCountByPublicKey{}
	db = db.First(logCountByPublicKey)

	count := uint64(0)
	if logCountByPublicKey != nil {
		count = logCountByPublicKey.Count
	}

	return count, db.Error
}

func (m *LogCountByPublicKeyModel) UpsertOne(
	logCountByPublicKey *models.LogCountByPublicKey,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*logCountByPublicKey),
		reflect.TypeOf(*logCountByPublicKey),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(logCountByPublicKey)

	return db.Error
}

// StartLogCountByPublicKeyLoader starts loader
func StartLogCountByPublicKeyLoader() {
	go func() {
		postgresLoaderChan := GetLogCountByPublicKeyModel().LoaderChannel

		for {
			// Read log
			newLogCountByPublicKey := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_addresses_log_count_by_address_" + newLogCountByPublicKey.PublicKey

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal(
					"Loader=LogCountByPublicKey",
					" Hash=", newLogCountByPublicKey.TransactionHash,
					" Log Index=", newLogCountByPublicKey.LogIndex,
					" Public Key=", newLogCountByPublicKey.PublicKey,
					" - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curLogCountByPublicKey, err := GetLogCountByPublicKeyModel().SelectOne(newLogCountByPublicKey.PublicKey)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=LogCountByPublicKey",
						" Hash=", newLogCountByPublicKey.TransactionHash,
						" Log Index=", newLogCountByPublicKey.LogIndex,
						" Public Key=", newLogCountByPublicKey.PublicKey,
						" - Error: ", err.Error())
				} else {
					count = int64(curLogCountByPublicKey.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal(
						"Loader=LogCountByPublicKey",
						" Hash=", newLogCountByPublicKey.TransactionHash,
						" Log Index=", newLogCountByPublicKey.LogIndex,
						" Public Key=", newLogCountByPublicKey.PublicKey,
						" - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add log to indexed
			newLogCountByPublicKeyIndex := &models.LogCountByPublicKeyIndex{
				TransactionHash: newLogCountByPublicKey.TransactionHash,
				LogIndex:        newLogCountByPublicKey.LogIndex,
				PublicKey:       newLogCountByPublicKey.PublicKey,
			}
			err = GetLogCountByPublicKeyIndexModel().Insert(newLogCountByPublicKeyIndex)
			if err != nil {
				// Record already exists, continue
				continue
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal(
					"Loader=LogCountByPublicKey",
					" Hash=", newLogCountByPublicKey.TransactionHash,
					" Log Index=", newLogCountByPublicKey.LogIndex,
					" Public Key=", newLogCountByPublicKey.PublicKey,
					" - Error: ", err.Error())
			}
			newLogCountByPublicKey.Count = uint64(count)

			err = GetLogCountByPublicKeyModel().UpsertOne(newLogCountByPublicKey)
			zap.S().Debug(
				"Loader=LogCountByPublicKey",
				" Hash=", newLogCountByPublicKey.TransactionHash,
				" Log Index=", newLogCountByPublicKey.LogIndex,
				" Public Key=", newLogCountByPublicKey.PublicKey,
				" - Upsert")
			if err != nil {
				// Postgres error
				zap.S().Fatal(
					"Loader=LogCountByPublicKey",
					" Hash=", newLogCountByPublicKey.TransactionHash,
					" Log Index=", newLogCountByPublicKey.LogIndex,
					" Public Key=", newLogCountByPublicKey.PublicKey,
					" - Error: ", err.Error())
			}
		}
	}()
}
