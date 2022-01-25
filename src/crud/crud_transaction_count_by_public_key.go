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

// TransactionCountByPublicKeyModel - type for address table model
type TransactionCountByPublicKeyModel struct {
	db            *gorm.DB
	model         *models.TransactionCountByPublicKey
	modelORM      *models.TransactionCountByPublicKeyORM
	LoaderChannel chan *models.TransactionCountByPublicKey
}

var transactionCountByPublicKeyModel *TransactionCountByPublicKeyModel
var transactionCountByPublicKeyModelOnce sync.Once

// GetPublicKeyModel - create and/or return the addresss table model
func GetTransactionCountByPublicKeyModel() *TransactionCountByPublicKeyModel {
	transactionCountByPublicKeyModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountByPublicKeyModel = &TransactionCountByPublicKeyModel{
			db:            dbConn,
			model:         &models.TransactionCountByPublicKey{},
			LoaderChannel: make(chan *models.TransactionCountByPublicKey, 1),
		}

		err := transactionCountByPublicKeyModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountByPublicKeyModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionCountByPublicKeyLoader()
	})

	return transactionCountByPublicKeyModel
}

// Migrate - migrate transactionCountByPublicKeys table
func (m *TransactionCountByPublicKeyModel) Migrate() error {
	// Only using TransactionCountByPublicKeyRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from transactionCountByPublicKeys table
func (m *TransactionCountByPublicKeyModel) SelectOne(publicKey string) (*models.TransactionCountByPublicKey, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByPublicKey{})

	// PublicKey
	db = db.Where("public_key = ?", publicKey)

	transactionCountByPublicKey := &models.TransactionCountByPublicKey{}
	db = db.First(transactionCountByPublicKey)

	return transactionCountByPublicKey, db.Error
}

// SelectMany - select from transactions table
// Returns: models, error (if present)
func (m *TransactionCountByPublicKeyModel) SelectMany(
	limit int,
	skip int,
) (*[]models.TransactionCountByPublicKey, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TransactionCountByPublicKey{})

	// Limit
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactionCountByPublicKeys := &[]models.TransactionCountByPublicKey{}
	db = db.Find(transactionCountByPublicKeys)

	return transactionCountByPublicKeys, db.Error
}

// Select - select from transactionCountByPublicKeys table
func (m *TransactionCountByPublicKeyModel) SelectCount(publicKey string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByPublicKey{})

	// PublicKey
	db = db.Where("public_key = ?", publicKey)

	transactionCountByPublicKey := &models.TransactionCountByPublicKey{}
	db = db.First(transactionCountByPublicKey)

	count := uint64(0)
	if transactionCountByPublicKey != nil {
		count = transactionCountByPublicKey.Count
	}

	return count, db.Error
}

func (m *TransactionCountByPublicKeyModel) UpsertOne(
	transactionCountByPublicKey *models.TransactionCountByPublicKey,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionCountByPublicKey),
		reflect.TypeOf(*transactionCountByPublicKey),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionCountByPublicKey)

	return db.Error
}

// StartTransactionCountByPublicKeyLoader starts loader
func StartTransactionCountByPublicKeyLoader() {
	go func() {
		postgresLoaderChan := GetTransactionCountByPublicKeyModel().LoaderChannel

		for {
			// Read transaction
			newTransactionCountByPublicKey := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_addresses_transaction_count_by_address_" + newTransactionCountByPublicKey.PublicKey

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByPublicKey.TransactionHash, " PublicKey=", newTransactionCountByPublicKey.PublicKey, " - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curTransactionCountByPublicKey, err := GetTransactionCountByPublicKeyModel().SelectOne(newTransactionCountByPublicKey.PublicKey)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTransactionCountByPublicKey.TransactionHash,
						" PublicKey=", newTransactionCountByPublicKey.PublicKey,
						" - Error: ", err.Error())
				} else {
					count = int64(curTransactionCountByPublicKey.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByPublicKey.TransactionHash, " PublicKey=", newTransactionCountByPublicKey.PublicKey, " - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add transaction to indexed
			newTransactionCountByPublicKeyIndex := &models.TransactionCountByPublicKeyIndex{
				TransactionHash: newTransactionCountByPublicKey.TransactionHash,
				PublicKey:       newTransactionCountByPublicKey.PublicKey,
			}
			err = GetTransactionCountByPublicKeyIndexModel().Insert(newTransactionCountByPublicKeyIndex)
			if err != nil {
				// Record already exists, continue
				continue
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByPublicKey.TransactionHash, " PublicKey=", newTransactionCountByPublicKey.PublicKey, " - Error: ", err.Error())
			}
			newTransactionCountByPublicKey.Count = uint64(count)

			err = GetTransactionCountByPublicKeyModel().UpsertOne(newTransactionCountByPublicKey)
			zap.S().Debug("Loader=Transaction, Hash=", newTransactionCountByPublicKey.TransactionHash, " PublicKey=", newTransactionCountByPublicKey.PublicKey, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByPublicKey.TransactionHash, " PublicKey=", newTransactionCountByPublicKey.PublicKey, " - Error: ", err.Error())
			}
		}
	}()
}
