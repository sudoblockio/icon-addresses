package crud

import (
	"reflect"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
)

// TransactionModel - type for transaction table model
type TransactionModel struct {
	db            *gorm.DB
	model         *models.Transaction
	modelORM      *models.TransactionORM
	LoaderChannel chan *models.Transaction
}

var transactionModel *TransactionModel
var transactionModelOnce sync.Once

// GetTransactionModel - create and/or return the transactions table model
func GetTransactionModel() *TransactionModel {
	transactionModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionModel = &TransactionModel{
			db:            dbConn,
			model:         &models.Transaction{},
			LoaderChannel: make(chan *models.Transaction, 1),
		}

		err := transactionModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionLoader()
	})

	return transactionModel
}

// Migrate - migrate transactions table
func (m *TransactionModel) Migrate() error {
	// Only using TransactionRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transaction into table
func (m *TransactionModel) Insert(transaction *models.Transaction) error {

	err := backoff.Retry(func() error {
		query := m.db.Create(transaction)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// UpdateOne - update one from transactions table
func (m *TransactionModel) UpdateOne(
	transaction *models.Transaction,
) error {
	db := m.db

	// Set table
	db = db.Model(&models.Transaction{})

	// Hash
	db = db.Where("hash = ?", transaction.Hash)

	// Log Index
	db = db.Where("log_index = ?", transaction.LogIndex)

	db = db.Save(transaction)

	return db.Error
}

func (m *TransactionModel) UpsertOne(
	transaction *models.Transaction,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transaction),
		reflect.TypeOf(*transaction),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}, {Name: "log_index"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transaction)

	return db.Error
}

// StartTransactionLoader starts loader
func StartTransactionLoader() {
	go func() {
		postgresLoaderChan := GetTransactionModel().LoaderChannel

		for {
			// Read transaction
			newTransaction := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTransactionModel().UpsertOne(newTransaction)
			zap.S().Debug("Loader=Transaction, Hash=", newTransaction.Hash, " LogIndex=", newTransaction.LogIndex, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=Transaction, Hash=", newTransaction.Hash, " LogIndex=", newTransaction.LogIndex, " - FATAL")
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
