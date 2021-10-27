package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// TransactionCountByPublicKeyIndexModel - type for address table model
type TransactionCountByPublicKeyIndexModel struct {
	db            *gorm.DB
	model         *models.TransactionCountByPublicKeyIndex
	modelORM      *models.TransactionCountByPublicKeyIndexORM
	LoaderChannel chan *models.TransactionCountByPublicKeyIndex
}

var transactionCountByPublicKeyIndexModel *TransactionCountByPublicKeyIndexModel
var transactionCountByPublicKeyIndexModelOnce sync.Once

// GetPublicKeyModel - create and/or return the addresss table model
func GetTransactionCountByPublicKeyIndexModel() *TransactionCountByPublicKeyIndexModel {
	transactionCountByPublicKeyIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountByPublicKeyIndexModel = &TransactionCountByPublicKeyIndexModel{
			db:            dbConn,
			model:         &models.TransactionCountByPublicKeyIndex{},
			LoaderChannel: make(chan *models.TransactionCountByPublicKeyIndex, 1),
		}

		err := transactionCountByPublicKeyIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountByPublicKeyIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionCountByPublicKeyIndexModel
}

// Migrate - migrate transactionCountByPublicKeyIndexs table
func (m *TransactionCountByPublicKeyIndexModel) Migrate() error {
	// Only using TransactionCountByPublicKeyIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByIndex into table
func (m *TransactionCountByPublicKeyIndexModel) Insert(transactionCountByPublicKeyIndex *models.TransactionCountByPublicKeyIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByPublicKeyIndex{})

	db = db.Create(transactionCountByPublicKeyIndex)

	return db.Error
}
