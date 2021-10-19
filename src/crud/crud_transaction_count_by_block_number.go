package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// TransactionCountByBlockNumberModel - type for address table model
type TransactionCountByBlockNumberModel struct {
	db            *gorm.DB
	model         *models.TransactionCountByBlockNumber
	modelORM      *models.TransactionCountByBlockNumberORM
	LoaderChannel chan *models.TransactionCountByBlockNumber
}

var transactionCountByBlockNumberModel *TransactionCountByBlockNumberModel
var transactionCountByBlockNumberModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionCountByBlockNumberModel() *TransactionCountByBlockNumberModel {
	transactionCountByBlockNumberModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountByBlockNumberModel = &TransactionCountByBlockNumberModel{
			db:            dbConn,
			model:         &models.TransactionCountByBlockNumber{},
			LoaderChannel: make(chan *models.TransactionCountByBlockNumber, 1),
		}

		err := transactionCountByBlockNumberModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountByBlockNumberModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionCountByBlockNumberLoader()
	})

	return transactionCountByBlockNumberModel
}

// Migrate - migrate transactionCountByBlockNumbers table
func (m *TransactionCountByBlockNumberModel) Migrate() error {
	// Only using TransactionCountByBlockNumberRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByBlockNumber into table
func (m *TransactionCountByBlockNumberModel) Insert(transactionCountByBlockNumber *models.TransactionCountByBlockNumber) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByBlockNumber{})

	db = db.Create(transactionCountByBlockNumber)

	return db.Error
}

// Select - select from transactionCountByBlockNumbers table
func (m *TransactionCountByBlockNumberModel) SelectOne(transactionHash string) (models.TransactionCountByBlockNumber, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByBlockNumber{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	transactionCountByBlockNumber := models.TransactionCountByBlockNumber{}
	db = db.First(&transactionCountByBlockNumber)

	return transactionCountByBlockNumber, db.Error
}

func (m *TransactionCountByBlockNumberModel) SelectLargestCountByBlockNumber(blockNumber uint64) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByBlockNumber{})

	// Block Number
	db = db.Where("block_number = ?", blockNumber)

	// Get max id
	count := uint64(0)
	row := db.Select("max(count)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartTransactionCountByBlockNumberLoader starts loader
func StartTransactionCountByBlockNumberLoader() {
	go func() {

		for {
			// Read transactionCountByBlockNumber
			newTransactionCountByBlockNumber := <-GetTransactionCountByBlockNumberModel().LoaderChannel

			// Insert
			_, err := GetTransactionCountByBlockNumberModel().SelectOne(
				newTransactionCountByBlockNumber.TransactionHash,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Last count
				lastCount, err := GetTransactionCountByBlockNumberModel().SelectLargestCountByBlockNumber(
					newTransactionCountByBlockNumber.BlockNumber,
				)
				if err != nil {
					zap.S().Fatal(err.Error())
				}
				newTransactionCountByBlockNumber.Count = uint32(lastCount + 1)

				// Insert
				err = GetTransactionCountByBlockNumberModel().Insert(newTransactionCountByBlockNumber)
				if err != nil {
					zap.S().Warn("Loader=TransactionCountByBlockNumber, BlockNumber=", newTransactionCountByBlockNumber.BlockNumber, " - Error: ", err.Error())
				}

				zap.S().Debug("Loader=TransactionCountByBlockNumber, BlockNumber=", newTransactionCountByBlockNumber.BlockNumber, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
