package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// LogCountByBlockNumberModel - type for address table model
type LogCountByBlockNumberModel struct {
	db            *gorm.DB
	model         *models.LogCountByBlockNumber
	modelORM      *models.LogCountByBlockNumberORM
	LoaderChannel chan *models.LogCountByBlockNumber
}

var logCountByBlockNumberModel *LogCountByBlockNumberModel
var logCountByBlockNumberModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetLogCountByBlockNumberModel() *LogCountByBlockNumberModel {
	logCountByBlockNumberModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logCountByBlockNumberModel = &LogCountByBlockNumberModel{
			db:            dbConn,
			model:         &models.LogCountByBlockNumber{},
			LoaderChannel: make(chan *models.LogCountByBlockNumber, 1),
		}

		err := logCountByBlockNumberModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogCountByBlockNumberModel: Unable migrate postgres table: ", err.Error())
		}

		StartLogCountByBlockNumberLoader()
	})

	return logCountByBlockNumberModel
}

// Migrate - migrate logCountByBlockNumbers table
func (m *LogCountByBlockNumberModel) Migrate() error {
	// Only using LogCountByBlockNumberRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert logCountByBlockNumber into table
func (m *LogCountByBlockNumberModel) Insert(logCountByBlockNumber *models.LogCountByBlockNumber) error {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByBlockNumber{})

	db = db.Create(logCountByBlockNumber)

	return db.Error
}

// Select - select from logCountByBlockNumbers table
func (m *LogCountByBlockNumberModel) SelectOne(transactionHash string, logIndex uint64) (*models.LogCountByBlockNumber, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByBlockNumber{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	logCountByBlockNumber := &models.LogCountByBlockNumber{}
	db = db.First(logCountByBlockNumber)

	return logCountByBlockNumber, db.Error
}

// SelectManyByBlockNumber - select from logCountByBlockNumbers table
func (m *LogCountByBlockNumberModel) SelectManyByBlockNumber(blockNumber uint64) ([]models.LogCountByBlockNumber, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByBlockNumber{})

	// Block number
	db = db.Where("block_number = ?", blockNumber)

	logCountByBlockNumber := []models.LogCountByBlockNumber{}
	db = db.Find(&logCountByBlockNumber)

	return logCountByBlockNumber, db.Error
}

func (m *LogCountByBlockNumberModel) SelectLargestCountByBlockNumber(blockNumber uint64) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByBlockNumber{})

	// Block Number
	db = db.Where("block_number = ?", blockNumber)

	// Get max id
	count := uint64(0)
	row := db.Select("max(count)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartLogCountByBlockNumberLoader starts loader
func StartLogCountByBlockNumberLoader() {
	go func() {

		for {
			// Read logCountByBlockNumber
			newLogCountByBlockNumber := <-GetLogCountByBlockNumberModel().LoaderChannel

			// Insert
			_, err := GetLogCountByBlockNumberModel().SelectOne(
				newLogCountByBlockNumber.TransactionHash,
				newLogCountByBlockNumber.LogIndex,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Last count
				lastCount, err := GetLogCountByBlockNumberModel().SelectLargestCountByBlockNumber(
					newLogCountByBlockNumber.BlockNumber,
				)
				if err != nil {
					zap.S().Fatal(err.Error())
				}
				newLogCountByBlockNumber.Count = uint32(lastCount + 1)

				// Insert
				err = GetLogCountByBlockNumberModel().Insert(newLogCountByBlockNumber)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=LogCountByBlockNumber, BlockNumber=", newLogCountByBlockNumber.BlockNumber, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}

			///////////////////////
			// Force enrichments //
			///////////////////////
			err = reloadBlock(uint32(newLogCountByBlockNumber.BlockNumber))
			if err != nil {
				// Postgress error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
