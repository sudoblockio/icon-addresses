package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-logs/models"
)

// LogCountModel - type for log table model
type LogCountModel struct {
	db            *gorm.DB
	model         *models.LogCount
	modelORM      *models.LogCountORM
	LoaderChannel chan *models.LogCount
}

var logCountModel *LogCountModel
var logCountModelOnce sync.Once

// GetLogModel - create and/or return the logs table model
func GetLogCountModel() *LogCountModel {
	logCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logCountModel = &LogCountModel{
			db:            dbConn,
			model:         &models.LogCount{},
			LoaderChannel: make(chan *models.LogCount, 1),
		}

		err := logCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartLogCountLoader()
	})

	return logCountModel
}

// Migrate - migrate logCounts table
func (m *LogCountModel) Migrate() error {
	// Only using LogCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert logCount into table
func (m *LogCountModel) Insert(logCount *models.LogCount) error {
	db := m.db

	// Set table
	db = db.Model(&models.LogCount{})

	db = db.Create(logCount)

	return db.Error
}

// Select - select from logCounts table
func (m *LogCountModel) SelectOne(transactionHash string, logIndex uint64) (models.LogCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogCount{})

	logCount := models.LogCount{}

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	db = db.First(&logCount)

	return logCount, db.Error
}

func (m *LogCountModel) SelectLargestCount() (uint64, error) {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.LogCount{})

	// Get max id
	count := uint64(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

func (m *LogCountModel) Update(logCount *models.LogCount) error {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.LogCount{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", logCount.TransactionHash)

	// Log Index
	db = db.Where("log_index = ?", logCount.LogIndex)

	// Update
	db = db.Save(logCount)

	return db.Error
}

// StartLogCountLoader starts loader
func StartLogCountLoader() {
	go func() {

		for {
			// Read logCount
			newLogCount := <-GetLogCountModel().LoaderChannel

			// Insert
			_, err := GetLogCountModel().SelectOne(
				newLogCount.TransactionHash,
				newLogCount.LogIndex,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetLogCountModel().Insert(newLogCount)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=LogCount, TransactionHash=", newLogCount.TransactionHash, " LogIndex=", newLogCount.LogIndex, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
