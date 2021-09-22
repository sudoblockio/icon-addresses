package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-logs/models"
)

// LogModel - type for log table model
type LogModel struct {
	db            *gorm.DB
	model         *models.Log
	modelORM      *models.LogORM
	LoaderChannel chan *models.Log
}

var logModel *LogModel
var logModelOnce sync.Once

// GetLogModel - create and/or return the logs table model
func GetLogModel() *LogModel {
	logModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logModel = &LogModel{
			db:            dbConn,
			model:         &models.Log{},
			LoaderChannel: make(chan *models.Log, 1),
		}

		err := logModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogModel: Unable migrate postgres table: ", err.Error())
		}

		StartLogLoader()
	})

	return logModel
}

// Migrate - migrate logs table
func (m *LogModel) Migrate() error {
	// Only using LogRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert log into table
func (m *LogModel) Insert(log *models.Log) error {
	db := m.db

	// Set table
	db = db.Model(&models.Log{})

	db = db.Create(log)

	return db.Error
}

// Select - select from logs table
// Returns: models, total count (if filters), error (if present)
func (m *LogModel) SelectMany(
	limit int,
	skip int,
	blockNumber uint32,
	blockStartNumber uint32,
	blockEndNumber uint32,
	transactionHash string,
	scoreAddress string,
) (*[]models.Log, int64, error) {
	db := m.db
	computeCount := false

	// Set table
	db = db.Model(&models.Log{})

	// Latest logs first
	db = db.Order("block_number desc")

	// Number
	if blockNumber != 0 {
		computeCount = true
		db = db.Where("block_number = ?", blockNumber)
	}

	// Start number and end number
	if blockStartNumber != 0 && blockEndNumber != 0 {
		computeCount = true
		db = db.Where("block_number BETWEEN ? AND ?", blockStartNumber, blockEndNumber)
	} else if blockStartNumber != 0 {
		computeCount = true
		db = db.Where("block_number > ?", blockStartNumber)
	} else if blockEndNumber != 0 {
		computeCount = true
		db = db.Where("block_number < ?", blockEndNumber)
	}

	// Hash
	if transactionHash != "" {
		computeCount = true
		db = db.Where("transaction_hash = ?", transactionHash)
	}

	// Address
	if scoreAddress != "" {
		// NOTE: addresses many have large counts, use log_count_by_addresses
		db = db.Where("address = ?", scoreAddress)
	}

	// Count, if needed
	count := int64(-1)
	if computeCount {
		db.Count(&count)
	}

	// Limit is required and defaulted to 1
	// Note: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// Note: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	logs := &[]models.Log{}
	db = db.Find(logs)

	return logs, count, db.Error
}

func (m *LogModel) SelectOne(
	transactionHash string,
	logIndex uint64,
) (*models.Log, error) {
	db := m.db

	// Set table
	db = db.Model(&models.Log{})

	db = db.Where("transaction_hash = ?", transactionHash)

	db = db.Where("log_index = ?", logIndex)

	log := &models.Log{}
	db = db.First(log)

	return log, db.Error
}

// UpdateOne - select from logs table
func (m *LogModel) UpdateOne(
	log *models.Log,
) error {
	db := m.db

	// Set table
	db = db.Model(&models.Log{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", log.TransactionHash)

	// Log Index
	db = db.Where("log_index = ?", log.LogIndex)

	db = db.Save(log)

	return db.Error
}

// StartLogLoader starts loader
func StartLogLoader() {
	go func() {

		for {
			// Read transaction
			newLog := <-GetLogModel().LoaderChannel

			// Update/Insert
			_, err := GetLogModel().SelectOne(newLog.TransactionHash, newLog.LogIndex)
			if errors.Is(err, gorm.ErrRecordNotFound) {

				// Insert
				GetLogModel().Insert(newLog)
			} else if err == nil {
				// Update
				GetLogModel().UpdateOne(newLog)
				zap.S().Debug("Loader=Log, TransactionHash=", newLog.TransactionHash, " LogIndex=", newLog.LogIndex, " - Updated")
			} else {
				// Postgress error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
