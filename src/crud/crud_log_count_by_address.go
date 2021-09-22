package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-logs/models"
)

// LogCountByAddressModel - type for logCountByAddress table model
type LogCountByAddressModel struct {
	db            *gorm.DB
	model         *models.LogCountByAddress
	modelORM      *models.LogCountByAddressORM
	LoaderChannel chan *models.LogCountByAddress
}

var logCountByAddressModel *LogCountByAddressModel
var logCountByAddressModelOnce sync.Once

// GetLogCountByAddressModel - create and/or return the logCountByAddresss table model
func GetLogCountByAddressModel() *LogCountByAddressModel {
	logCountByAddressModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logCountByAddressModel = &LogCountByAddressModel{
			db:            dbConn,
			model:         &models.LogCountByAddress{},
			LoaderChannel: make(chan *models.LogCountByAddress, 1),
		}

		err := logCountByAddressModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogCountByAddressModel: Unable migrate postgres table: ", err.Error())
		}

		StartLogCountByAddressLoader()
	})

	return logCountByAddressModel
}

// Migrate - migrate logCountByAddresss table
func (m *LogCountByAddressModel) Migrate() error {
	// Only using LogCountByAddressRawORM (ORM version of the proto generated struct) to create the TABLE
	zap.S().Info("Migrating LogCountByAddressModel....")
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert logCountByAddress into table
func (m *LogCountByAddressModel) Insert(logCountByAddress *models.LogCountByAddress) error {

	db := m.db

	// Set table
	db = db.Model(&models.LogCountByAddress{})

	db = db.Create(logCountByAddress)

	return db.Error
}

func (m *LogCountByAddressModel) SelectLargestCountByAddress(address string) (uint64, error) {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.LogCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	// Get max count
	count := uint64(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

func (m *LogCountByAddressModel) SelectOne(transactionHash string, logIndex uint64) (*models.LogCountByAddress, error) {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.LogCountByAddress{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	// Select
	logCountByAddress := &models.LogCountByAddress{}
	db = db.First(logCountByAddress)

	return logCountByAddress, db.Error
}

func (m *LogCountByAddressModel) Update(logCountByAddress *models.LogCountByAddress) error {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.LogCountByAddress{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", logCountByAddress.TransactionHash)

	// Log Index
	db = db.Where("log_index = ?", logCountByAddress.LogIndex)

	// Update
	db = db.Save(logCountByAddress)

	return db.Error
}

// StartLogCountByAddressLoader starts loader
func StartLogCountByAddressLoader() {
	go func() {

		for {
			// Read transaction
			newLogCountByAddress := <-GetLogCountByAddressModel().LoaderChannel

			// Insert
			_, err := GetLogCountByAddressModel().SelectOne(
				newLogCountByAddress.TransactionHash,
				newLogCountByAddress.LogIndex,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetLogCountByAddressModel().Insert(newLogCountByAddress)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=LogCountByAddress, TransactionHash=", newLogCountByAddress.TransactionHash, " LogIndex=", newLogCountByAddress.LogIndex, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
