package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// LogCountByPublicKeyIndexModel - type for address table model
type LogCountByPublicKeyIndexModel struct {
	db            *gorm.DB
	model         *models.LogCountByPublicKeyIndex
	modelORM      *models.LogCountByPublicKeyIndexORM
	LoaderChannel chan *models.LogCountByPublicKeyIndex
}

var logCountByPublicKeyIndexModel *LogCountByPublicKeyIndexModel
var logCountByPublicKeyIndexModelOnce sync.Once

// GetPublicKeyModel - create and/or return the addresss table model
func GetLogCountByPublicKeyIndexModel() *LogCountByPublicKeyIndexModel {
	logCountByPublicKeyIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logCountByPublicKeyIndexModel = &LogCountByPublicKeyIndexModel{
			db:            dbConn,
			model:         &models.LogCountByPublicKeyIndex{},
			LoaderChannel: make(chan *models.LogCountByPublicKeyIndex, 1),
		}

		err := logCountByPublicKeyIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogCountByPublicKeyIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return logCountByPublicKeyIndexModel
}

// Migrate - migrate logCountByPublicKeyIndexs table
func (m *LogCountByPublicKeyIndexModel) Migrate() error {
	// Only using LogCountByPublicKeyIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert logCountByIndex into table
func (m *LogCountByPublicKeyIndexModel) Insert(logCountByPublicKeyIndex *models.LogCountByPublicKeyIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.LogCountByPublicKeyIndex{})

	db = db.Create(logCountByPublicKeyIndex)

	return db.Error
}
