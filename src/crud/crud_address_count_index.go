package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// AddressCountIndexModel - type for address table model
type AddressCountIndexModel struct {
	db            *gorm.DB
	model         *models.AddressCountIndex
	modelORM      *models.AddressCountIndexORM
	LoaderChannel chan *models.AddressCountIndex
}

var addressCountIndexModel *AddressCountIndexModel
var addressCountIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetAddressCountIndexModel() *AddressCountIndexModel {
	addressCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		addressCountIndexModel = &AddressCountIndexModel{
			db:            dbConn,
			model:         &models.AddressCountIndex{},
			LoaderChannel: make(chan *models.AddressCountIndex, 1),
		}

		err := addressCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return addressCountIndexModel
}

// Migrate - migrate addressCountIndexs table
func (m *AddressCountIndexModel) Migrate() error {
	// Only using AddressCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert addressCountByIndex into table
func (m *AddressCountIndexModel) Insert(addressCountIndex *models.AddressCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.AddressCountIndex{})

	db = db.Create(addressCountIndex)

	return db.Error
}
