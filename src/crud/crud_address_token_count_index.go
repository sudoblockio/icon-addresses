package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// AddressTokenCountIndexModel - type for address table model
type AddressTokenCountIndexModel struct {
	db            *gorm.DB
	model         *models.AddressTokenCountIndex
	modelORM      *models.AddressTokenCountIndexORM
	LoaderChannel chan *models.AddressTokenCountIndex
}

var addressTokenCountIndexModel *AddressTokenCountIndexModel
var addressTokenCountIndexModelOnce sync.Once

// GetAddressTokenModel - create and/or return the addressTokens table model
func GetAddressTokenCountIndexModel() *AddressTokenCountIndexModel {
	addressTokenCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		addressTokenCountIndexModel = &AddressTokenCountIndexModel{
			db:            dbConn,
			model:         &models.AddressTokenCountIndex{},
			LoaderChannel: make(chan *models.AddressTokenCountIndex, 1),
		}

		err := addressTokenCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressTokenCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return addressTokenCountIndexModel
}

// Migrate - migrate addressTokenCountIndexs table
func (m *AddressTokenCountIndexModel) Migrate() error {
	// Only using AddressTokenCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert addressTokenCountByIndex into table
func (m *AddressTokenCountIndexModel) Insert(addressTokenCountIndex *models.AddressTokenCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.AddressTokenCountIndex{})

	db = db.Create(addressTokenCountIndex)

	return db.Error
}
