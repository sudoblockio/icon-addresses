package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// AddressContractCountIndexModel - type for address table model
type AddressContractCountIndexModel struct {
	db            *gorm.DB
	model         *models.AddressContractCountIndex
	modelORM      *models.AddressContractCountIndexORM
	LoaderChannel chan *models.AddressContractCountIndex
}

var addressContractCountIndexModel *AddressContractCountIndexModel
var addressContractCountIndexModelOnce sync.Once

// GetAddressContractModel - create and/or return the addressContracts table model
func GetAddressContractCountIndexModel() *AddressContractCountIndexModel {
	addressContractCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		addressContractCountIndexModel = &AddressContractCountIndexModel{
			db:            dbConn,
			model:         &models.AddressContractCountIndex{},
			LoaderChannel: make(chan *models.AddressContractCountIndex, 1),
		}

		err := addressContractCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressContractCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return addressContractCountIndexModel
}

// Migrate - migrate addressContractCountIndexs table
func (m *AddressContractCountIndexModel) Migrate() error {
	// Only using AddressContractCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert addressContractCountByIndex into table
func (m *AddressContractCountIndexModel) Insert(addressContractCountIndex *models.AddressContractCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.AddressContractCountIndex{})

	db = db.Create(addressContractCountIndex)

	return db.Error
}
