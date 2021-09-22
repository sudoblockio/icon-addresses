package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// AddressModel - type for log table model
type AddressModel struct {
	db            *gorm.DB
	model         *models.Address
	modelORM      *models.AddressORM
	LoaderChannel chan *models.Address
}

var logModel *AddressModel
var logModelOnce sync.Once

// GetAddressModel - create and/or return the logs table model
func GetAddressModel() *AddressModel {
	logModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logModel = &AddressModel{
			db:            dbConn,
			model:         &models.Address{},
			LoaderChannel: make(chan *models.Address, 1),
		}

		err := logModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressModel: Unable migrate postgres table: ", err.Error())
		}

		StartAddressLoader()
	})

	return logModel
}

// Migrate - migrate logs table
func (m *AddressModel) Migrate() error {
	// Only using AddressRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert log into table
func (m *AddressModel) Insert(log *models.Address) error {
	db := m.db

	// Set table
	db = db.Model(&models.Address{})

	db = db.Create(log)

	return db.Error
}

// UpdateOne - update one from addresses table
func (m *AddressModel) UpdateOne(
	address *models.Address,
) error {
	db := m.db

	// Set table
	db = db.Model(&models.Address{})

	// Address
	db = db.Where("address = ?", address.Address)

	db = db.Save(address)

	return db.Error
}

// StartAddressLoader starts loader
func StartAddressLoader() {
	go func() {

		for {
			// Read transaction
			newAddress := <-GetAddressModel().LoaderChannel

			// Update/Insert
			_, err := GetAddressModel().SelectOne(newAddress.Address)
			if errors.Is(err, gorm.ErrRecordNotFound) {

				// Insert
				GetAddressModel().Insert(newAddress)
			} else if err == nil {
				// Update
				GetAddressModel().UpdateOne(newAddress)
				zap.S().Debug("Loader=Address, Address=", newAddress.Address, " - Updated")
			} else {
				// Postgress error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
