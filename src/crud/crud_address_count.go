package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// AddressCountModel - type for address table model
type AddressCountModel struct {
	db            *gorm.DB
	model         *models.AddressCount
	modelORM      *models.AddressCountORM
	LoaderChannel chan *models.AddressCount
}

var addressCountModel *AddressCountModel
var addressCountModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetAddressCountModel() *AddressCountModel {
	addressCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		addressCountModel = &AddressCountModel{
			db:            dbConn,
			model:         &models.AddressCount{},
			LoaderChannel: make(chan *models.AddressCount, 1),
		}

		err := addressCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartAddressCountLoader()
	})

	return addressCountModel
}

// Migrate - migrate addressCounts table
func (m *AddressCountModel) Migrate() error {
	// Only using AddressCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert addressCount into table
func (m *AddressCountModel) Insert(addressCount *models.AddressCount) error {
	db := m.db

	// Set table
	db = db.Model(&models.AddressCount{})

	db = db.Create(addressCount)

	return db.Error
}

// Select - select from addressCounts table
func (m *AddressCountModel) SelectOne(publicKey string) (models.AddressCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.AddressCount{})

	addressCount := models.AddressCount{}

	// Transaction Hash
	db = db.Where("public_key = ?", publicKey)

	db = db.First(&addressCount)

	return addressCount, db.Error
}

func (m *AddressCountModel) SelectLargestCount() (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.AddressCount{})

	// Get max id
	count := uint64(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartAddressCountLoader starts loader
func StartAddressCountLoader() {
	go func() {

		for {
			// Read addressCount
			newAddressCount := <-GetAddressCountModel().LoaderChannel

			// Insert
			_, err := GetAddressCountModel().SelectOne(
				newAddressCount.PublicKey,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetAddressCountModel().Insert(newAddressCount)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=AddressCount, Address=", newAddressCount.PublicKey, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
