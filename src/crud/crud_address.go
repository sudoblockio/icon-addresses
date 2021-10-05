package crud

import (
	"errors"
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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
	db = db.Where("public_key = ?", address.PublicKey)

	db = db.Save(address)

	return db.Error
}

// SelectOne - select one from addresses table
func (m *AddressModel) SelectOne(
	publicKey string,
) (*models.Address, error) {
	db := m.db

	// Set table
	db = db.Model(&models.Address{})

	// Public Key
	db = db.Where("public_key = ?", publicKey)

	address := &models.Address{}
	db = db.First(address)

	return address, db.Error
}

// SelectMany - select many from addreses table
func (m *AddressModel) SelectMany(
	limit int,
	skip int,
	isContract bool,
) (*[]models.Address, error) {
	db := m.db

	// Set table
	db = db.Model(&models.Address{})

	db = db.Where("is_contract = ?", isContract)

	// Limit
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	addresses := &[]models.Address{}
	db = db.Find(addresses)

	return addresses, db.Error
}

func (m *AddressModel) UpsertOne(
	address *models.Address,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*address),
		reflect.TypeOf(*address),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "public_key"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(address)

	return db.Error
}

// StartAddressLoader starts loader
func StartAddressLoader() {
	go func() {

		for {
			// Read address
			newAddress := <-GetAddressModel().LoaderChannel

			/////////////////
			// Enrichments //
			/////////////////
			transactionCount := uint64(0)
			logCount := uint64(0)

			//////////////////////////////////
			// Transaction Count By Address //
			//////////////////////////////////

			// transaction count
			count, err := GetTransactionCountByAddressModel().SelectLargestCountByPublicKey(
				newAddress.PublicKey,
			)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
			transactionCount = count

			//////////////////////////
			// Log Count By Address //
			//////////////////////////

			// transaction count
			count, err = GetLogCountByAddressModel().SelectLargestCountByPublicKey(
				newAddress.PublicKey,
			)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
			logCount = count

			newAddress.TransactionCount = transactionCount
			newAddress.LogCount = logCount

			//////////////////////
			// Load to postgres //
			//////////////////////
			err = GetAddressModel().UpsertOne(newAddress)
			zap.S().Debug("Loader=Address, Address=", newAddress.PublicKey, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=Address, Address=", newAddress.PublicKey, " - FATAL")
				zap.S().Fatal(err.Error())
			}

		}
	}()
}

// reloadAddress - Send address back to loader for updates
func reloadAddress(publicKey string) error {

	curAddress, err := GetAddressModel().SelectOne(publicKey)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create empty block
		curAddress = &models.Address{}
		curAddress.PublicKey = publicKey
	} else if err != nil {
		// Postgres error
		return err
	}
	GetAddressModel().LoaderChannel <- curAddress

	return nil
}
