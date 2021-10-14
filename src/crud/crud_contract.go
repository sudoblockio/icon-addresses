package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
)

// ContractModel - type for contract table model
type ContractModel struct {
	db            *gorm.DB
	model         *models.ContractProcessed
	modelORM      *models.ContractProcessedORM
	LoaderChannel chan *models.ContractProcessed
}

var contractModel *ContractModel
var contractModelOnce sync.Once

// GetContractModel - create and/or return the contracts table model
func GetContractModel() *ContractModel {
	contractModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		contractModel = &ContractModel{
			db:            dbConn,
			model:         &models.ContractProcessed{},
			LoaderChannel: make(chan *models.ContractProcessed, 1),
		}

		err := contractModel.Migrate()
		if err != nil {
			zap.S().Fatal("ContractModel: Unable migrate postgres table: ", err.Error())
		}

		StartContractLoader()
	})

	return contractModel
}

// Migrate - migrate contracts table
func (m *ContractModel) Migrate() error {
	// Only using ContractRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectOne - select one from addresses table
func (m *ContractModel) SelectOne(
	address string,
) (*models.ContractProcessed, error) {
	db := m.db

	// Set table
	db = db.Model(&models.ContractProcessed{})

	// Address
	db = db.Where("address = ?", address)

	contract := &models.ContractProcessed{}
	db = db.First(contract)

	return contract, db.Error
}

func (m *ContractModel) UpsertOne(
	contract *models.ContractProcessed,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*contract),
		reflect.TypeOf(*contract),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(contract)

	return db.Error
}

// StartContractLoader starts loader
func StartContractLoader() {
	go func() {

		for {
			// Read contract
			newContract := <-GetContractModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetContractModel().UpsertOne(newContract)
			zap.S().Debug("Loader=Contract, Address=", newContract.Address, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=Contract, Address=", newContract.Address, " - FATAL")
				zap.S().Fatal(err.Error())
			}

			// Force addresses enrichment
			err = reloadAddress(newContract.Address)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
