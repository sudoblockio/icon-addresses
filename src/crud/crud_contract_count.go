package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-addresses/models"
)

// ContractCountModel - type for contract_count table model
type ContractCountModel struct {
	db            *gorm.DB
	model         *models.ContractCount
	modelORM      *models.ContractCountORM
	LoaderChannel chan *models.ContractCount
}

var contractCountModel *ContractCountModel
var contractCountModelOnce sync.Once

// GetContactModel - create and/or return the contract_count table model
func GetContractCountModel() *ContractCountModel {
	contractCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		contractCountModel = &ContractCountModel{
			db:            dbConn,
			model:         &models.ContractCount{},
			LoaderChannel: make(chan *models.ContractCount, 1),
		}

		err := contractCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("ContractCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartContractCountLoader()
	})

	return contractCountModel
}

// Migrate - migrate contractCounts table
func (m *ContractCountModel) Migrate() error {
	// Only using ContractCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert contractCount into table
func (m *ContractCountModel) Insert(contractCount *models.ContractCount) error {
	db := m.db

	// Set table
	db = db.Model(&models.ContractCount{})

	db = db.Create(contractCount)

	return db.Error
}

// Select - select from contractCount table
func (m *ContractCountModel) SelectOne(publicKey string) (models.ContractCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.ContractCount{})

	contractCount := models.ContractCount{}

	// Transaction Hash
	db = db.Where("public_key = ?", publicKey)

	db = db.First(&contractCount)

	return contractCount, db.Error
}

func (m *ContractCountModel) SelectLargestCount() (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.ContractCount{})

	// Get max id
	count := uint64(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartContractCountLoader starts loader
func StartContractCountLoader() {
	go func() {

		for {
			// Read contractCount
			newContractCount := <-GetContractCountModel().LoaderChannel

			_, err := GetContractCountModel().SelectOne(
				newContractCount.PublicKey,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetContractCountModel().Insert(newContractCount)
				if err != nil {
					zap.S().Warn("Loader=ContractCount, Address=", newContractCount.PublicKey, " - Error: ", err.Error())
				}

				zap.S().Debug("Loader=ContractCount, Address=", newContractCount.PublicKey, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
