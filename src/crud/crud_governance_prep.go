package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
)

// GovernancePrepProcessedModel - type for goveranancePrep table model
type GovernancePrepProcessedModel struct {
	db            *gorm.DB
	model         *models.GovernancePrepProcessed
	modelORM      *models.GovernancePrepProcessedORM
	LoaderChannel chan *models.GovernancePrepProcessed
}

var goveranancePrepModel *GovernancePrepProcessedModel
var goveranancePrepModelOnce sync.Once

// GetGovernancePrepProcessedModel - create and/or return the goveranancePreps table model
func GetGovernancePrepProcessedModel() *GovernancePrepProcessedModel {
	goveranancePrepModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		goveranancePrepModel = &GovernancePrepProcessedModel{
			db:            dbConn,
			model:         &models.GovernancePrepProcessed{},
			LoaderChannel: make(chan *models.GovernancePrepProcessed, 1),
		}

		err := goveranancePrepModel.Migrate()
		if err != nil {
			zap.S().Fatal("GovernancePrepProcessedModel: Unable migrate postgres table: ", err.Error())
		}

		StartGovernancePrepProcessedLoader()
	})

	return goveranancePrepModel
}

// Migrate - migrate goveranancePreps table
func (m *GovernancePrepProcessedModel) Migrate() error {
	// Only using GovernancePrepProcessedRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectOne - select one from addresses table
func (m *GovernancePrepProcessedModel) SelectOne(
	address string,
) (*models.GovernancePrepProcessed, error) {
	db := m.db

	// Set table
	db = db.Model(&models.GovernancePrepProcessed{})

	// Address
	db = db.Where("address = ?", address)

	goveranancePrep := &models.GovernancePrepProcessed{}
	db = db.First(goveranancePrep)

	return goveranancePrep, db.Error
}

func (m *GovernancePrepProcessedModel) UpsertOne(
	goveranancePrep *models.GovernancePrepProcessed,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*goveranancePrep),
		reflect.TypeOf(*goveranancePrep),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(goveranancePrep)

	return db.Error
}

// StartGovernancePrepProcessedLoader starts loader
func StartGovernancePrepProcessedLoader() {
	go func() {

		for {
			// Read goveranancePrep
			newGovernancePrepProcessed := <-GetGovernancePrepProcessedModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetGovernancePrepProcessedModel().UpsertOne(newGovernancePrepProcessed)
			zap.S().Debug("Loader=GovernancePrepProcessed, Address=", newGovernancePrepProcessed.Address, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=GovernancePrepProcessed, Address=", newGovernancePrepProcessed.Address, " - FATAL")
				zap.S().Fatal(err.Error())
			}

			// Force addresses enrichment
			err = reloadAddress(newGovernancePrepProcessed.Address)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
