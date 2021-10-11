package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
)

// AddressTokenModel - type for addressToken table model
type AddressTokenModel struct {
	db            *gorm.DB
	model         *models.AddressToken
	modelORM      *models.AddressTokenORM
	LoaderChannel chan *models.AddressToken
}

var addressTokenModel *AddressTokenModel
var addressTokenModelOnce sync.Once

// GetAddressTokenModel - create and/or return the addressTokens table model
func GetAddressTokenModel() *AddressTokenModel {
	addressTokenModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		addressTokenModel = &AddressTokenModel{
			db:            dbConn,
			model:         &models.AddressToken{},
			LoaderChannel: make(chan *models.AddressToken, 1),
		}

		err := addressTokenModel.Migrate()
		if err != nil {
			zap.S().Fatal("AddressTokenModel: Unable migrate postgres table: ", err.Error())
		}

		StartAddressTokenLoader()
	})

	return addressTokenModel
}

// Migrate - migrate addressTokens table
func (m *AddressTokenModel) Migrate() error {
	// Only using AddressTokenRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectTokensByPublicKey - select token contract addresses by public_key
func (m *AddressTokenModel) SelectManyByPublicKey(
	publicKey string,
) (*[]models.AddressToken, error) {
	db := m.db

	// Set table
	db = db.Model(&models.AddressToken{})

	// Public key
	db = db.Where("public_key = ?", publicKey)

	addressTokens := &[]models.AddressToken{}
	db = db.Find(addressTokens)

	return addressTokens, db.Error
}

func (m *AddressTokenModel) UpsertOne(
	address *models.AddressToken,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*address),
		reflect.TypeOf(*address),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "public_key"},
			{Name: "token_contract_address"},
		}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(address)

	return db.Error
}

// StartAddressTokenLoader starts loader
func StartAddressTokenLoader() {
	go func() {

		for {
			// Read address
			newAddressToken := <-GetAddressTokenModel().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetAddressTokenModel().UpsertOne(newAddressToken)
			zap.S().Debug(
				"Loader=AddressToken",
				",Address=", newAddressToken.PublicKey,
				",TokenContractAddress=", newAddressToken.TokenContractAddress,
				" - Upserted",
			)
			if err != nil {
				// Postgres error
				zap.S().Info(
					"Loader=AddressToken",
					",Address=", newAddressToken.PublicKey,
					",TokenContractAddress=", newAddressToken.TokenContractAddress,
					" - FATAL",
				)
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
