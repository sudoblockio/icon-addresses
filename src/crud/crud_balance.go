package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
)

// BalanceModel - type for balance table model
type BalanceModel struct {
	db            *gorm.DB
	model         *models.Balance
	modelORM      *models.BalanceORM
	LoaderChannel chan *models.Balance
}

var balanceModel *BalanceModel
var balanceModelOnce sync.Once

// GetBalanceModel - create and/or return the balances table model
func GetBalanceModel() *BalanceModel {
	balanceModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		balanceModel = &BalanceModel{
			db:            dbConn,
			model:         &models.Balance{},
			LoaderChannel: make(chan *models.Balance, 1),
		}

		err := balanceModel.Migrate()
		if err != nil {
			zap.S().Fatal("BalanceModel: Unable migrate postgres table: ", err.Error())
		}

		StartBalanceLoader()
	})

	return balanceModel
}

// Migrate - migrate balances table
func (m *BalanceModel) Migrate() error {
	// Only using BalanceRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *BalanceModel) SelectLatest(
	publicKey string,
) (*models.Balance, error) {
	db := m.db

	// Order by block number
	db = db.Order("block_number DESC")

	// publicKey
	db = db.Where("public_key = ?", publicKey)

	balance := &models.Balance{}
	db = db.First(balance)

	return balance, db.Error
}

func (m *BalanceModel) SelectOneByBlockNumber(
	publicKey string,
	blockNumber uint64,
) (*models.Balance, error) {
	db := m.db

	// Order by block number
	db = db.Order("block_number DESC")

	// publicKey
	db = db.Where("public_key = ?", publicKey)

	// Block number
	db = db.Where("block_number <= ?", blockNumber)

	balance := &models.Balance{}
	db = db.First(balance)

	return balance, db.Error
}

func (m *BalanceModel) UpsertOne(
	balance *models.Balance,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*balance),
		reflect.TypeOf(*balance),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "block_number"},
			{Name: "transaction_index"},
			{Name: "log_index"},
			{Name: "public_key"},
		}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(balance)

	return db.Error
}

// StartBalanceLoader starts loader
func StartBalanceLoader() {
	go func() {
		postgresLoaderChan := GetBalanceModel().LoaderChannel

		for {
			// Read balance
			newBalance := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetBalanceModel().UpsertOne(newBalance)
			zap.S().Debug(
				"Loader=Balance,",
				"BlockNumber=", newBalance.BlockNumber,
				"TransactionIndex=", newBalance.TransactionIndex,
				"LogIndex=", newBalance.LogIndex,
				"PublicKey=", newBalance.PublicKey,
				" - Upserted",
			)
			if err != nil {
				// Postgres error
				zap.S().Info(
					"Loader=Balance,",
					"BlockNumber=", newBalance.BlockNumber,
					"TransactionIndex=", newBalance.TransactionIndex,
					"LogIndex=", newBalance.LogIndex,
					"PublicKey=", newBalance.PublicKey,
					" - FATAL",
				)
				zap.S().Fatal(err.Error())
			}

			// Force addresses enrichment
			err = reloadAddress(newBalance.PublicKey)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
