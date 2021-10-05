package crud

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-addresses/models"
)

// BlockModel - type for block table model
type BlockModel struct {
	db            *gorm.DB
	model         *models.Block
	modelORM      *models.BlockORM
	LoaderChannel chan *models.Block
}

var blockModel *BlockModel
var blockModelOnce sync.Once

// GetBlockModel - create and/or return the blocks table model
func GetBlockModel() *BlockModel {
	blockModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		blockModel = &BlockModel{
			db:            dbConn,
			model:         &models.Block{},
			LoaderChannel: make(chan *models.Block, 1),
		}

		err := blockModel.Migrate()
		if err != nil {
			zap.S().Fatal("BlockModel: Unable migrate postgres table: ", err.Error())
		}

		StartBlockLoader()
	})

	return blockModel
}

// Migrate - migrate blocks table
func (m *BlockModel) Migrate() error {
	// Only using BlockRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert block into table
func (m *BlockModel) Insert(block *models.Block) error {

	err := backoff.Retry(func() error {
		query := m.db.Create(block)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// SelectOne - select from blocks table
func (m *BlockModel) SelectOne(
	number uint32,
) (*models.Block, error) {
	db := m.db

	db = db.Where("number = ?", number)

	block := &models.Block{}
	db = db.First(block)

	return block, db.Error
}

// UpdateOne - select from blocks table
func (m *BlockModel) UpdateOne(
	block *models.Block,
) error {
	db := m.db

	db = db.Where("number = ?", block.Number)

	db = db.Save(block)

	return db.Error
}

func (m *BlockModel) UpsertOne(
	block *models.Block,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*block),
		reflect.TypeOf(*block),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "number"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(block)

	return db.Error
}

// StartBlockLoader starts loader
func StartBlockLoader() {
	go func() {

		for {
			// Read block
			newBlock := <-GetBlockModel().LoaderChannel

			/////////////////
			// Enrichments //
			/////////////////
			logCount := uint32(0)

			////////////////////////
			// Log Count By Block //
			////////////////////////
			allLogCountsByBlockNumber, err := GetLogCountByBlockNumberModel().SelectManyByBlockNumber(newBlock.Number)
			if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			// logCount
			seenTransactionHashesInAllLogCountsByBlockNumber := map[string]bool{}
			for _, logCountsByBlockNumber := range allLogCountsByBlockNumber {
				transactionHash := logCountsByBlockNumber.TransactionHash
				_, ok := seenTransactionHashesInAllLogCountsByBlockNumber[transactionHash]
				if ok == false {
					// new transaction hash
					logCount += logCountsByBlockNumber.MaxCountByTransaction
				}

				seenTransactionHashesInAllLogCountsByBlockNumber[transactionHash] = true
			}

			newBlock.LogCount = logCount

			//////////////////////
			// Load to postgres //
			//////////////////////
			err = GetBlockModel().UpsertOne(newBlock)
			zap.S().Debug("Loader=Block, Number=", newBlock.Number, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=Block, Number=", newBlock.Number, " - FATAL")
				zap.S().Fatal(err.Error())
			}
		}
	}()
}

// reloadBlock - Send block back to loader for updates
func reloadBlock(number uint32) error {

	curBlock, err := GetBlockModel().SelectOne(number)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create empty block
		curBlock = &models.Block{}
		curBlock.Number = number
	} else if err != nil {
		// Postgres error
		return err
	}
	GetBlockModel().LoaderChannel <- curBlock

	return nil
}
