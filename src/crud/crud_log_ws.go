package crud

import (
	"encoding/json"
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-logs/models"
	"github.com/geometry-labs/icon-logs/redis"
)

// LogWebsocketIndexModel - type for logWebsocketIndex table model
type LogWebsocketIndexModel struct {
	db            *gorm.DB
	model         *models.LogWebsocketIndex
	modelORM      *models.LogWebsocketIndexORM
	LoaderChannel chan *models.LogWebsocket // Write LogWebsocket to create a LogWebsocketIndex
}

var logWebsocketIndexModel *LogWebsocketIndexModel
var logWebsocketIndexModelOnce sync.Once

// GetLogWebsocketIndexModel - create and/or return the logWebsocketIndexs table model
func GetLogWebsocketIndexModel() *LogWebsocketIndexModel {
	logWebsocketIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		logWebsocketIndexModel = &LogWebsocketIndexModel{
			db:            dbConn,
			model:         &models.LogWebsocketIndex{},
			LoaderChannel: make(chan *models.LogWebsocket, 1),
		}

		err := logWebsocketIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("LogWebsocketIndexModel: Unable migrate postgres table: ", err.Error())
		}

		StartLogWebsocketIndexLoader()
	})

	return logWebsocketIndexModel
}

// Migrate - migrate logWebsocketIndexs table
func (m *LogWebsocketIndexModel) Migrate() error {
	// Only using LogWebsocketIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert logWebsocketIndex into table
func (m *LogWebsocketIndexModel) Insert(logWebsocketIndex *models.LogWebsocketIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.LogWebsocketIndex{})

	db = db.Create(logWebsocketIndex)

	return db.Error
}

func (m *LogWebsocketIndexModel) SelectOne(
	transactionHash string,
	logIndex uint64,
) (*models.LogWebsocketIndex, error) {
	db := m.db

	// Set table
	db = db.Model(&models.LogWebsocketIndex{})

	db = db.Where("transaction_hash = ?", transactionHash)

	db = db.Where("log_index = ?", logIndex)

	logWebsocketIndex := &models.LogWebsocketIndex{}
	db = db.First(logWebsocketIndex)

	return logWebsocketIndex, db.Error
}

// StartLogWebsocketIndexLoader starts loader
func StartLogWebsocketIndexLoader() {
	go func() {

		for {
			// Read transaction
			newLogWebsocket := <-GetLogWebsocketIndexModel().LoaderChannel

			// LogWebsocket -> LogWebsocketIndex
			newLogWebsocketIndex := &models.LogWebsocketIndex{
				TransactionHash: newLogWebsocket.TransactionHash,
				LogIndex:        newLogWebsocket.LogIndex,
			}

			// Update/Insert
			_, err := GetLogWebsocketIndexModel().SelectOne(newLogWebsocketIndex.TransactionHash, newLogWebsocketIndex.LogIndex)
			if errors.Is(err, gorm.ErrRecordNotFound) {

				// Insert
				GetLogWebsocketIndexModel().Insert(newLogWebsocketIndex)

				// Publish to redis
				newLogWebsocketJSON, _ := json.Marshal(newLogWebsocket)
				redis.GetRedisClient().Publish(newLogWebsocketJSON)
			} else if err != nil {
				// Postgres error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
