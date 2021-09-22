package transformers

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-logs/config"
	"github.com/geometry-labs/icon-logs/crud"
	"github.com/geometry-labs/icon-logs/kafka"
	"github.com/geometry-labs/icon-logs/metrics"
	"github.com/geometry-labs/icon-logs/models"
)

func StartLogsTransformer() {
	go logsTransformer()
}

func logsTransformer() {
	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input Channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumers[consumerTopicNameLogs].TopicChannel

	// Output channels
	logLoaderChan := crud.GetLogModel().LoaderChannel
	logWebsocketLoaderChan := crud.GetLogWebsocketIndexModel().LoaderChannel
	logCountLoaderChan := crud.GetLogCountModel().LoaderChannel
	logCountByAddressLoaderChan := crud.GetLogCountByAddressModel().LoaderChannel

	zap.S().Debug("Logs Worker: started working")
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanLogs
		logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
		if err != nil {
			zap.S().Fatal("Logs Worker: Unable to proceed cannot convert kafka msg value to LogRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Loads to: logs
		log := transformLogRawToLog(logRaw)
		logLoaderChan <- log

		// Loads to: log_websocket_indices
		logWebsocket := transformLogToLogWS(log)
		logWebsocketLoaderChan <- logWebsocket

		// Loads to: log_counts
		logCount := transformLogToLogCount(log)
		logCountLoaderChan <- logCount

		// Loads to: log_count_by_addresses
		logCountByAddress := transformLogToLogCountByAddress(log)
		logCountByAddressLoaderChan <- logCountByAddress

		/////////////
		// Metrics //
		/////////////
		metrics.MaxBlockNumberLogsRawGauge.Set(float64(logRaw.BlockNumber))

	}
}

func convertBytesToLogRawProtoBuf(value []byte) (*models.LogRaw, error) {
	log := models.LogRaw{}
	err := proto.Unmarshal(value[6:], &log)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &log, err
}

// Business logic goes here
func transformLogRawToLog(logRaw *models.LogRaw) *models.Log {

	////////////
	// Method //
	////////////
	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}
	method := strings.Split(indexed[0], "(")[0]

	return &models.Log{
		Type:             logRaw.Type,
		LogIndex:         logRaw.LogIndex,
		MaxLogIndex:      logRaw.MaxLogIndex,
		TransactionHash:  logRaw.TransactionHash,
		TransactionIndex: logRaw.TransactionIndex,
		Address:          logRaw.Address,
		Data:             logRaw.Data,
		Indexed:          logRaw.Indexed,
		BlockNumber:      logRaw.BlockNumber,
		BlockTimestamp:   logRaw.BlockTimestamp,
		BlockHash:        logRaw.BlockHash,
		ItemId:           logRaw.ItemId,
		ItemTimestamp:    logRaw.ItemTimestamp,
		Method:           method,
	}
}

// Business logic goes here
func transformLogToLogWS(log *models.Log) *models.LogWebsocket {
	return &models.LogWebsocket{
		Type:             log.Type,
		LogIndex:         log.LogIndex,
		TransactionHash:  log.TransactionHash,
		TransactionIndex: log.TransactionIndex,
		Address:          log.Address,
		Data:             log.Data,
		Indexed:          log.Indexed,
		BlockNumber:      log.BlockNumber,
		BlockTimestamp:   log.BlockTimestamp,
		BlockHash:        log.BlockHash,
		Method:           log.Method,
	}
}

func transformLogToLogCount(log *models.Log) *models.LogCount {
	return &models.LogCount{
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}
}

func transformLogToLogCountByAddress(log *models.Log) *models.LogCountByAddress {
	return &models.LogCountByAddress{
		LogIndex:        log.LogIndex,
		TransactionHash: log.TransactionHash,
		Address:         log.Address,
	}
}
