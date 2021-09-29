package transformers

import (
	"encoding/hex"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/metrics"
	"github.com/geometry-labs/icon-addresses/models"
)

func StartLogsTransformer() {
	go logsTransformer()
}

func logsTransformer() {
	consumerTopicNameLogs := config.Config.ConsumerTopicLogs

	// Input Channels
	consumerTopicChanLogs := kafka.KafkaTopicConsumers[consumerTopicNameLogs].TopicChannel

	// Output channels
	logCountByAddressLoaderChan := crud.GetLogCountByAddressModel().LoaderChannel

	zap.S().Debug("Logs Worker: started working")
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanLogs
		logRaw, err := convertBytesToLogRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Logs Transformer: Processing log in tx hash=", logRaw.TransactionHash)
		if err != nil {
			zap.S().Fatal("Logs Worker: Unable to proceed cannot convert kafka msg value to LogRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Loads to log_count_by_addresses
		logCountByAddressFromAddress := transformLogRawToLogCountByAddress(logRaw)
		logCountByAddressLoaderChan <- logCountByAddressFromAddress

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

func transformLogRawToLogCountByAddress(logRaw *models.LogRaw) *models.LogCountByAddress {

	return &models.LogCountByAddress{
		TransactionHash: logRaw.TransactionHash,
		LogIndex:        logRaw.LogIndex,
		PublicKey:       logRaw.Address,
		Count:           0, // Adds in loader
	}
}
