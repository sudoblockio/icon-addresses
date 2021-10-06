package transformers

import (
	"encoding/hex"
	"encoding/json"
	"strings"

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
	addressLoaderChan := crud.GetAddressModel().LoaderChannel
	transactionLoaderChan := crud.GetTransactionModel().LoaderChannel
	logCountByAddressLoaderChan := crud.GetLogCountByAddressModel().LoaderChannel
	logCountByBlockNumberLoaderChan := crud.GetLogCountByBlockNumberModel().LoaderChannel

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

		// Loads to addresses (from address)
		fromAddress := transformLogRawToAddress(logRaw, true)
		if fromAddress != nil {
			addressLoaderChan <- fromAddress
		}

		// Loads to addresses (to address)
		toAddress := transformLogRawToAddress(logRaw, false)
		if toAddress != nil {
			addressLoaderChan <- toAddress
		}

		transaction := transformLogRawToTransaction(logRaw)
		if transaction != nil {
			transactionLoaderChan <- transaction
		}

		// Loads to log_count_by_addresses
		logCountByAddressFromAddress := transformLogRawToLogCountByAddress(logRaw)
		logCountByAddressLoaderChan <- logCountByAddressFromAddress

		// Loads to log_count_by_block_number
		logCountByBlockNumber := transformLogRawToLogCountByBlockNumber(logRaw)
		logCountByBlockNumberLoaderChan <- logCountByBlockNumber

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

func transformLogRawToAddress(logRaw *models.LogRaw, useFromAddress bool) *models.Address {

	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}

	method := strings.Split(indexed[0], "(")[0]

	if method != "ICXTransfer" {
		// Not internal transaction
		return nil
	}

	// Public Key
	publicKey := ""

	if useFromAddress == true {
		publicKey = indexed[1]
	} else {
		publicKey = indexed[2]
	}

	// Is Contract
	isContract := false
	if publicKey[:2] == "cx" {
		isContract = true
	}

	return &models.Address{
		PublicKey:        publicKey,
		IsContract:       isContract,
		TransactionCount: 0, // Enriched in loader
		LogCount:         0, // Enriched in loader
	}
}

func transformLogRawToTransaction(logRaw *models.LogRaw) *models.Transaction {

	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}

	method := strings.Split(indexed[0], "(")[0]

	if method != "ICXTransfer" {
		// Not internal transaction
		return nil
	}

	// indexed[3] = value
	if indexed[3] == "0x0" {
		// No value transaction
		return nil
	}

	return &models.Transaction{
		FromAddress: indexed[1],
		ToAddress:   indexed[2],
		Value:       indexed[3],
		Hash:        logRaw.TransactionHash,
		BlockNumber: logRaw.BlockNumber,
		LogIndex:    int32(logRaw.LogIndex),
	}
}

func transformLogRawToLogCountByAddress(logRaw *models.LogRaw) *models.LogCountByAddress {

	return &models.LogCountByAddress{
		TransactionHash: logRaw.TransactionHash,
		LogIndex:        logRaw.LogIndex,
		PublicKey:       logRaw.Address,
		Count:           0, // Adds in loader
	}
}

func transformLogRawToLogCountByBlockNumber(logRaw *models.LogRaw) *models.LogCountByBlockNumber {

	return &models.LogCountByBlockNumber{
		TransactionHash:       logRaw.TransactionHash,
		LogIndex:              logRaw.LogIndex,
		BlockNumber:           uint32(logRaw.BlockNumber),
		Count:                 0, // Adds in loader
		MaxCountByTransaction: uint32(logRaw.MaxLogIndex),
	}
}
