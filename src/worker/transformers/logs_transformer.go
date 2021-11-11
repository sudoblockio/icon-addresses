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
	consumerTopicChanLogs := kafka.KafkaTopicConsumers.TopicChannels[consumerTopicNameLogs]

	// Output channels
	addressLoaderChan := crud.GetAddressModel().LoaderChannel
	addressTokenLoaderChan := crud.GetAddressTokenModel().LoaderChannel
	transactionLoaderChan := crud.GetTransactionModel().LoaderChannel
	logCountByPublicKeyLoaderChan := crud.GetLogCountByPublicKeyModel().LoaderChannel
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

		// Loads to address_tokens (from address)
		fromAddressToken := transformLogRawToAddressToken(logRaw, true)
		if fromAddressToken != nil {
			addressTokenLoaderChan <- fromAddressToken
		}

		// Loads to address_tokens (to address)
		toAddressToken := transformLogRawToAddressToken(logRaw, false)
		if toAddressToken != nil {
			addressTokenLoaderChan <- toAddressToken
		}

		// Loads to transactions
		transaction := transformLogRawToTransaction(logRaw)
		if transaction != nil {
			transactionLoaderChan <- transaction
		}

		// Loads to log_count_by_addresses
		logCountByPublicKeyFromAddress := transformLogRawToLogCountByPublicKey(logRaw)
		logCountByPublicKeyLoaderChan <- logCountByPublicKeyFromAddress

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
		Balance:          0, // Enriched in loader
	}
}

func transformLogRawToAddressToken(logRaw *models.LogRaw, useFromAddress bool) *models.AddressToken {

	var indexed []string
	err := json.Unmarshal([]byte(logRaw.Indexed), &indexed)
	if err != nil {
		zap.S().Fatal("Unable to parse indexed field in log; indexed=", logRaw.Indexed, " error: ", err.Error())
	}

	if indexed[0] != "Transfer(Address,Address,int,bytes)" || len(indexed) != 4 {
		// Not token transfer
		return nil
	}

	// Public Key
	publicKey := ""
	if useFromAddress == true {
		publicKey = indexed[1]
	} else {
		publicKey = indexed[2]
	}

	return &models.AddressToken{
		PublicKey:            publicKey,
		TokenContractAddress: logRaw.Address,
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
		FromAddress:      indexed[1],
		ToAddress:        indexed[2],
		Value:            indexed[3],
		Hash:             logRaw.TransactionHash,
		BlockNumber:      logRaw.BlockNumber,
		TransactionIndex: logRaw.TransactionIndex,
		BlockTimestamp:   logRaw.BlockTimestamp,
		TransactionFee:   "0x0", // No fees for internal transactions
		LogIndex:         int32(logRaw.LogIndex),
	}
}

func transformLogRawToLogCountByPublicKey(logRaw *models.LogRaw) *models.LogCountByPublicKey {

	return &models.LogCountByPublicKey{
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
		BlockNumber:           logRaw.BlockNumber,
		Count:                 0, // Adds in loader
		MaxCountByTransaction: uint32(logRaw.MaxLogIndex),
	}
}
