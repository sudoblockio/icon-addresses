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

func StartTransactionsTransformer() {
	go transactionsTransformer()
}

func transactionsTransformer() {
	consumerTopicNameTransactions := config.Config.ConsumerTopicTransactions

	// Input channels
	consumerTopicChanTransactions := kafka.KafkaTopicConsumers[consumerTopicNameTransactions].TopicChannel

	// Output channels
	addressLoaderChan := crud.GetAddressModel().LoaderChannel
	addressCountLoaderChan := crud.GetAddressCountModel().LoaderChannel

	zap.S().Debug("Transactions Transformer: started working")

	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanTransactions
		transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Info("Transactions Transformer: Processing transaction hash=", transactionRaw.Hash)
		if err != nil {
			zap.S().Fatal("Transactions Transformer: Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Loads to: addresses (from address)
		fromAddress := transformTransactionRawToAddress(transactionRaw, true)
		addressLoaderChan <- fromAddress

		// Loads to: addresses (to address)
		toAddress := transformTransactionRawToAddress(transactionRaw, false)
		addressLoaderChan <- toAddress

		// Loads to: addresses_count (from address)
		fromAddressCount := transformTransactionToAddressCount(transaction, true)
		addressCountLoaderChan <- fromAddressCount

		// Loads to: addresses_count (to address)
		toAddressCount := transformTransactionToAddressCount(transaction, false)
		addressCountLoaderChan <- toAddressCount

		/////////////
		// Metrics //
		/////////////

		metrics.MaxBlockNumberTransactionsRawGauge.Set(float64(transactionRaw.BlockNumber))
	}
}

func convertBytesToTransactionRawProtoBuf(value []byte) (*models.TransactionRaw, error) {
	tx := models.TransactionRaw{}
	err := proto.Unmarshal(value[6:], &tx)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
		zap.S().Error("Value=", hex.Dump(value[6:]))
	}
	return &tx, err
}

// Business logic goes here
func transformTransactionRawToAddressToAddress(txRaw *models.TransactionRaw, useFromAddress bool) *models.Address {

	address := ""

	if useFromAddress == true {
		address = txRaw.FromAddress
	} else {
		address = txRaw.ToAddress
	}

	return &models.Address{
		Address:        address,
		CurrentBalance: "0x0",
	}
}

func transformTransactionToAddressCount(tx *models.Transaction, useFromAddress bool) *models.AddressCount {

	address := ""

	if useFromAddress == true {
		address = txRaw.FromAddress
	} else {
		address = txRaw.ToAddress
	}

	return &models.AddressCount{
		Address: address,
	}
}
