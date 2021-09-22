package transformers

import (
	"encoding/hex"
	"math/big"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/metrics"
	"github.com/geometry-labs/icon-addresses/models"
	"github.com/geometry-labs/icon-addresses/worker/utils"
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
		if fromAddress != nil {
			addressLoaderChan <- fromAddress
		}

		// Loads to: addresses (to address)
		toAddress := transformTransactionRawToAddress(transactionRaw, false)
		if toAddress != nil {
			addressLoaderChan <- toAddress
		}

		// Loads to: addresses_count (from address)
		if fromAddress != nil {
			fromAddressCount := transformAddressToAddressCount(fromAddress)
			addressCountLoaderChan <- fromAddressCount
		}

		// Loads to: addresses_count (to address)
		if toAddress != nil {
			toAddressCount := transformAddressToAddressCount(toAddress)
			addressCountLoaderChan <- toAddressCount
		}

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
func transformTransactionRawToAddress(txRaw *models.TransactionRaw, useFromAddress bool) *models.Address {

	// Public Key
	publicKey := ""

	if useFromAddress == true {
		publicKey = txRaw.FromAddress
	} else {
		publicKey = txRaw.ToAddress
	}
	if publicKey == "None" {
		return nil
	}

	// Current Balance Hex
	currentBalanceHex, err := utils.IconNodeServiceGetBalanceOf(publicKey)
	if err != nil {
		zap.S().Fatal(err.Error())
	}

	// Current Balance Dec
	currentBalanceDec := float64(0)
	if currentBalanceHex != "0x0" {
		currentBalanceDecBigInt, isSuccess := new(big.Int).SetString(currentBalanceHex[2:], 16)
		if isSuccess == false {
			zap.S().Fatal("Cannot parse string to big int, currentBalanceHex=", currentBalanceHex)
		}
		currentBalanceDecBigFloat := new(big.Float).SetInt(currentBalanceDecBigInt)

		// 10^18
		icxBaseBigInt, _ := new(big.Int).SetString("DE0B6B3A7640000", 16)
		icxBaseBigFloat := new(big.Float).SetInt(icxBaseBigInt)

		currentBalanceDecBigFloat = currentBalanceDecBigFloat.Quo(currentBalanceDecBigFloat, icxBaseBigFloat)

		currentBalanceDec, _ = currentBalanceDecBigFloat.Float64()
	} else {
		// current balance is 0
		currentBalanceDec = 0
	}

	return &models.Address{
		PublicKey:         publicKey,
		CurrentBalanceHex: currentBalanceHex,
		CurrentBalanceDec: currentBalanceDec,
	}
}

func transformAddressToAddressCount(address *models.Address) *models.AddressCount {

	return &models.AddressCount{
		PublicKey: address.PublicKey,
	}
}
