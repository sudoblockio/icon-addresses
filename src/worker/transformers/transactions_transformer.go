package transformers

import (
	"encoding/hex"
	"fmt"
	"math/big"

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
	transactionLoaderChan := crud.GetTransactionModel().LoaderChannel
	transactionCountByPublicKeyLoaderChan := crud.GetTransactionCountByPublicKeyModel().LoaderChannel
	transactionCountByBlockNumberLoaderChan := crud.GetTransactionCountByBlockNumberModel().LoaderChannel

	zap.S().Debug("Transactions Transformer: started working")

	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanTransactions
		transactionRaw, err := convertBytesToTransactionRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Transactions Transformer: Processing transaction hash=", transactionRaw.Hash)
		if err != nil {
			zap.S().Fatal("Transactions Transformer: Unable to proceed cannot convert kafka msg value to TransactionRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Loads to addresses (from address)
		fromAddress := transformTransactionRawToAddress(transactionRaw, true)
		if fromAddress != nil {
			addressLoaderChan <- fromAddress
		}

		// Loads to addresses (to address)
		toAddress := transformTransactionRawToAddress(transactionRaw, false)
		if toAddress != nil {
			addressLoaderChan <- toAddress
		}

		// Loads to addresses_count (from address) (all)
		if fromAddress != nil {
			fromAddressCount := transformAddressToAddressCountAll(fromAddress)
			addressCountLoaderChan <- fromAddressCount
		}

		// Loads to addresses_count (to address) (all)
		if toAddress != nil {
			toAddressCount := transformAddressToAddressCountAll(toAddress)
			addressCountLoaderChan <- toAddressCount
		}

		// Loads to addresses_count (from address) (contract)
		if fromAddress != nil {
			fromAddressCountContract := transformAddressToAddressCountContract(fromAddress)
			if fromAddressCountContract != nil {
				addressCountLoaderChan <- fromAddressCountContract
			}
		}

		// Loads to addresses_count (to address) (contract)
		if toAddress != nil {
			toAddressCountContract := transformAddressToAddressCountContract(toAddress)
			if toAddressCountContract != nil {
				addressCountLoaderChan <- toAddressCountContract
			}
		}

		// Loads to transactions
		transaction := transformTransactionRawToTransaction(transactionRaw)
		if transaction != nil {
			transactionLoaderChan <- transaction
		}

		// Loads to transaction_count_by_public_key (from address)
		transactionCountByPublicKeyFromAddress := transformTransactionRawToTransactionCountByPublicKey(transactionRaw, true)
		if transactionCountByPublicKeyFromAddress != nil {
			transactionCountByPublicKeyLoaderChan <- transactionCountByPublicKeyFromAddress
		}

		// Loads to transaction_count_by_public_key (to address)
		transactionCountByPublicKeyToAddress := transformTransactionRawToTransactionCountByPublicKey(transactionRaw, false)
		if transactionCountByPublicKeyToAddress != nil {
			transactionCountByPublicKeyLoaderChan <- transactionCountByPublicKeyToAddress
		}

		// Loads to transaction_count_by_block_number
		transactionCountByBlockNumber := transformTransactionRawTransactionCountByBlockNumber(transactionRaw)
		transactionCountByBlockNumberLoaderChan <- transactionCountByBlockNumber

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

func transformAddressToAddressCountAll(address *models.Address) *models.AddressCount {

	return &models.AddressCount{
		Type:      "all",
		Count:     0, // Adds in loader
		PublicKey: address.PublicKey,
	}
}

func transformAddressToAddressCountContract(address *models.Address) *models.AddressCount {

	if address.IsContract == false {
		return nil
	}

	return &models.AddressCount{
		Type:      "contract",
		Count:     0, // Adds in loader
		PublicKey: address.PublicKey,
	}
}

func transformTransactionRawToTransaction(txRaw *models.TransactionRaw) *models.Transaction {

	if txRaw.Value == "0x0" {
		// No value transaction
		return nil
	}

	// Transaction fee calculation
	// Use big int
	// NOTE: transaction fees, once calculated (price*used) may be too large for postgres
	receiptStepPriceBig := big.NewInt(int64(txRaw.ReceiptStepPrice))
	receiptStepUsedBig := big.NewInt(int64(txRaw.ReceiptStepUsed))
	transactionFeesBig := receiptStepUsedBig.Mul(receiptStepUsedBig, receiptStepPriceBig)

	// to hex
	transactionFee := fmt.Sprintf("0x%x", transactionFeesBig)

	return &models.Transaction{
		FromAddress:      txRaw.FromAddress,
		ToAddress:        txRaw.ToAddress,
		Value:            txRaw.Value,
		Hash:             txRaw.Hash,
		BlockNumber:      txRaw.BlockNumber,
		TransactionIndex: txRaw.TransactionIndex,
		BlockTimestamp:   txRaw.BlockTimestamp,
		TransactionFee:   transactionFee,
		LogIndex:         -1,
	}
}

func transformTransactionRawToTransactionCountByPublicKey(txRaw *models.TransactionRaw, isFromAddress bool) *models.TransactionCountByPublicKey {

	// Public Key
	publicKey := ""

	if isFromAddress == true {
		publicKey = txRaw.FromAddress
	} else {
		publicKey = txRaw.ToAddress
	}
	if publicKey == "None" {
		return nil
	}

	return &models.TransactionCountByPublicKey{
		TransactionHash: txRaw.Hash,
		PublicKey:       publicKey,
		Count:           0, // Adds in loader
	}
}

func transformTransactionRawTransactionCountByBlockNumber(txRaw *models.TransactionRaw) *models.TransactionCountByBlockNumber {

	return &models.TransactionCountByBlockNumber{
		BlockNumber:     txRaw.BlockNumber,
		TransactionHash: txRaw.Hash,
		Count:           0, // Adds in loader
	}
}
