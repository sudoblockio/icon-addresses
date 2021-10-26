package transformers

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/models"
)

// StartContractsTransformer - start contract transformer go routine
func StartContractsTransformer() {
	go contractsTransformer()
}

func contractsTransformer() {
	consumerTopicNameContracts := config.Config.ConsumerTopicContractsProcessed

	// Input channels
	consumerTopicChanContracts := kafka.KafkaTopicConsumers[consumerTopicNameContracts].TopicChannel

	// Output channels
	contractLoaderChan := crud.GetContractModel().LoaderChannel
	addressCountLoaderChan := crud.GetAddressCountModel().LoaderChannel

	zap.S().Debug("Contracts transformer: started working")
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanContracts
		contractRaw, err := convertToContractRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Contracts Transformer: Processing contract #", contractRaw.Address)
		if err != nil {
			zap.S().Fatal("Contracts transformer: Unable to proceed cannot convert kafka msg value to BlockRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Load to: contracts
		contractLoaderChan <- contractRaw

		addressCountToken := transformContractToAddressCountToken(contractRaw)
		if addressCountToken != nil {
			addressCountLoaderChan <- addressCountToken
		}
	}
}

func convertToContractRawProtoBuf(value []byte) (*models.ContractProcessed, error) {
	contract := models.ContractProcessed{}
	err := proto.Unmarshal(value[6:], &contract)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
	}
	return &contract, err
}

func transformContractToAddressCountToken(contract *models.ContractProcessed) *models.AddressCount {

	if contract.IsToken == false {
		return nil
	}

	return &models.AddressCount{
		Type:      "token",
		Count:     0, // Adds in loader
		PublicKey: contract.Address,
	}
}
