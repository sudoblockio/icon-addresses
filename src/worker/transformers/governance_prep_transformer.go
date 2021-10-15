package transformers

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/models"
)

// StartGovernancePrepsTransformer - start governancePrep transformer go routine
func StartGovernancePrepsTransformer() {
	go governancePrepsTransformer()
}

func governancePrepsTransformer() {
	consumerTopicNameGovernancePreps := config.Config.ConsumerTopicGovernancePrepsProcessed

	// Input channels
	consumerTopicChanGovernancePreps := kafka.KafkaTopicConsumers[consumerTopicNameGovernancePreps].TopicChannel

	// Output channels
	governancePrepLoaderChan := crud.GetGovernancePrepProcessedModel().LoaderChannel

	zap.S().Debug("GovernancePreps transformer: started working")
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanGovernancePreps
		governancePrepRaw, err := convertToGovernancePrepRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("GovernancePreps Transformer: Processing governancePrep #", governancePrepRaw.Address)
		if err != nil {
			zap.S().Fatal("GovernancePreps transformer: Unable to proceed cannot convert kafka msg value to BlockRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Load to: governancePreps
		governancePrepLoaderChan <- governancePrepRaw
	}
}

func convertToGovernancePrepRawProtoBuf(value []byte) (*models.GovernancePrepProcessed, error) {
	governancePrep := models.GovernancePrepProcessed{}
	err := proto.Unmarshal(value[6:], &governancePrep)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
	}
	return &governancePrep, err
}
