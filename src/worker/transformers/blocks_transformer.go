package transformers

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/metrics"
	"github.com/geometry-labs/icon-addresses/models"
)

// StartBlocksTransformer - start block transformer go routine
func StartBlocksTransformer() {
	go blocksTransformer()
}

func blocksTransformer() {
	consumerTopicNameBlocks := config.Config.ConsumerTopicBlocks

	// Input channels
	consumerTopicChanBlocks := kafka.KafkaTopicConsumer.TopicChannels[consumerTopicNameBlocks]

	// Output channels
	blockLoaderChan := crud.GetBlockModel().LoaderChannel

	zap.S().Debug("Blocks transformer: started working")
	for {

		///////////////////
		// Kafka Message //
		///////////////////

		consumerTopicMsg := <-consumerTopicChanBlocks
		blockRaw, err := convertToBlockRawProtoBuf(consumerTopicMsg.Value)
		zap.S().Debug("Blocks Transformer: Processing block #", blockRaw.Number)
		if err != nil {
			zap.S().Fatal("Blocks transformer: Unable to proceed cannot convert kafka msg value to BlockRaw, err: ", err.Error())
		}

		/////////////
		// Loaders //
		/////////////

		// Load to: blocks
		block := transformBlockRawToBlock(blockRaw)
		blockLoaderChan <- block

		/////////////
		// Metrics //
		/////////////

		// max_block_number_blocks_raw
		metrics.MaxBlockNumberBlocksRawGauge.Set(float64(blockRaw.Number))
	}
}

func convertToBlockRawProtoBuf(value []byte) (*models.BlockRaw, error) {
	block := models.BlockRaw{}
	err := proto.Unmarshal(value[6:], &block)
	if err != nil {
		zap.S().Error("Error: ", err.Error())
	}
	return &block, err
}

func transformBlockRawToBlock(blockRaw *models.BlockRaw) *models.Block {

	return &models.Block{
		Number:           blockRaw.Number,
		TransactionCount: blockRaw.TransactionCount,
		LogCount:         0, // Adds in loader
	}
}
