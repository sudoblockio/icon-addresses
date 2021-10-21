package main

import (
	"log"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/global"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/logging"
	"github.com/geometry-labs/icon-addresses/metrics"
	"github.com/geometry-labs/icon-addresses/worker/builders"
	"github.com/geometry-labs/icon-addresses/worker/routines"
	"github.com/geometry-labs/icon-addresses/worker/transformers"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	metrics.Start()

	// Start kafka consumer
	kafka.StartWorkerConsumers()

	// Start transformers
	transformers.StartBlocksTransformer()
	transformers.StartTransactionsTransformer()
	transformers.StartLogsTransformer()
	transformers.StartContractsTransformer()
	transformers.StartGovernancePrepsTransformer()

	// Start builders
	builders.StartBalanceBuilder()

	// Start Routines
	routines.StartBalanceRoutine()

	global.WaitShutdownSig()
}
