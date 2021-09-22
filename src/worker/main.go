package main

import (
	"log"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/global"
	"github.com/geometry-labs/icon-addresses/kafka"
	"github.com/geometry-labs/icon-addresses/logging"
	"github.com/geometry-labs/icon-addresses/metrics"
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
	transformers.StartTransactionsTransformer()

	global.WaitShutdownSig()
}
