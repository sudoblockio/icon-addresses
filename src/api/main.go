package main

import (
	"log"

	"github.com/geometry-labs/icon-addresses/api/healthcheck"
	"github.com/geometry-labs/icon-addresses/api/routes"
	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/global"
	"github.com/geometry-labs/icon-addresses/logging"
	"github.com/geometry-labs/icon-addresses/metrics"
	_ "github.com/geometry-labs/icon-addresses/models" // for swagger docs
	"github.com/geometry-labs/icon-addresses/redis"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	// Go routine starts in function
	metrics.Start()

	// Start Redis Client
	// NOTE: redis is used for websockets
	redis.GetBroadcaster().Start()
	redis.GetRedisClient().StartSubscriber()

	// Start API server
	// Go routine starts in function
	routes.Start()

	// Start Health server
	// Go routine starts in function
	healthcheck.Start()

	global.WaitShutdownSig()
}
