package main

import (
	"log"

	"github.com/geometry-labs/icon-logs/api/healthcheck"
	"github.com/geometry-labs/icon-logs/api/routes"
	"github.com/geometry-labs/icon-logs/config"
	"github.com/geometry-labs/icon-logs/global"
	"github.com/geometry-labs/icon-logs/logging"
	"github.com/geometry-labs/icon-logs/metrics"
	_ "github.com/geometry-labs/icon-logs/models" // for swagger docs
	"github.com/geometry-labs/icon-logs/redis"
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
