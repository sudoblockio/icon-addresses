package healthcheck

import (
	"time"
	"net/http"
	"net/url"

	"go.uber.org/zap"
	"github.com/InVisionApp/go-health/v2"
	"github.com/InVisionApp/go-health/v2/checkers"
	"github.com/InVisionApp/go-health/v2/handlers"

	"github.com/geometry-labs/icon-logs/config"
)

func Start() {
	// create a new health instance
	h := health.New()

	// create a couple of checks
	logsCheckerURL, _ := url.Parse("http://localhost:" + config.Config.Port + "/version")
	logsChecker, _ := checkers.NewHTTP(&checkers.HTTPConfig{
		URL: logsCheckerURL,
	})

	// Add the checks to the health instance
	h.AddChecks([]*health.Config{
		{
			Name:     "logs-rest-check",
			Checker:  logsChecker,
			Interval: time.Duration(config.Config.HealthPollingInterval) * time.Second,
			Fatal:    true,
		},
	})

	//  Start the healthcheck process
	if err := h.Start(); err != nil {
		zap.S().Fatalf("Unable to start healthcheck: %v", err)
	}

	// Define a healthcheck endpoint and use the built-in JSON handler
	http.HandleFunc(config.Config.HealthPrefix, handlers.NewJSONHandlerFunc(h, nil))
	go http.ListenAndServe(":"+config.Config.HealthPort, nil)
	zap.S().Info("Started Healthcheck:", config.Config.HealthPort)
}
