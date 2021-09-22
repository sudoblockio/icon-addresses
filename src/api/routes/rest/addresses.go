package rest

import (
	"encoding/json"
	"strconv"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-addresses/config"
	"github.com/geometry-labs/icon-addresses/crud"
)

type AddressesQuery struct {
	Limit int `query:"limit"`
	Skip  int `query:"skip"`
}

func AddressesAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/addresses"

	app.Get(prefix+"/", handlerGetAddresses)
}

// Addresses
// @Summary Get Addresses
// @Description get list of addresses
// @Tags Addresses
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param limit query int false "amount of records"
// @Param skip query int false "skip to a record"
// @Router /api/v1/addresses [get]
// @Success 200 {object} []models.Address
// @Failure 422 {object} map[string]interface{}
func handlerGetAddresses(c *fiber.Ctx) error {
	params := new(AddressesQuery)
	if err := c.QueryParser(params); err != nil {
		zap.S().Warnf("Addresses Get Handler ERROR: %s", err.Error())

		c.Status(422)
		return c.SendString(`{"error": "could not parse query parameters"}`)
	}

	// Default Params
	if params.Limit <= 0 {
		params.Limit = 25
	}

	// Check Params
	if params.Limit < 1 || params.Limit > config.Config.MaxPageSize {
		c.Status(422)
		return c.SendString(`{"error": "limit must be greater than 0 and less than 101"}`)
	}

	// Get Addresses
	addresses, count, err := crud.GetAddressModel().SelectMany(
		params.Limit,
		params.Skip,
	)
	if err != nil {
		zap.S().Warnf("Addresses CRUD ERROR: %s", err.Error())
		c.Status(500)
		return c.SendString(`{"error": "could not retrieve addresses"}`)
	}

	if len(*addresses) == 0 {
		// No Content
		c.Status(204)
	}

	// Set X-TOTAL-COUNT
	// Total count in the address_counts table
	counter, err := crud.GetAddressCountModel().SelectLargestCount()
	if err != nil {
		counter = 0
		zap.S().Warn("Could not retrieve address count: ", err.Error())
	}
	c.Append("X-TOTAL-COUNT", strconv.FormatUint(counter, 10))

	body, _ := json.Marshal(addresses)
	return c.SendString(string(body))
}
