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
	Limit     int    `query:"limit"`
	Skip      int    `query:"skip"`
	PublicKey string `query:"address"`
}

func AddressesAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/addresses"

	app.Get(prefix+"/", handlerGetAddresses)
	app.Get(prefix+"/details/:address", handlerGetAddressDetails)
	app.Get(prefix+"/contracts", handlerGetContracts)
	app.Get(prefix+"/address-tokens/:address", handlerGetAddressTokens)
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
// @Param is_contract query bool false "contract addresses only"
// @Param address query string false "find by address"
// @Router /api/v1/addresses [get]
// @Success 200 {object} []models.AddressAPIList
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
	if params.Skip < 0 || params.Skip > config.Config.MaxPageSkip {
		c.Status(422)
		return c.SendString(`{"error": "invalid skip"}`)
	}

	// Get Addresses
	addresses, err := crud.GetAddressModel().SelectManyAPI(
		params.Limit,
		params.Skip,
		params.PublicKey,
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
	counter, err := crud.GetAddressCountModel().SelectCount("all")
	if err != nil {
		counter = 0
		zap.S().Warn("Could not retrieve address count: ", err.Error())
	}
	c.Append("X-TOTAL-COUNT", strconv.FormatUint(counter, 10))

	body, _ := json.Marshal(addresses)
	return c.SendString(string(body))
}

// Address Details
// @Summary Get Address Details
// @Description get details of an address
// @Tags Addresses
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param address path string true "find by address"
// @Router /api/v1/addresses/details/{address} [get]
// @Success 200 {object} models.Address
// @Failure 422 {object} map[string]interface{}
func handlerGetAddressDetails(c *fiber.Ctx) error {
	publicKey := c.Params("address")
	if publicKey == "" {
		c.Status(422)
		return c.SendString(`{"error": "public required"}`)
	}

	params := new(AddressesQuery)
	if err := c.QueryParser(params); err != nil {
		zap.S().Warnf("Addresses Get Handler ERROR: %s", err.Error())

		c.Status(422)
		return c.SendString(`{"error": "could not parse query parameters"}`)
	}

	// Get Addresses
	address, err := crud.GetAddressModel().SelectOne(
		publicKey,
	)
	if err != nil {
		c.Status(500)

		zap.S().Warnf("Addresses CRUD ERROR: %s", err.Error())
		return c.SendString(`{"error": "could not retrieve addresses"}`)
	}

	body, _ := json.Marshal(address)
	return c.SendString(string(body))
}

// Contract
// @Summary Get contracts
// @Description get list of contracts
// @Tags Addresses
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param limit query int false "amount of records"
// @Param skip query int false "skip to a record"
// @Router /api/v1/addresses/contracts [get]
// @Success 200 {object} []models.ContractAPIList
// @Failure 422 {object} map[string]interface{}
func handlerGetContracts(c *fiber.Ctx) error {
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
	if params.Skip < 0 || params.Skip > config.Config.MaxPageSkip {
		c.Status(422)
		return c.SendString(`{"error": "invalid skip"}`)
	}

	// Get contracts
	contracts, err := crud.GetAddressModel().SelectManyContractsAPI(
		params.Limit,
		params.Skip,
	)
	if err != nil {
		zap.S().Warnf("Addresses CRUD ERROR: %s", err.Error())
		c.Status(500)
		return c.SendString(`{"error": "could not retrieve addresses"}`)
	}

	if len(*contracts) == 0 {
		// No Content
		c.Status(204)
	}

	// TODO
	// Set X-TOTAL-COUNT
	// Total count in the address_counts table
	counter, err := crud.GetAddressCountModel().SelectCount("Contract")
	if err != nil {
		counter = 0
		zap.S().Warn("Could not retrieve address count: ", err.Error())
	}
	c.Append("X-TOTAL-COUNT", strconv.FormatUint(counter, 10))

	body, _ := json.Marshal(contracts)
	return c.SendString(string(body))
}

// Address Tokens
// @Summary Get Address Tokens
// @Description get list of token contracts by address
// @Tags Addresses
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param address path string true "address"
// @Router /api/v1/addresses/address-tokens/{address} [get]
// @Success 200 {object} []string
// @Failure 422 {object} map[string]interface{}
func handlerGetAddressTokens(c *fiber.Ctx) error {
	publicKey := c.Params("address")

	// Get AddressTokens
	addressTokens, err := crud.GetAddressTokenModel().SelectManyByPublicKey(publicKey)
	if err != nil {
		zap.S().Warnf("AddressTokens CRUD ERROR: %s", err.Error())
		c.Status(500)
		return c.SendString(`{"error": "could not retrieve addresses"}`)
	}

	if len(*addressTokens) == 0 {
		// No Content
		c.Status(204)
	}

	// Set X-TOTAL-COUNT
	c.Append("X-TOTAL-COUNT", strconv.FormatUint(uint64(len(*addressTokens)), 10))

	// []models.AddressToken -> []string
	var tokenContractAddresses []string
	for _, a := range *addressTokens {
		tokenContractAddresses = append(tokenContractAddresses, a.TokenContractAddress)
	}

	body, _ := json.Marshal(&tokenContractAddresses)
	return c.SendString(string(body))
}
