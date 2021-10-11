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
	Limit      int  `query:"limit"`
	Skip       int  `query:"skip"`
	IsContract bool `query:"is_contract"`
}

func AddressesAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/addresses"

	app.Get(prefix+"/", handlerGetAddresses)
	app.Get(prefix+"/contracts", handlerGetContracts)
	app.Get(prefix+"/address-tokens/:public_key", handlerGetAddressTokens)
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

	// Get Addresses
	addresses, err := crud.GetAddressModel().SelectManyAPI(
		params.Limit,
		params.Skip,
		params.IsContract,
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
	counter, err := crud.GetContractCountModel().SelectLargestCount()
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
// @Param public_key path string true "public_key"
// @Router /api/v1/addresses/address-tokens/{public_key} [get]
// @Success 200 {object} []string
// @Failure 422 {object} map[string]interface{}
func handlerGetAddressTokens(c *fiber.Ctx) error {
	publicKey := c.Params("public_key")

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
