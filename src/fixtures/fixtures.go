package fixtures

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-addresses/models"
)

const (
	logRawFixturesPath = "addresses_raw.json"
)

// Fixtures - slice of Fixture
type Fixtures []Fixture

// Fixture - loaded from fixture file
type Fixture map[string]interface{}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// LoadAddressFixtures - load log fixtures from disk
func LoadAddressFixtures() []*models.Address {
	addresses := make([]*models.Address, 0)

	fixtures, err := loadFixtures(logRawFixturesPath)
	check(err)

	for _, fixture := range fixtures {
		addresses = append(addresses, parseFixtureToAddress(fixture))
	}

	return addresses
}

func loadFixtures(file string) (Fixtures, error) {
	var fs Fixtures

	dat, err := ioutil.ReadFile(getFixtureDir() + file)
	check(err)
	err = json.Unmarshal(dat, &fs)

	return fs, err
}

func getFixtureDir() string {

	callDir, _ := os.Getwd()
	callDirSplit := strings.Split(callDir, "/")

	for i := len(callDirSplit) - 1; i >= 0; i-- {
		if callDirSplit[i] != "src" {
			callDirSplit = callDirSplit[:len(callDirSplit)-1]
		} else {
			break
		}
	}

	callDirSplit = append(callDirSplit, "fixtures")
	fixtureDir := strings.Join(callDirSplit, "/")
	fixtureDir = fixtureDir + "/"
	zap.S().Info(fixtureDir)

	return fixtureDir
}

func parseFixtureToAddress(m map[string]interface{}) *models.Address {

	// TODO
	return &models.Address{}
}
