package main

import(
	"fmt"
	"os"
	"log"
	"strconv"
	"time"
	"strings"
	"encoding/json"
	"net/http"
	"gopkg.in/gin-gonic/gin.v1"
	"github.com/go-redis/redis"
	"github.com/influxdata/influxdb/client/v2"
)


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Redis
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Open a connection to Redis
var cli = redis.NewClient(&redis.Options{
	Addr: fmt.Sprintf(
		"%s:%s",
		os.Getenv("REDIS_HOST"),
		os.Getenv("REDIS_PORT"),
	),
})

var dbProtocol = GetEnvOrDefault("DB_PROTOCOL", "http")

var dbHost = GetEnvOrDefault("DB_HOST", "localhost")
var dbPort = GetEnvOrDefault("DB_PORT", "8086")
var dbUser = GetEnvOrDefault("DB_USER", "commute")
var dbPassword = GetEnvOrDefault("DB_PASSWORD","commute")
var dbDatabase = GetEnvOrDefault("DB_DATABASE", "commute")

// Create a new HTTPClient
var influx, influxCliErr = client.NewHTTPClient(client.HTTPConfig{
	Addr: fmt.Sprintf(
		"%s://%s:%s",
		dbProtocol,
		dbHost,
		dbPort,
	),
	Username: dbUser,
	Password: dbPassword,
})


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Structs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Image struct {
	Uid int `json:"uid"`
	Width int `json:"width"`
	Quality int `json:"quality"`
}

type Position struct {
	Latitude float64 `json:"lat"`
	Longitude float64 `json:"lng"`
}

type Station struct {
	Number int `json:"number"`
	Name string `json:"name"`
	Address string `json:"address"`
	Position Position `json:"position"`
	Banking bool `json:"banking"`
	Bonus bool `json:"bonus"`
	Status string `json:"status"`
	ContractName string `json:"contract_name"`
	BikeStands int64 `json:"bike_stands"`
	AvailableBikeStands int64 `json:"available_bike_stands"`
	AvailableBikes int64 `json:"available_bikes"`
	LastUpdate string `json:"last_update"`
	Images []Image `json:"images"`
}

type StationBikeState struct {
	Time string `json:"time"`
	AvailableBikeStands int64 `json:"available_bike_stands"`
	AvailableBikes int64 `json:"available_bikes"`
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func main() {
	defer cli.Close()

	if influxCliErr != nil {
		log.Fatal(influxCliErr)
	}

	defer influx.Close()

	r := gin.Default()

	// Global middleware
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.GET("/stations", stationsHandler)
	r.GET("/stations/:contractName/:stationNumber/:date/availability-infos", stationsAvailabilityInfosHandler)

	r.Run() // listen and serve on 0.0.0.0:8080
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func stationsHandler(c *gin.Context) {
	contractName := c.DefaultQuery("contract-name", "Paris")
	numbers := FilterString(strings.Split(c.DefaultQuery("numbers", ""), ","), func(number string) bool {
		return number != ""
	})
	lat, _ := strconv.ParseFloat(c.Query("lat"), 64)
	lng, _ := strconv.ParseFloat(c.Query("lng"), 64)
	distance, _ := strconv.ParseFloat(c.DefaultQuery("distance", "5000"), 64)


	log.Println(fmt.Printf("contractName: %v\n", contractName))
	log.Println(fmt.Printf("numbers: %v\n", numbers))
	log.Println(fmt.Printf("len(numbers): %v\n", len(numbers)))
	log.Println(fmt.Printf("lat: %v\n", lat))
	log.Println(fmt.Printf("lng: %v\n", lng))
	log.Println(fmt.Printf("distance: %v\n", distance))

	if len(numbers) > 0 {
		log.Println(fmt.Printf("Station search type: findByNumbers (contract-name: %v , number: %v)\n", contractName, numbers))
		c.JSON(http.StatusOK, findByNumbers(contractName, numbers))
	} else if lat != 0 && lng != 0 {
		log.Println(fmt.Printf("Station search type: findNearby (contract-name: %v, lat: %v, lng: %v, distance: %v)\n", contractName, lat, lng, distance))
		c.JSON(http.StatusOK, findNearby(contractName, lat, lng, distance))
	} else {
		log.Println(fmt.Printf("Station search type: findByContractName (contract-name: %v)\n", contractName))
		c.JSON(http.StatusOK, findByContractName(contractName))
	}
}

func stationsAvailabilityInfosHandler(c *gin.Context) {
	contractName := c.Param("contract-name")
	if contractName == "" {
		contractName = "Paris"
	}
	date, _ := time.Parse("20060102-1504" , c.Param("date"))
	stationNumber := c.Param("stationNumber")

	log.Println(fmt.Printf("contractName: %v\n", contractName))
	log.Println(fmt.Printf("date: %v\n", date))
	log.Println(fmt.Printf("stationNumber: %v\n", stationNumber))

	c.JSON(http.StatusOK, fetchInfluxDbDataByDateAndStationNumber(contractName, date, stationNumber, 60))
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Domain
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func findNearby(contractName string, lat float64, lng float64, distance float64) []Station {

	geoLocations := cli.GeoRadius(contractName + "_stations", lng, lat, &redis.GeoRadiusQuery{ Radius: 100, Unit: "km", WithDist: true, Sort: "ASC" }).Val()

	keys := MapStringsToStrings(MapGeoLocationsToStationNumbers(geoLocations), func(number string) string {
		return contractName + "_" + number
	})

	log.Println(fmt.Printf("keys: %v", keys))

	keysFiltered := FilterString(keys, func(v string) bool {
		return v != contractName + "_images"
	})

	var results = MapObjectsToStrings(FilterNil(cli.MGet(keysFiltered...).Val()))

	var stations = MapStringsToStations(results, UnmarshalStringsToStations)

	return stations
}

func findByNumbers(contractName string, numbers []string) []Station {

	if len(numbers) <= 0 {
		return []Station {}
	}

	keys := MapStringsToStrings(numbers, func(number string) string {
		return contractName + "_" + number
	})

	log.Println(fmt.Printf("keys: %v", keys))

	keysFiltered := FilterString(keys, func(v string) bool {
		return v != contractName + "_images"
	})

	var results = MapObjectsToStrings(FilterNil(cli.MGet(keysFiltered...).Val()))

	var stations = MapStringsToStations(results, UnmarshalStringsToStations)

	return stations
}

func findByContractName(contractName string) []Station {

	keys := cli.Keys(contractName + "_*").Val()

	log.Println(fmt.Printf("keys: %v", keys))

	keysFiltered := FilterString(keys, func(v string) bool {
		return v != contractName + "_images"
	})

	var results = MapObjectsToStrings(FilterNil(cli.MGet(keysFiltered...).Val()))

	var stations = MapStringsToStations(results, UnmarshalStringsToStations)

	return stations
}

func fetchInfluxDbDataByDateAndStationNumber(contractName string, date time.Time, stationNumber string, modulus int64) []StationBikeState {

	dateFormatted := date.Format("2006-01-02 15:04") + ":00"
	query := fmt.Sprintf(
		"SELECT * FROM %s_%s WHERE time >= '%s' AND time < '%s' + 24h",
		contractName,
		stationNumber,
		dateFormatted,
		dateFormatted,
	)
	log.Println(fmt.Printf("query: %v", query))

	results, err := queryDB(influx, dbDatabase, query)
	if err != nil {
		log.Println("Failed on error")
		log.Fatal(err)
	}

	log.Println(fmt.Printf("results: %v\n", results))

	if len(results) <= 0 {
		return []StationBikeState{}
	}

	log.Println(fmt.Printf("results[0]: %v\n", results[0]))

	if len(results[0].Series) <= 0 {
		return []StationBikeState{}
	}

	log.Println(fmt.Printf("results[0].Series[0]: %v\n", results[0].Series[0]))

	if len(results[0].Series[0].Values) <= 0 {
		return []StationBikeState{}
	}

	log.Println(fmt.Printf("results[0].Series[0].Values: %v\n", results[0].Series[0].Values))

	points := results[0].Series[0].Values

	var stationBikeState = FilterStationBikeStatesByModulus(MapPointsToStationBikeStates(points), int(modulus))

	return stationBikeState
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// InfluxDB
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// queryDB convenience function to query the database
func queryDB(clnt client.Client, db string, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Utils
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)

	if value != "" {
		return value
	}

	return defaultValue
}

func FilterString(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func FilterNil(vs []interface{}) []interface{} {
	vsf := make([]interface{}, 0)
	for _, v := range vs {
		if v != nil {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func FilterStationBikeStatesByModulus(vs []StationBikeState, modulus int) []StationBikeState {
	vsf := make([]StationBikeState, 0)
	for i, v := range vs {
		if i %modulus == 0 {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func MapPointsToStationBikeStates(vs [][]interface{}) []StationBikeState {
	vsm := make([]StationBikeState, len(vs))
	for i, v := range vs {
		log.Println(fmt.Printf("v: %v\n", v))
		log.Println(fmt.Printf("v[1]: %v\n", v[1]))

		t := v[0].(string)
		availableBikeStands, _ := v[1].(json.Number).Int64()
		availableBikes, _ := v[2].(json.Number).Int64()

		vsm[i] = StationBikeState{t,  availableBikeStands, availableBikes}
	}
	return vsm
}

func MapObjectsToStrings(vs []interface{}) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = v.(string)
	}
	return vsm
}

func MapGeoLocationsToStationNumbers(vs []redis.GeoLocation) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = v.Name
	}
	return vsm
}

func MapStringsToStrings(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func MapStringsToStations(vs []string, f func(string) Station) []Station {
	vsm := make([]Station, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func UnmarshalStringsToStations(serializedStation string) Station {

	// log.Println(fmt.Printf("serializedStation: %v\n", serializedStation))

	var station Station

	json.Unmarshal([]byte(serializedStation), &station)

	return station
}
