package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	awsCloudProvider            = "Amazon Web Services"
	electricityMapsAPIKeyEnvVar = "ELECTRICITY_MAPS_API_KEY"
	electricityMapsBaseURL      = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest"
	openStreetMapBaseURL        = "https://nominatim.openstreetmap.org/search"
	wattTimeUserEnvVar          = "WATT_TIME_USER"
	wattTimePasswordEnvVar      = "WATT_TIME_PASSWORD"
	wattTimeLoginURL            = "https://api.watttime.org/login"
	wattTimeRegionURL           = "https://api.watttime.org/v3/region-from-loc"
)

var (
	header = []string{
		"cloud_provider",
		"cloud_region",
		"location",
		"location_override",
		"location_source",
		"location_type",
		"latitude",
		"longitude",
		"electricity_maps_zone",
		"watt_time_region",
	}
)

func main() {
	err := mainWithError(context.Background())
	if err != nil {
		fmt.Printf("%#v", err)
	}
}

func mainWithError(ctx context.Context) error {
	electricityMapsAPIKey := os.Getenv(electricityMapsAPIKeyEnvVar)
	if electricityMapsAPIKey == "" {
		return fmt.Errorf("%s env var must be set", electricityMapsAPIKey)
	}

	wattTimeUser := os.Getenv(wattTimeUserEnvVar)
	if wattTimeUser == "" {
		return fmt.Errorf("%s env var must be set", wattTimeUserEnvVar)
	}
	wattTimePassword := os.Getenv(wattTimePasswordEnvVar)
	if wattTimePassword == "" {
		return fmt.Errorf("%s env var must be set", wattTimePasswordEnvVar)
	}
	wattTimeAccessToken, err := getWattTimeAccessToken(ctx, wattTimeUser, wattTimePassword)
	if err != nil {
		return err
	}

	regions, err := loadRegions(ctx, os.Args[1])
	if err != nil {
		return err
	}

	for i, region := range regions {
		if region.ElectricityMapsZone == "" {
			zone, err := getElectricityMapsZone(ctx, electricityMapsAPIKey, region.Latitude, region.Longitude)
			if err != nil {
				return err
			}
			region.ElectricityMapsZone = zone
		}
		if region.WattTimeRegion == "" {
			regionID, err := getWattTimeRegion(ctx, wattTimeAccessToken, region.Latitude, region.Longitude)
			if err != nil {
				return err
			}
			region.WattTimeRegion = regionID
		}
		regions[i] = region

		// Sleep to prevent rate limiting
		time.Sleep(1 * time.Second)
	}

	writer := csv.NewWriter(os.Stdout)

	if err := writer.Write(header); err != nil {
		return err
	}

	for _, region := range regions {
		record := []string{
			region.CloudProvider,
			region.CloudRegion,
			region.Location,
			region.LocationOverride,
			region.LocationSource,
			region.LocationType,
			fmt.Sprintf("%f", region.Latitude),
			fmt.Sprintf("%f", region.Longitude),
			region.ElectricityMapsZone,
			region.WattTimeRegion,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return err
	}

	return nil
}

func getElectricityMapsZone(ctx context.Context, apiKey string, latitude, longitude float64) (string, error) {
	params := url.Values{}
	params.Add("lat", fmt.Sprintf("%f", latitude))
	params.Add("lon", fmt.Sprintf("%f", longitude))
	requestURL := electricityMapsBaseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	req.Header.Add("auth-token", apiKey)
	if err != nil {
		return "", err
	}

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No coverage
		return "", nil
	} else if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("expected %d response got %d", http.StatusOK, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var electricityMapsResp ElectricityMapsResponse

	err = json.Unmarshal(body, &electricityMapsResp)
	if err != nil {
		return "", err
	}

	return electricityMapsResp.Zone, nil
}

func getGeolocation(ctx context.Context, location, locationType string) (float64, float64, error) {
	params := url.Values{}
	params.Add(locationType, location)
	params.Add("format", "json")
	params.Add("limit", "1")
	requestURL := openStreetMapBaseURL + "?" + params.Encode()

	// fmt.Println(requestURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return 0, 0, err
	}

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("expected %d response got %d", http.StatusOK, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	var places []OpenStreetMapPlace

	err = json.Unmarshal(body, &places)
	if err != nil {
		return 0, 0, err
	}

	if len(places) == 1 {
		latitude, err := strconv.ParseFloat(places[0].Lat, 64)
		if err != nil {
			return 0, 0, err
		}
		longitude, err := strconv.ParseFloat(places[0].Lon, 64)
		if err != nil {
			return 0, 0, err
		}

		return latitude, longitude, nil
	}

	return 0, 0, nil
}

func getWattTimeAccessToken(ctx context.Context, apiUser, apiPassword string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wattTimeLoginURL, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(apiUser, apiPassword)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("expected %d response got %d", http.StatusOK, resp.StatusCode)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil
	}

	loginResp := WattTimeLoginResp{}
	err = json.Unmarshal(bytes, &loginResp)
	if err != nil {
		return "", nil
	}

	return loginResp.Token, nil
}

func getWattTimeRegion(ctx context.Context, accessToken string, latitude, longitude float64) (string, error) {
	params := url.Values{}
	params.Add("latitude", fmt.Sprintf("%f", latitude))
	params.Add("longitude", fmt.Sprintf("%f", longitude))
	params.Add("signal_type", "co2_moer")
	requestURL := wattTimeRegionURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No coverage
		return "", nil
	} else if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("expected %d response got %d", http.StatusOK, resp.StatusCode)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil
	}

	regionResp := WattTimeRegionResp{}
	err = json.Unmarshal(bytes, &regionResp)
	if err != nil {
		return "", nil
	}

	return regionResp.Region, nil
}

func loadRegions(ctx context.Context, fileName string) ([]Region, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read and ignore the header line.
	if _, err := reader.Read(); err != nil {
		return nil, err
	}

	var regions []Region

	for {
		record, err := reader.Read()
		if err != nil {
			if err == csv.ErrFieldCount {
				continue
			}
			break
		}

		var latitude, longitude float64

		if record[6] != "" && record[7] != "" {
			latitude, err = strconv.ParseFloat(record[6], 64)
			if err != nil {
				return nil, err
			}
			longitude, err = strconv.ParseFloat(record[7], 64)
			if err != nil {
				return nil, err
			}
		} else {
			cloudProvider := record[0]
			location := record[2]
			locationOverride := record[3]
			if locationOverride != "" {
				location = locationOverride
			}
			locationType := record[5]
			if locationOverride == "" && cloudProvider == awsCloudProvider {
				location = parseAWSLocation(location)
			}

			latitude, longitude, err = getGeolocation(ctx, location, locationType)
			if err != nil {
				return nil, err
			}
		}

		region := Region{
			CloudProvider:       record[0],
			CloudRegion:         record[1],
			Location:            record[2],
			LocationOverride:    record[3],
			LocationSource:      record[4],
			LocationType:        record[5],
			Latitude:            latitude,
			Longitude:           longitude,
			ElectricityMapsZone: record[8],
			WattTimeRegion:      record[9],
		}
		regions = append(regions, region)
	}

	return regions, nil
}

func parseAWSLocation(input string) string {
	var location string

	split := strings.Split(input, "(")
	if len(split) > 0 {
		location = split[1]
	}
	if strings.HasSuffix(location, ")") {
		location = strings.Replace(location, ")", "", 1)
	}

	return location
}

type Region struct {
	CloudProvider       string
	CloudRegion         string
	Location            string
	LocationOverride    string
	LocationSource      string
	LocationType        string
	Latitude            float64
	Longitude           float64
	ElectricityMapsZone string
	WattTimeRegion      string
}

type ElectricityMapsResponse struct {
	Zone string `json:"zone"`
}

type OpenStreetMapPlace struct {
	Lat string `json:"lat"`
	Lon string `json:"lon"`
}

type WattTimeLoginResp struct {
	Token string `json:"token"`
}

type WattTimeRegionResp struct {
	Region string `json:"region"`
}
