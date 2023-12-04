# cloud-region-to-grid-carbon-mapping

Takes in a CSV file of cloud provider regions and finds the Electricity Maps Zone
and WattTime region using their latitude and longitude.

For Azure the geolocation comes from the Azure CLI. For Google Cloud and AWS the 
OpenStreetMap Nominatim API is used.

```sh
ELECTRICITY_MAPS_API_KEY=***** \
WATT_TIME_USER=***** \
WATT_TIME_PASSWORD=***** \
go run . sample_regions.csv
```
