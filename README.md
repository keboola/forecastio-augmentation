forecastio-augmentation
================

KBC Docker app for getting weather conditions of given locations from 
[Forecast.io API](https://developer.forecast.io/docs/v2). API access
is provisioned by Keboola itself. Optionally it can get historical data. 
You tell the app where it will find table columns with coordinates, 
optionally column with dates, and what weather conditions you want to 
know about them. 
You can get daily or hourly conditions from the API.

## Status

[![Build Status](https://travis-ci.org/keboola/forecastio-augmentation.svg)](https://travis-ci.org/keboola/forecastio-augmentation) 

# Source data

- You can select multiple tables in input mapping
- You have to choose only following columns in the order
 - latitude -  decimal degrees
 - longitude -  decimal degrees
 - date -  (optional) date values must have format `yyyy-mm-dd` (e.g. 2015-06-22)
- you should deduplicate your data to avoid useless exhausting of your credit quota

## Parameters

- **parameters**
    - **conditions** (optional) - array of weather conditions to get, you get all by default; see list of possible conditions below
    - **units** (optional) - units of conditions; **si** for metric units by default, other option is **us** for imperials units 
    - **granularity** (optional) - `daily` or `hourly`. `daily` is default.
## Full Configuration example
```
{
  "storage": {
    "input": {
      "tables": [
        {
          "source": "in.c-csv-import.coords",
          "destination": "coords.csv",
          "columns": [
            "latitude",
            "longitude"
          ]
        }
      ]
    }
  },
  "parameters": {
    "#apiToken": "YOUR_FORECASTIO_TOKEN",
    "conditions": [
      "temperature",
      "pressure",
      "humidity"
    ],
    "units": "si"
  }
}
```

## Supported conditions

- common:
  - **summary**: A human-readable text summary of weather
  - **icon**: A machine-readable text summary of weather (one of: `clear-day, clear-night, rain, snow, sleet, wind, fog, cloudy, partly-cloudy-day, partly-cloudy-night`)
  - **precipIntensity**: A numerical value representing the average expected intensity (in cm/inches of liquid water per hour)
  - **precipProbability**: A numerical value between 0 and 1 (inclusive) representing the probability of precipitation
  - **precipType**: A string representing the type of precipitation occurring at the given time. If defined, this property will have one of the following values: rain, snow, sleet or hail
  - **precipAccumulation**: the amount of snowfall accumulation expected to occur on the given day
  - **dewPoint**: A numerical value representing the dew point at the given time in degrees Celsius/Fahrenheit
  - **windSpeed**: A numerical value representing the wind speed in km/miles per hour
  - **windBearing**: A numerical value representing the direction that the wind is coming from in degrees, with true north at 0° and progressing clockwise. (If windSpeed is zero, then this value will not be defined.)
  - **cloudCover**: A numerical value between 0 and 1 (inclusive) representing the percentage of sky occluded by clouds. A value of 0 corresponds to clear sky, 0.4 to scattered clouds, 0.75 to broken cloud cover, and 1 to completely overcast skies.
  - **humidity**: A numerical value between 0 and 1 (inclusive) representing the relative humidity
  - **pressure**: A numerical value representing the sea-level air pressure in hectopascals/millibars
  - **visibility**: A numerical value representing the average visibility in km/miles, capped at 10 miles.
  - **ozone**: A numerical value representing the columnar density of total atmospheric ozone at the given time in Dobson units.
- only for daily conditions:
  - **sunriseTime**: UNIX timestamp of the last sunrise before the solar noon closest to local noon on the given day
  - **sunsetTime**: UNIX timestamp of the first sunset after the solar noon closest to local noon on the given day
  - **moonPhase**: A number representing the fractional part of [the lunation number](https://en.wikipedia.org/wiki/Lunation_Number) of the given day
  - **precipIntensityMax**: A numerical value representing the average expected intensity (in cm/inches of liquid water per hour)
  - **precipIntensityMin**: A numerical value representing the average expected intensity (in cm/inches of liquid water per hour)
  - **temperatureMin**: A numerical value representing minimal temperature on the given day in degrees Celsius/Fahrenheit
  - **temperatureMinTime**: UNIX timestamp of minimal daily temperature occurance
  - **temperatureMax**: A numerical value representing maximal temperature on the given day in degrees Celsius/Fahrenheit
  - **temperatureMaxTime**: UNIX timestamp of maximal daily temperature occurance
  - **apparentTemperatureMin**: A numerical value representing minimal apparent (or “feels like”) temperature on the given day in degrees Celsius/Fahrenheit
  - **apparentTemperatureMinTime**: UNIX timestamp of minimal daily apparent (or “feels like”) temperature occurance
  - **apparentTemperatureMax**: A numerical value representing maximal apparent (or “feels like”) temperature on the given day in degrees Celsius/Fahrenheit
  - **apparentTemperatureMaxTime**: UNIX timestamp of maximal daily apparent (or “feels like”) temperature occurance
- only for hourly conditions:
  - **temperature**: A numerical value representing the temperature at the given time in degrees Celsius/Fahrenheit
  - **apparentTemperature**: A numerical value representing the apparent (or “feels like”) temperature at the given time in degrees Celsius/Fahrenheit
      
# Output
New bucket is created for each configuration with one table `forecast`. The table will have columns 
`primary,latitude,longitude,date,key,value` and is filled incrementally. Weather conditions are saved as key-value pairs: 

- **primary** - hash of latitude, longitude, date and key used for incremental saving of data
- **latitude** - latitude of coordinates translated from the address
- **longitude** - longitude of coordinates translated from the address
- **date** - date and time of weather conditions validity
- **key** - name of weather condition
- **value** - value of weather condition
