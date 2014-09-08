<?php
/**
 * Created by IntelliJ IDEA.
 * User: JakubM
 * Date: 04.09.14
 * Time: 15:19
 */

namespace Keboola\ForecastIoExtractorBundle\Extractor;


use Geocoder\Geocoder;
use Geocoder\Provider\ChainProvider;
use Geocoder\Provider\GoogleMapsProvider;
use Geocoder\Provider\MapQuestProvider;
use Geocoder\Provider\NominatimProvider;
use Geocoder\Provider\YandexProvider;
use Keboola\ForecastIoExtractorBundle\ForecastTools\Forecast;
use Keboola\ForecastIoExtractorBundle\ForecastTools\Response;
use Keboola\ForecastIoExtractorBundle\Geocoder\GuzzleAdapter;

class Executor
{
	protected $sharedStorage;
	protected $forecastIoKey;
	protected $googleApiKey;
	protected $mapQuestKey;

	const TEMPERATURE_UNITS_SI = 'si';
	const TEMPERATURE_UNITS_US = 'us';

	public function __construct(SharedStorage $sharedStorage, $googleApiKey, $forecastIoKey, $mapQuestKey)
	{
		$this->sharedStorage = $sharedStorage;
		$this->forecastIoKey = $forecastIoKey;
		$this->googleApiKey = $googleApiKey;
		$this->mapQuestKey = $mapQuestKey;
	}


	public function getForecast($coords, $date, $units=self::TEMPERATURE_UNITS_SI)
	{
		$dateHour = date('YmdH', strtotime($date));

		$savedForecasts = $this->sharedStorage->getSavedForecasts($coords, $date);

		$result = array();
		$apiData = array();
		foreach ($coords as $loc => $c) {
			$key = md5(sprintf('%s.%s.%s', $dateHour, $c['latitude'], $c['longitude']));
			$result[$c['latitude'] . ':' . $c['longitude']] = array(
				'location' => $loc,
				'latitude' => $c['latitude'],
				'longitude' => $c['longitude']
			);
			if (!isset($savedForecasts[$key])) {
				$apiData[] = array(
					'latitude' => $c['latitude'],
					'longitude' => $c['longitude'],
					'units' => 'si'
				);
			} else {
				$result[$c['latitude'] . ':' . $c['longitude']]['temperature'] = $savedForecasts[$key]['temperature'];
				$result[$c['latitude'] . ':' . $c['longitude']]['weather'] = $savedForecasts[$key]['weather'];
			}
		}

		$forecastToSave = array();
		if (count($apiData)) {
			$forecast = new Forecast($this->forecastIoKey, 10);
			foreach ($forecast->getData($apiData) as $r) {
				/** @var Response $r */
				$curr = $r->getCurrently();
				$result[$r->getLatitude() . ':' . $r->getLongitude()]['temperature'] = $curr->getTemperature();
				$result[$r->getLatitude() . ':' . $r->getLongitude()]['weather'] = $curr->getSummary();
				$forecastToSave[] = array(
					md5(sprintf('%s.%s.%s', $dateHour, $r->getLatitude(), $r->getLongitude())),
					$date,
					$r->getLatitude(),
					$r->getLongitude(),
					$curr->getTemperature(),
					$curr->getSummary()
				);
			}
			if (count($forecastToSave)) {
				$this->sharedStorage->updateTable(SharedStorage::FORECASTS_TABLE_NAME, $forecastToSave);
			}
		}

		$finalResult = array();
		foreach ($coords as $loc => $c) {
			$res = $result[$c['latitude'] . ':' . $c['longitude']];
			$finalResult[] = array(
				'address' => $loc,
				'latitude' => $c['latitude'],
				'longitude' => $c['longitude'],
				'date' => $date,
				'temperature' => ($units == self::TEMPERATURE_UNITS_US)? ($res['temperature'] * (9/5)) + 32 : $res['temperature'],
				'weather' => $res['weather']
			);
		}
		return $finalResult;
	}


	public function getCoordinates($locations)
	{
		$savedLocations = $this->sharedStorage->getSavedLocations($locations);

		$result = array();
		$locationsToSave = array();
		foreach ($locations as $loc) {
			if (!isset($savedLocations[$loc])) {
				$location = $this->getAddressCoordinates($loc);
				$coords = $location? $this->getForecastLocation($location) : array('latitude' => '-', 'longitude' => '-');
				$savedLocations[$loc] = $coords;
				$locationsToSave[] = array($loc, $coords['latitude'], $coords['longitude']);
			}
			$result[$loc] = $savedLocations[$loc];
		}

		if (count($locationsToSave)) {
			$this->sharedStorage->updateTable(SharedStorage::LOCATIONS_TABLE_NAME, $locationsToSave);
		}
		return $result;
	}

	public function getAddressCoordinates($address)
	{
		$adapter = new GuzzleAdapter();

		$chain = new ChainProvider(array(
			new GoogleMapsProvider($adapter, null, null, true, $this->googleApiKey),
			new MapQuestProvider($adapter, $this->mapQuestKey),
			new YandexProvider($adapter),
			new NominatimProvider($adapter, 'http://nominatim.openstreetmap.org'),
		));
		$geocoder = new Geocoder($chain);
		try {
			$geocode = $geocoder->geocode($address);
			return $geocode->getCoordinates();
		} catch (\Exception $e) {
			echo $e->getMessage();
			return false;
		}
	}

	public function getForecastLocation($coords)
	{
		return array(
			'latitude' => round($coords[0], 1),
			'longitude' => round($coords[1], 1)
		);
	}

} 