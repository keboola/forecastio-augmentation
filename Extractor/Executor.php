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
use Syrup\ComponentBundle\Filesystem\Temp;
use Syrup\ComponentBundle\Job\Metadata\Job;

class Executor extends \Syrup\ComponentBundle\Job\Executor
{
	protected $forecastIoKey;
	protected $googleApiKey;
	protected $mapQuestKey;

	protected $sharedStorage;
	protected $temp;
	protected $userStorage;
	protected $configuration;

	const TEMPERATURE_UNITS_SI = 'si';
	const TEMPERATURE_UNITS_US = 'us';

	public function __construct(AppConfiguration $appConfiguration, SharedStorage $sharedStorage, Temp $temp)
	{
		$this->sharedStorage = $sharedStorage;
		$this->temp = $temp;

		$this->forecastIoKey = $appConfiguration->forecastio_key;
		$this->googleApiKey = $appConfiguration->google_key;
		$this->mapQuestKey = $appConfiguration->mapquest_key;
	}

	public function execute(Job $job)
	{
		$this->configuration = new Configuration($this->storageApi);
		$this->userStorage = new UserStorage($this->storageApi, $this->temp);

		foreach ($this->configuration->getConfiguration() as $config) {
			$locations = $this->userStorage->getTableColumn($config['tableId'], $config['column']);

			$coordinates = $this->getCoordinates($locations);
			$result = $this->getConditions($coordinates, date('c'), $config['conditions'], $config['units']);

			$this->userStorage->saveConditions($result);
		}
	}

	public function getConditions($coordinates, $date, $conditions=null, $units=self::TEMPERATURE_UNITS_SI)
	{
		$savedConditions = $this->sharedStorage->getSavedConditions($coordinates, $date, $conditions);
		$result = array();
		$locations = array();

		$apiData = array();
		foreach ($coordinates as $address => $c) if ($c['latitude'] != '-' && $c['longitude'] != '-') {
			$location = $c['latitude'] . ':' . $c['longitude'];
			if (!isset($savedConditions[$location])) {
				$apiData[] = array(
					'latitude' => $c['latitude'],
					'longitude' => $c['longitude'],
					'units' => 'si'
				);
				$locations[$location] = $address;
			} else {
				foreach ($savedConditions[$location] as $sc) {
					$result[$c['latitude'] . ':' . $c['longitude']][$sc['key']] = $sc['value'];
				}
			}
		}

		if (count($apiData)) {
			$forecast = new Forecast($this->forecastIoKey, 10);
			foreach ($forecast->getData($apiData) as $r) {
				/** @var Response $r */
				$allConditions = (array)$r->getRawData()->currently;
				unset($allConditions['time']);
				foreach ($allConditions as $k => $v) {
					$this->sharedStorage->saveCondition($r->getLatitude(), $r->getLongitude(), $date, $k, $v);
					if (!$conditions || in_array($k, $conditions)) {
						$result[$r->getLatitude() . ':' . $r->getLongitude()][$k] = $v;
					}
				}
			}
		}

		$finalResult = array();
		foreach ($coordinates as $address => $c) {
			$res = $result[$c['latitude'] . ':' . $c['longitude']];
			foreach ($res as $k => $v) {

				if ($units == self::TEMPERATURE_UNITS_US) {
					switch ($k) {
						case 'temperature':
						case 'temperatureMin':
						case 'temperatureMax':
						case 'apparentTemperature':
						case 'dewPoint':
							// From Fahrenheit To Celsius
							$v = ($v * (9/5)) + 32;
							break;
						case 'precipAccumulation':
							// From centimeters To inches
							$v = $v * 0.393701;
							break;
						case 'nearestStormDistance':
						case 'visibility':
							// From kilometers To miles
							$v = $v * 0.621371;
							break;
						case 'precipIntensity':
						case 'precipIntensityMax':
							// From millimeters per hour To inches per hour
							$v = $v * 0.03937;
							break;
						case 'windSpeed':
							// From meters per second To miles per hour
							$v = $v * 2.2369362920544025;
							break;
					}
				}

				$finalResult[] = array(
					'address' => $address,
					'latitude' => $c['latitude'],
					'longitude' => $c['longitude'],
					'date' => $date,
					'key' => $k,
					'value' => $v
				);
			}
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
				$savedLocations[$loc] = $this->getForecastLocation($location);
				$locationsToSave[] = array(
					'name' => $loc,
					'latitude' => $savedLocations[$loc]['latitude'],
					'longitude' => $savedLocations[$loc]['longitude']
				);
			}
			$result[$loc] = $savedLocations[$loc];
		}

		if (count($locationsToSave)) foreach ($locationsToSave as $loc) {
			$this->sharedStorage->saveLocation($loc['name'], $loc['latitude'], $loc['longitude']);
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
		$geoCoder = new Geocoder($chain);
		try {
			$geocode = $geoCoder->geocode($address);
			return $geocode->getCoordinates();
		} catch (\Exception $e) {
			echo $e->getMessage();
			return false;
		}
	}

	public function getForecastLocation($coordinates)
	{
		return $coordinates? array(
			'latitude' => round($coordinates[0], 2),
			'longitude' => round($coordinates[1], 2)
		) : array(
			'latitude' => '-',
			'longitude' => '-'
		);
	}

} 