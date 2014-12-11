<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation;


use Geocoder\Exception\ChainNoResultException;
use Geocoder\Geocoder;
use Geocoder\Provider\ChainProvider;
use Geocoder\Provider\GoogleMapsProvider;
use Geocoder\Provider\MapQuestProvider;
use Geocoder\Provider\NominatimProvider;
use Geocoder\Provider\YandexProvider;
use Keboola\ForecastIoAugmentation\ForecastTools\Forecast;
use Keboola\ForecastIoAugmentation\ForecastTools\Response;
use Keboola\ForecastIoAugmentation\Geocoder\GuzzleAdapter;
use Keboola\ForecastIoAugmentation\Service\AppConfiguration;
use Keboola\ForecastIoAugmentation\Service\Configuration;
use Keboola\ForecastIoAugmentation\Service\EventLogger;
use Keboola\ForecastIoAugmentation\Service\SharedStorage;
use Keboola\ForecastIoAugmentation\Service\UserStorage;
use Monolog\Logger;
use Syrup\ComponentBundle\Filesystem\Temp;
use Syrup\ComponentBundle\Job\Metadata\Job;

class JobExecutor extends \Syrup\ComponentBundle\Job\Executor
{
	protected $forecastIoKey;
	protected $googleApiKey;
	protected $mapQuestKey;

	/**
	 * @var SharedStorage
	 */
	protected $sharedStorage;
	/**
	 * @var \Syrup\ComponentBundle\Filesystem\Temp
	 */
	protected $temp;
	/**
	 * @var \Monolog\Logger
	 */
	protected $logger;
	/**
	 * @var UserStorage
	 */
	protected $userStorage;
	/**
	 * @var Configuration
	 */
	protected $configuration;
	/**
	 * @var EventLogger
	 */
	protected $eventLogger;
	/**
	 * @var AppConfiguration
	 */
	protected $appConfiguration;

	const TEMPERATURE_UNITS_SI = 'si';
	const TEMPERATURE_UNITS_US = 'us';

	public function __construct(AppConfiguration $appConfiguration, SharedStorage $sharedStorage, Temp $temp, Logger $logger)
	{
		$this->appConfiguration = $appConfiguration;
		$this->sharedStorage = $sharedStorage;
		$this->temp = $temp;
		$this->logger = $logger;

		$this->forecastIoKey = $appConfiguration->forecastio_key;
		$this->googleApiKey = $appConfiguration->google_key;
		$this->mapQuestKey = $appConfiguration->mapquest_key;
	}

	public function execute(Job $job)
	{
		$this->eventLogger = new EventLogger($this->appConfiguration, $this->storageApi, $job->getId());
		$this->configuration = new Configuration($this->storageApi);
		$this->userStorage = new UserStorage($this->storageApi, $this->temp);

		$addressesInBatch = 50;
		$batchNum = 1;

		foreach ($this->configuration->getConfiguration() as $config) {
			$locationsFile = $this->userStorage->getTableColumnData($config['tableId'], $config['column']);
			$locations = array();
			$firstRow = true;
			$handle = fopen($locationsFile, "r");
			if ($handle) {
				while (($line = fgetcsv($handle)) !== false) {
					if ($firstRow) {
						$firstRow = false;
					} else {
						$locations[] = $line[0];
						if (count($locations) >= $addressesInBatch) {
							$this->process($config, $locations);
							$locations = array();
							$this->eventLogger->log('Processed ' . ($batchNum * $addressesInBatch) . ' addresses');
							$batchNum++;
						}
					}
				}
			}
			if (count($locations)) {
				$this->process($config, $locations);
			}
			fclose($handle);
		}
	}

	public function process($config, $locations)
	{
		$coordinates = $this->getCoordinates($locations);
		$result = $this->getConditions($coordinates, date('c'), $config['conditions'], $config['units']);
		$this->userStorage->saveConditions($result);
	}

	public function getConditions($coordinates, $date, $conditions=null, $units=self::TEMPERATURE_UNITS_SI)
	{
		$savedConditions = $this->sharedStorage->getSavedConditions($coordinates, $date, $conditions);
		$result = array();
		$locations = array();

		$apiData = array();
		foreach ($coordinates as $address => $c) if ($c['latitude'] != 0 && $c['longitude'] != 0) {
			$location = round($c['latitude'], 2) . ':' . round($c['longitude'], 2);
			if (!isset($savedConditions[$location])) {
				if (!array_key_exists($location, $locations)) {
					$apiData[] = array(
						'latitude' => $c['latitude'],
						'longitude' => $c['longitude'],
						'units' => 'si'
					);
					$locations[$location] = $address;
				}
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
		foreach ($coordinates as $address => $c) if ($c['latitude'] != 0 && $c['longitude'] != 0) {
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
		$result = array();
		$savedLocations = $this->sharedStorage->getSavedLocations($locations);

		$locationsToSave = array();
		foreach ($locations as $loc) {
			if (!isset($savedLocations[$loc])) {
				if (!in_array($loc, $locationsToSave)) {
					$locationsToSave[] = $loc;
				}
			} else {
				$result[$loc] = $savedLocations[$loc];
			}
		}

		if (count($locationsToSave)) {
			$adapter = new GuzzleAdapter();
			$geocoder = new Geocoder();
			$geocoder->registerProvider(new ChainProvider(array(
				new GoogleMapsProvider($adapter, null, null, true, $this->googleApiKey),
				new MapQuestProvider($adapter, $this->mapQuestKey),
				new YandexProvider($adapter),
				new NominatimProvider($adapter, 'http://nominatim.openstreetmap.org'),
			)));
			$geotools = new \League\Geotools\Geotools();

			$geocoded = $geotools->batch($geocoder)->geocode($locationsToSave)->parallel();
			foreach ($geocoded as $g) {
				/** @var \League\Geotools\Batch\BatchGeocoded $g */
				$result[$g->getQuery()] = array('latitude' => $g->getLatitude(), 'longitude' => $g->getLongitude());
				$this->sharedStorage->saveLocation($g->getQuery(), $g->getLatitude(), $g->getLongitude());
				if ($g->getLatitude() == 0 && $g->getLongitude() == 0) {
					$this->eventLogger->log('No coordinates for address "' . $g->getQuery() . '" found', array(), null, EventLogger::TYPE_WARN);
				}
			}
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
		} catch (ChainNoResultException $e) {
			// Ignore no result
		} catch (\Exception $e) {
			$this->logger->alert('Error from Geocoding of address ' . $address, array('e' => $e));
		}
		return false;
	}

} 