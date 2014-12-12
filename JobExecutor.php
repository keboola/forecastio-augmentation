<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation;


use Keboola\ForecastIoAugmentation\ForecastTools\Forecast;
use Keboola\ForecastIoAugmentation\ForecastTools\Response;
use Keboola\ForecastIoAugmentation\Service\Configuration;
use Keboola\ForecastIoAugmentation\Service\EventLogger;
use Keboola\ForecastIoAugmentation\Service\SharedStorage;
use Keboola\ForecastIoAugmentation\Service\UserStorage;
use Monolog\Logger;
use Syrup\ComponentBundle\Filesystem\Temp;
use Syrup\ComponentBundle\Job\Metadata\Job;

class JobExecutor extends \Syrup\ComponentBundle\Job\Executor
{
	/**
	 * @var Forecast
	 */
	protected $forecast;

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

	const TEMPERATURE_UNITS_SI = 'si';
	const TEMPERATURE_UNITS_US = 'us';

	public function __construct(SharedStorage $sharedStorage, Temp $temp, Logger $logger, $forecastIoKey)
	{
		$this->sharedStorage = $sharedStorage;
		$this->temp = $temp;
		$this->logger = $logger;

		$this->forecast = new Forecast($forecastIoKey, 10);
	}

	/**
	 * @TODO move config to parameters only?
	 * @TODO save all conditions to output table?
	 */
	public function execute(Job $job)
	{
		$this->configuration = new Configuration($this->storageApi);
		$this->eventLogger = new EventLogger($this->storageApi, $job->getId());
		$this->userStorage = new UserStorage($this->storageApi, $this->temp);

		foreach ($this->configuration->getConfiguration() as $config) {
			$dataFile = $this->userStorage->getData($config['tableId'], array($config['latitudeCol'], $config['longitudeCol']));

			$this->process($dataFile, date('c'), $config['conditions'], $config['units']);
		}

		$this->userStorage->uploadData();
	}

	public function process($dataFile, $date, $conditions=null, $units=self::TEMPERATURE_UNITS_SI)
	{
		// Download file with data column to disk and read line-by-line
		// Query Geocoding API by 50 queries
		$batchNumber = 1;
		$countInBatch = 50;
		$lines = array();
		$handle = fopen($dataFile, "r");
		if ($handle) {
			while (($line = fgetcsv($handle)) !== false) {
				$lines[] = $line;

				// Run for every 50 lines
				if (count($lines) >= $countInBatch) {
					$this->processBatch($lines, $date, $conditions, $units);
					$this->eventLogger->log(sprintf('Processed %d queries', $batchNumber * $countInBatch));

					$lines = array();
					$batchNumber++;
				}

			}
		}
		if (count($lines)) {
			// Run the rest of lines above the highest multiple of 50
			$this->processBatch($lines, $date, $conditions, $units);
			$this->eventLogger->log(sprintf('Processed %d queries', (($batchNumber - 1) * $countInBatch) + count($lines)));
		}
		fclose($handle);
	}

	public function processBatch($coordinates, $date, $conditions=null, $units=self::TEMPERATURE_UNITS_SI)
	{
		$cache = $this->sharedStorage->get($coordinates, $date, $conditions);
		$result = array();

		$paramsForApi = array();
		foreach ($coordinates as $c) {

			// Basically analyze validity of coordinate
			if ($c[0] === null || $c[1] === null || (!$c[0] && !$c[1]) || !is_numeric($c[0]) || !is_numeric($c[1])) {
				$this->eventLogger->log(sprintf("Value '%s %s' is not valid coordinate", $c[0], $c[1]), array(), null, EventLogger::TYPE_WARN);
			} else {

				// Round coordinates to two decimals, will be sufficient for weather requests
				$lat = round($c[0], 2);
				$lon = round($c[1], 2);
				$locKey = sprintf('%s:%s', $lat, $lon);
				if (!isset($cache[$locKey])) {
					if (!isset($paramsForApi[$locKey])) {
						$paramsForApi[$locKey] = array(
							'latitude' => $lat,
							'longitude' => $lon,
							'units' => 'si'
						);
					}
				} else {
					foreach ($cache[$locKey] as $sc) {
						$result[$locKey][$sc['key']] = $sc['value'];
					}
				}
			}
		}

		if (count($paramsForApi)) {
			foreach ($this->forecast->getData($paramsForApi) as $r) {
				/** @var Response $r */
				$allConditions = (array)$r->getRawData()->currently;
				unset($allConditions['time']);
				$locKey = sprintf('%s:%s', $r->getLatitude(), $r->getLongitude());
				foreach ($allConditions as $k => $v) {
					$this->sharedStorage->save($r->getLatitude(), $r->getLongitude(), $date, $k, $v);
					if (!$conditions || in_array($k, $conditions)) {
						$result[$locKey][$k] = $v;
					}
				}
			}
		}

		foreach ($coordinates as $coord) {
			$locKey = sprintf('%s:%s', round($coord[0], 2), round($coord[1], 2));
			if (isset($result[$locKey])) {
				$res = $result[$locKey];
				foreach ($res as $k => $v) {

					if ($units == self::TEMPERATURE_UNITS_US) {
						switch ($k) {
							case 'temperature':
							case 'temperatureMin':
							case 'temperatureMax':
							case 'apparentTemperature':
							case 'dewPoint':
								// From Fahrenheit To Celsius
								$v = ($v * (9 / 5)) + 32;
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

					$this->userStorage->save(array(
						'latitude' => $coord[0],
						'longitude' => $coord[1],
						'date' => $date,
						'key' => $k,
						'value' => $v
					));
				}
			} else {
				$this->eventLogger->log(sprintf("Conditions for coordinate '%s %s' not found", $coord[0], $coord[1]), array(), null, EventLogger::TYPE_WARN);
			}
		}
	}

} 