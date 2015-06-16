<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation;

use Keboola\ForecastIoAugmentation\ForecastTools\Forecast;
use Keboola\ForecastIoAugmentation\ForecastTools\Response;
use Keboola\ForecastIoAugmentation\Service\ConfigurationStorage;
use Keboola\ForecastIoAugmentation\Service\EventLogger;
use Keboola\ForecastIoAugmentation\Service\CacheStorage;
use Keboola\ForecastIoAugmentation\Service\UserStorage;
use Monolog\Logger;
use Keboola\Temp\Temp;
use Keboola\Syrup\Job\Metadata\Job;

class JobExecutor extends \Keboola\Syrup\Job\Executor
{
    /**
     * @var Forecast
     */
    protected $forecast;

    /**
     * @var CacheStorage
     */
    protected $cacheStorage;
    /**
     * @var \Keboola\Temp\Temp
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
     * @var EventLogger
     */
    protected $eventLogger;

    protected $actualTime;

    const TEMPERATURE_UNITS_SI = 'si';
    const TEMPERATURE_UNITS_US = 'us';

    public function __construct(CacheStorage $cacheStorage, Temp $temp, Logger $logger, $forecastIoKey)
    {
        $this->cacheStorage = $cacheStorage;
        $this->temp = $temp;
        $this->logger = $logger;
        $this->actualTime = date('Y-m-d H:i:s');

        $this->forecast = new Forecast($forecastIoKey, 10);
    }

    /**
     * @TODO move config to parameters only?
     * @TODO save all conditions to output table?
     */
    public function execute(Job $job)
    {
        $configurationStorage = new ConfigurationStorage($this->storageApi);
        $this->eventLogger = new EventLogger($this->storageApi, $job->getId());
        $this->userStorage = new UserStorage($this->storageApi, $this->temp);

        $params = $job->getParams();
        $configIds = isset($params['config'])? [$params['config']] : $configurationStorage->getConfigurationsList();

        foreach ($configIds as $configId) {
            $configuration = $configurationStorage->getConfiguration($configId);
            foreach ($configuration as $configTable) {
                $columnsToGet = [$configTable['latitudeCol'], $configTable['longitudeCol']];
                if (!empty($configTable['timeCol'])) {
                    $columnsToGet[] = $configTable['timeCol'];
                }
                $dataFile = $this->userStorage->getData($configTable['tableId'], $columnsToGet);

                $this->process($configId, $dataFile, $configTable['conditions'], $configTable['units']);
            }
        }

        $this->userStorage->uploadData();
    }

    public function process($configId, $dataFile, $conditions = [], $units = self::TEMPERATURE_UNITS_SI)
    {
        // Download file with data column to disk and read line-by-line
        // Query Geocoding API by 50 queries
        $batchNumber = 1;
        $countInBatch = 50;
        $lines = [];
        $handle = fopen($dataFile, "r");
        if ($handle) {
            while (($line = fgetcsv($handle)) !== false) {
                $lines[] = $line;

                // Run for every 50 lines
                if (count($lines) >= $countInBatch) {
                    $this->processBatch($configId, $lines, $conditions, $units);
                    $this->eventLogger->log(sprintf('Processed %d queries', $batchNumber * $countInBatch));

                    $lines = [];
                    $batchNumber++;
                }

            }
        }
        if (count($lines)) {
            // Run the rest of lines above the highest multiple of 50
            $this->processBatch($configId, $lines, $conditions, $units);
            $this->eventLogger->log(sprintf('Processed %d queries', (($batchNumber - 1) * $countInBatch) + count($lines)));
        }
        fclose($handle);
    }

    public function processBatch($configId, $coordinates, $conditions = [], $units = self::TEMPERATURE_UNITS_SI)
    {
        // Basically analyze validity of coordinates and date
        foreach ($coordinates as $i => &$c) {
            if ($c[0] === null || $c[1] === null || (!$c[0] && !$c[1]) || !is_numeric($c[0]) || !is_numeric($c[1])) {
                $this->eventLogger->log(sprintf("Value '%s %s' is not valid coordinate", $c[0], $c[1]), [], null, EventLogger::TYPE_WARN);
                unset($coordinates[$i]);
                continue;
            }
            if (!isset($c[2])) {
                $c[2] = $this->actualTime;
            } else {
                $timestamp = strtotime($c[2]);
                if (!$timestamp) {
                    $this->eventLogger->log(sprintf("Time value %s for coordinates '%s %s' is not valid", $c[2], $c[0], $c[1]), [], null, EventLogger::TYPE_WARN);
                    unset($coordinates[$i]);
                }
            }
        }
        $coordinates = array_values($coordinates);

        $cache = $this->cacheStorage->get($coordinates, $conditions);
        $result = [];

        $paramsForApi = [];
        foreach ($coordinates as $coord) {
            // Round coordinates to two decimals, will be sufficient for weather requests
            $lat = round($coord[0], 2);
            $lon = round($coord[1], 2);
            $cacheKey = CacheStorage::getCacheKey($coord[0], $coord[1], $coord[2]);
            if (!isset($cache[$cacheKey])) {
                if (!isset($paramsForApi[$cacheKey])) {
                    $paramsForApi[$cacheKey] = [
                        'latitude' => $lat,
                        'longitude' => $lon,
                        'time' => strtotime($coord[2]),
                        'units' => 'si',
                        'exclude' => 'minutely,hourly,daily,alerts,flags'
                    ];
                }
            } else {
                foreach ($cache[$cacheKey] as $sc) {
                    $result[$cacheKey][$sc['key']] = $sc['value'];
                }
            }
        }

        if (count($paramsForApi)) {
            foreach ($this->forecast->getData($paramsForApi) as $r) {
                /** @var Response $r */
                $data = (array)$r->getRawData();
                if (isset($data['error'])) {
                    $this->logger->debug('Getting conditions failed', [
                        'coords' => $data['coords'],
                        'time' => date('Y-m-d H:i:s', $data['time']),
                        'error' => $data['error']
                    ]);
                } else {
                    $currentlyData = (array)$data['currently'];
                    $time = date('Y-m-d H:i:s', $currentlyData['time']);
                    unset($currentlyData['time']);
                    $cacheKey = CacheStorage::getCacheKey($r->getLatitude(), $r->getLongitude(), $time);
                    foreach ($currentlyData as $k => $v) {
                        $this->cacheStorage->save($r->getLatitude(), $r->getLongitude(), $time, $k, $v);
                        if (!count($conditions) || in_array($k, $conditions)) {
                            $result[$cacheKey][$k] = $v;
                        }
                    }
                }
            }
        }

        foreach ($coordinates as $coord) {
            $cacheKey = CacheStorage::getCacheKey($coord[0], $coord[1], $coord[2]);
            if (isset($result[$cacheKey])) {
                $res = $result[$cacheKey];
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

                    $this->userStorage->save($configId, [
                        'latitude' => $coord[0],
                        'longitude' => $coord[1],
                        'date' => $coord[2],
                        'key' => $k,
                        'value' => $v
                    ]);
                }
            } else {
                $this->eventLogger->log(sprintf("Conditions for coordinate '%s %s' not found", $coord[0], $coord[1]), [], null, EventLogger::TYPE_WARN);
            }
        }
    }
}
