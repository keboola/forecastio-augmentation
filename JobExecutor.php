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
        $coordinates = [];
        $handle = fopen($dataFile, "r");
        if ($handle) {
            while (($line = fgetcsv($handle)) !== false) {
                $coordinates[] = [
                    'lat' => isset($line[0]) ? $line[0] : null,
                    'lon' => isset($line[1]) ? $line[1] : null,
                    'time' => isset($line[2]) ? $line[2] : null,
                    'daily' => false
                ];

                // Run for every 50 lines
                if (count($coordinates) >= $countInBatch) {
                    $this->processBatch($configId, $coordinates, $conditions, $units);
                    $this->eventLogger->log(sprintf('Processed %d queries', $batchNumber * $countInBatch));

                    $coordinates = [];
                    $batchNumber++;
                }

            }
        }
        if (count($coordinates)) {
            // Run the rest of lines above the highest multiple of 50
            $this->processBatch($configId, $coordinates, $conditions, $units);
            $this->eventLogger->log(sprintf('Processed %d queries', (($batchNumber - 1) * $countInBatch) + count($coordinates)));
        }
        fclose($handle);
    }

    public function processBatch($configId, $coordinates, $conditions = [], $units = self::TEMPERATURE_UNITS_SI)
    {
        // Basically analyze validity of coordinates and date
        foreach ($coordinates as $i => &$coordinate) {
            if ($coordinate['lat'] === null || $coordinate['lon'] === null || (!$coordinate['lat'] && !$coordinate['lon'])
                || !is_numeric($coordinate['lat']) || !is_numeric($coordinate['lon'])) {
                $this->eventLogger->log(
                    sprintf("Value '%s %s' is not valid coordinate", $coordinate['lat'], $coordinate['lon']),
                    [],
                    null,
                    EventLogger::TYPE_WARN
                );
                unset($coordinates[$i]);
                continue;
            }
            if (!isset($coordinate['time'])) {
                $coordinate['time'] = $this->actualTime;
            } else {
                if (preg_match('/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/', $coordinate['time'])) {
                    // pass
                } elseif (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $coordinate['time'])) {
                    $coordinate['daily'] = true;
                } else {
                    $this->eventLogger->log(
                        sprintf(
                            "Date value %s for coordinate '%s %s' is not valid",
                            $coordinate['time'],
                            $coordinate['lat'],
                            $coordinate['lon']
                        ),
                        [],
                        null,
                        EventLogger::TYPE_WARN
                    );
                    unset($coordinates[$i]);
                }
            }
        }
        $coordinates = array_values($coordinates);


        // Get and save conditions missing in cache
        $coordinatesByKey = [];
        foreach ($coordinates as $c) {
            $coordinatesByKey[CacheStorage::getCacheKey($c['lat'], $c['lon'], $c['time'], $c['daily'])] = $c;
        }
        $missingKeys = $this->cacheStorage->missing(array_keys($coordinatesByKey));

        $paramsForApi = [];
        foreach ($missingKeys as $key) {
            $c = $coordinatesByKey[$key];
            $paramsForApi[$key] = [
                'latitude' => round($c['lat'], 2),
                'longitude' => round($c['lon'], 2),
                'time' => strtotime($c['time']),
                'units' => 'si',
                'exclude' => 'currently,minutely,alerts,flags'
            ];
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
                    if (isset($data['daily']->data[0])) {
                        $dailyData = (array)$data['daily']->data[0];
                        $time = date('Y-m-d H:i:s', $dailyData['time']);
                        unset($dailyData['time']);
                        foreach ($dailyData as $k => $v) {
                            $this->cacheStorage->save($r->getLatitude(), $r->getLongitude(), $time, $k, $v, true);
                        }
                    }
                    if (isset($data['hourly']->data)) {
                        foreach ($data['hourly']->data as $hourlyData) {
                            $hourlyData = (array)$hourlyData;
                            $time = date('Y-m-d H:i:s', $hourlyData['time']);
                            unset($hourlyData['time']);
                            foreach ($hourlyData as $k => $v) {
                                $this->cacheStorage->save($r->getLatitude(), $r->getLongitude(), $time, $k, $v);
                            }
                        }
                    }
                }
            }
        }


        // Get data from cache
        $data = $this->cacheStorage->get(array_keys($coordinatesByKey), $conditions);

        foreach ($coordinates as $c) {
            $cacheKey = CacheStorage::getCacheKey($c['lat'], $c['lon'], $c['time'], $c['daily']);
            if (isset($data[$cacheKey])) {
                $locationData = $data[$cacheKey];
                foreach ($locationData as $ld) {
                    if ($units == self::TEMPERATURE_UNITS_US) {
                        switch ($ld['key']) {
                            case 'temperature':
                            case 'temperatureMin':
                            case 'temperatureMax':
                            case 'apparentTemperature':
                            case 'dewPoint':
                                // From Fahrenheit To Celsius
                                $ld['value'] = ($ld['value'] * (9 / 5)) + 32;
                                break;
                            case 'precipAccumulation':
                                // From centimeters To inches
                                $ld['value'] = $ld['value'] * 0.393701;
                                break;
                            case 'nearestStormDistance':
                            case 'visibility':
                                // From kilometers To miles
                                $ld['value'] = $ld['value'] * 0.621371;
                                break;
                            case 'precipIntensity':
                            case 'precipIntensityMax':
                                // From millimeters per hour To inches per hour
                                $ld['value'] = $ld['value'] * 0.03937;
                                break;
                            case 'windSpeed':
                                // From meters per second To miles per hour
                                $ld['value'] = $ld['value'] * 2.2369362920544025;
                                break;
                        }
                    }

                    $this->userStorage->save($configId, [
                        'latitude' => $c['lat'],
                        'longitude' => $c['lon'],
                        'date' => $c['time'],
                        'key' => $ld['key'],
                        'value' => $ld['value']
                    ]);
                }
            } else {
                $this->eventLogger->log(
                    sprintf("Conditions for coordinate '%s %s' not found", $c['lat'], $c['lon']),
                    [],
                    null,
                    EventLogger::TYPE_WARN
                );
            }
        }
    }
}
