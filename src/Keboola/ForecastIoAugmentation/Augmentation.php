<?php
/**
 * @package forecastio-augmentation
 * @copyright Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\ForecastIoAugmentation;

use Symfony\Component\Process\Process;

class Augmentation
{
    const TEMPERATURE_UNITS_SI = 'si';
    const TEMPERATURE_UNITS_US = 'us';

    protected $actualTime;

    /** @var UserStorage */
    protected $userStorage;

    /** @var \Forecast */
    protected $api;

    /** @var CacheStorage  */
    protected $cacheStorage;

    public function __construct($apiKey, array $dbParams, $folder, $bucket)
    {
        $this->api = new \Forecast($apiKey, 10);
        $this->cacheStorage = new CacheStorage($dbParams);
        $this->actualTime = date('Y-m-d 12:00:00');
        
        $this->userStorage = new UserStorage($folder, $bucket);
    }


    public function process(
        $dataFile,
        $latitude,
        $longitude,
        $time = null,
        array $conditions = [],
        $units = self::TEMPERATURE_UNITS_SI
    ) {
        $handle = fopen($dataFile, "r");
        $header = fgetcsv($handle);
        $latitudeIndex = array_search($latitude, $header);
        $longitudeIndex = array_search($longitude, $header);
        $timeIndex = $time ? array_search($time, $header) : false;
        fclose($handle);

        $this->prepareFile($dataFile, $latitudeIndex, $longitudeIndex, $timeIndex);

        $handle = fopen($dataFile, "r");

        $latitudeIndex = 0;
        $longitudeIndex = 1;
        $timeIndex = $time ? 2 : false;

        // query for each 50 lines from the file
        $countInBatch = 50;
        $queries = [];
        while (($line = fgetcsv($handle)) !== false) {
            $queries[] = [
                'lat' => $line[$latitudeIndex],
                'lon' => $line[$longitudeIndex],
                'time' => $timeIndex !== false ? $line[$timeIndex] : null,
                'daily' => false
            ];

            // Run for every 50 lines
            if (count($queries) >= $countInBatch) {
                $this->processBatch($queries, $conditions, $units);

                $queries = [];
            }
        }

        if (count($queries)) {
            // run the rest of lines above the highest multiple of 50
            $this->processBatch($queries, $conditions, $units);
        }
        fclose($handle);
    }

    public function processBatch($queries, array $conditions = [], $units = self::TEMPERATURE_UNITS_SI)
    {
        $queries = $this->validateQueries($queries);

        $queriesByKey = [];
        foreach ($queries as $q) {
            $cacheKey = CacheStorage::getCacheKey($q['lat'], $q['lon'], $q['time'], $q['daily']);
            $queriesByKey[$cacheKey] = $q;
        }

        $this->getMissingDataFromApi($queriesByKey);
        

        // Get data from cache
        $data = $this->cacheStorage->get(array_keys($queriesByKey), $conditions);

        foreach ($queries as $q) {
            $cacheKey = CacheStorage::getCacheKey($q['lat'], $q['lon'], $q['time'], $q['daily']);
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
                                // From Fahrenheit to Celsius
                                $ld['value'] = ($ld['value'] * (9 / 5)) + 32;
                                break;
                            case 'precipAccumulation':
                                // From centimeters to inches
                                $ld['value'] = $ld['value'] * 0.393701;
                                break;
                            case 'nearestStormDistance':
                            case 'visibility':
                                // From kilometers to miles
                                $ld['value'] = $ld['value'] * 0.621371;
                                break;
                            case 'precipIntensity':
                            case 'precipIntensityMax':
                                // From millimeters per hour to inches per hour
                                $ld['value'] = $ld['value'] * 0.03937;
                                break;
                            case 'windSpeed':
                                // From meters per second to miles per hour
                                $ld['value'] = $ld['value'] * 2.2369362920544025;
                                break;
                        }
                    }

                    $this->userStorage->save(KBC_CONFIGID, [
                        'primary' => md5($q['lat'].':'.$q['lon'].':'.$q['time'].':'.$ld['key']),
                        'latitude' => $q['lat'],
                        'longitude' => $q['lon'],
                        'date' => $q['time'],
                        'key' => $ld['key'],
                        'value' => $ld['value']
                    ]);
                }
            } else {
                error_log("Conditions for coordinate '{$q['lat']} {$q['lon']}' not found");
            }
        }
    }

    /**
     * Basically analyze validity of coordinates and date
     */
    protected function validateQueries(array $queries)
    {
        foreach ($queries as $i => &$q) {
            if ($q['lat'] === null || $q['lon'] === null || (!$q['lat'] && !$q['lon'])
                || !is_numeric($q['lat']) || !is_numeric($q['lon'])) {
                error_log("Value '{$q['lat']} {$q['lon']}' is not valid coordinate");
                unset($queries[$i]);
                continue;
            }
            if (!isset($q['time'])) {
                $q['time'] = $this->actualTime;
            } else {
                if (preg_match('/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/', $q['time'])) {
                    if (substr($q['time'], 0, 10) > date('Y-m-d')) {
                        error_log("Date '{$q['time']}' for coordinate '{$q['lat']} {$q['lon']}' lies in future");
                        unset($queries[$i]);
                    }
                } elseif (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $q['time'])) {
                    $q['daily'] = true;
                    if ($q['time'] > date('Y-m-d')) {
                        error_log("Date '{$q['time']}' for coordinate '{$q['lat']} {$q['lon']}' lies in future");
                        unset($queries[$i]);
                    }
                } else {
                    error_log("Date value '{$q['time']}' for coordinate '{$q['lat']} {$q['lon']}' is not valid");
                    unset($queries[$i]);
                }
            }
        }
        return array_values($queries);
    }
    
    protected function getMissingDataFromApi(array $queriesByKey)
    {
        $missingKeys = $this->cacheStorage->getMissingKeys(array_keys($queriesByKey));

        $paramsForApi = [];
        foreach ($missingKeys as $key) {
            $q = $queriesByKey[$key];
            $paramsForApi[$key] = [
                'latitude' => CacheStorage::roundCoordinate($q['lat']),
                'longitude' => CacheStorage::roundCoordinate($q['lon']),
                'time' => $q['daily'] ? $q['time'].'T12:00:00' : str_replace(' ', 'T', $q['time']),
                'units' => 'si',
                'exclude' => 'currently,minutely,alerts,flags'
            ];
        }

        if (count($paramsForApi)) {
            foreach ($this->api->getData($paramsForApi) as $r) {
                /** @var \ForecastResponse $r */
                $data = (array)$r->getRawData();
                if (isset($data['error'])) {
                    error_log("Getting conditions for {$data['coords']} on {$data['time']} failed: {$data['error']}");
                } else {
                    $dataToSave = [];
                    if (isset($data['daily']->data[0])) {
                        $dailyData = (array)$data['daily']->data[0];
                        $time = date('Y-m-d H:i:s', $dailyData['time']);
                        unset($dailyData['time']);
                        foreach ($dailyData as $k => $v) {
                            $dataToSave[] = [
                                'location' => CacheStorage::getCacheKey($r->getLatitude(), $r->getLongitude(), $time, true),
                                'key' => $k,
                                'value' => $v
                            ];
                        }
                    }
                    if (isset($data['hourly']->data)) {
                        foreach ($data['hourly']->data as $hourlyData) {
                            $hourlyData = (array)$hourlyData;
                            $time = date('Y-m-d H:i:s', $hourlyData['time']);
                            unset($hourlyData['time']);
                            foreach ($hourlyData as $k => $v) {
                                $dataToSave[] = [
                                    'location' => CacheStorage::getCacheKey($r->getLatitude(), $r->getLongitude(), $time, false),
                                    'key' => $k,
                                    'value' => $v
                                ];
                            }
                        }
                    }
                    $this->cacheStorage->saveBulk($dataToSave);
                }
            }
        }
    }

    /**
     * Command removes all columns except lat, lon and time, removes header and deduplicates rows
     */
    protected function prepareFile($file, $latIndex, $lonIndex, $timeIndex = false)
    {
        // cut indexes columns from 1
        $latIndex++;
        $lonIndex++;
        if ($timeIndex !== false) {
            $timeIndex++;
        }
        $this->runCliCommand("mv $file $file.orig");
        $this->runCliCommand(
            "cat {$file}.orig "
            . "| cut -d, -f{$latIndex},{$lonIndex}" . ($timeIndex !== false ? ",$timeIndex" : null)
            . "| sed -e \"1d\" | sort | uniq > $file"
        );
        unlink("$file.orig");
        return $file;
    }

    protected function runCliCommand($command)
    {
        $process = new Process($command);
        $process->setTimeout(null);
        $process->run();
        if (!$process->isSuccessful()) {
            print("Preparation of csv file failed with command '$command' on: " . $process->getErrorOutput());
            exit(1);
        }
    }
}
