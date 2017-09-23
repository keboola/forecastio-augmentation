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

    public function __construct($apiKey, $outputFile)
    {
        $this->api = new \Forecast($apiKey, 10);
        $this->actualTime = date('Y-m-d\TH:i:s');
        
        $this->userStorage = new UserStorage($outputFile);
    }


    public function process(
        $dataFile,
        array $conditions = [],
        $units = self::TEMPERATURE_UNITS_SI
    ) {
        $csvFile = new \Keboola\Csv\CsvFile($dataFile);

        // query for each 50 lines from the file
        $countInBatch = 50;
        $queries = [];
        foreach ($csvFile as $row => $line) {
            if ($row == 0) {
                continue;
            }
            try {
                $queries[] = $this->buildQuery($line, $units);
            } catch (Exception $e) {
                error_log($e->getMessage());
                continue;
            }

            // Run for every 50 lines
            if (count($queries) >= $countInBatch) {
                $this->processBatch($queries, $conditions);

                $queries = [];
            }
        }

        if (count($queries)) {
            // run the rest of lines above the highest multiple of 50
            $this->processBatch($queries, $conditions);
        }
    }

    public function processBatch($queries, array $conditions = [])
    {
        foreach ($this->api->getData($queries) as $r) {
            /** @var \ForecastResponse $r */
            $data = (array)$r->getRawData();
            if (isset($data['error'])) {
                error_log("Getting conditions for {$data['coords']} on {$data['time']} failed: {$data['error']}");
            } else {
                if (isset($data['daily']->data[0])) {
                    $dailyData = (array)$data['daily']->data[0];
                    $this->saveData(
                        $data['latitude'],
                        $data['longitude'],
                        date('Y-m-d', $dailyData['time']),
                        $dailyData,
                        $conditions
                    );
                }
                if (isset($data['currently'])) {
                    $currentlyData = (array)$data['currently'];
                    $this->saveData(
                        $data['latitude'],
                        $data['longitude'],
                        date('Y-m-d H:i:s', $currentlyData['time']),
                        $currentlyData,
                        $conditions
                    );
                }
            }
        }
    }

    protected function saveData($latitude, $longitude, $time, $data, $conditions)
    {
        unset($data['time']);
        foreach ($data as $key => $value) {
            if (in_array($key, $conditions) || !count($conditions)) {
                $this->userStorage->save([
                    'primary' => md5("{$latitude}:{$longitude}:{$time}:{$key}"),
                    'latitude' => $latitude,
                    'longitude' => $longitude,
                    'date' => $time,
                    'key' => $key,
                    'value' => $value
                ]);
            }
        }
    }

    /**
     * Basically analyze validity of coordinates and date
     */
    protected function buildQuery($q, $units)
    {
        if ($q[0] === null || $q[1] === null || (!$q[0] && !$q[1]) || !is_numeric($q[0]) || !is_numeric($q[1])) {
            throw new Exception("Value '{$q[0]} {$q[1]}' is not valid coordinate");
        }

        $result = [
            'latitude' => $q[0],
            'longitude' => $q[1],
            'units' => $units
        ];

        if (!empty($q[2])) {
            if (preg_match('/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/', $q[2])) {
                if (substr($q[2], 0, 10) > date('Y-m-d')) {
                    throw new Exception("Date '{$q[2]}' for coordinate '{$q[0]} {$q[1]}' lies in future");
                }
                $result['time'] = str_replace(' ', 'T', $q[2]);
                $result['exclude'] = 'minutely,daily,alerts,flags';
            } elseif (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $q[2])) {
                if ($q[2] > date('Y-m-d')) {
                    throw new Exception("Date '{$q[2]}' for coordinate '{$q[0]} {$q[1]}' lies in future");
                }
                $result['time'] = "{$q[2]}T12:00:00";
                $result['exclude'] = 'currently,minutely,alerts,flags';
            } else {
                throw new Exception("Date '{$q[2]}' for coordinate '{$q[0]} {$q[1]}' is not valid");
            }
        } else {
            $result['time'] = $this->actualTime;
            $result['exclude'] = 'minutely,daily,alerts,flags';
        }

        return $result;
    }
}
