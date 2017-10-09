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

    const GRANULARITY_HOURLY = 'hourly';
    const  GRANULARITY_DAILY = 'daily';

    const COL_LATITUDE = 0;
    const COL_LONGITUDE = 1;
    const COL_DATE = 2;

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
        $units = self::TEMPERATURE_UNITS_SI,
        $granularity = self::GRANULARITY_DAILY
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
                $queries[] = $this->buildQuery($line, $units, $granularity);
            } catch (Exception $e) {
                error_log($e->getMessage());
                continue;
            }

            // Run for every 50 lines
            if (count($queries) >= $countInBatch) {
                $this->processBatch($queries, $conditions, $granularity);

                $queries = [];
            }
        }

        if (count($queries)) {
            // run the rest of lines above the highest multiple of 50
            $this->processBatch($queries, $conditions, $granularity);
        }
    }

    public function processBatch($queries, array $conditions, $granularity)
    {
        foreach ($this->api->getData($queries) as $r) {
            /** @var \ForecastResponse $r */
            $data = (array) $r->getRawData();
            if (isset($data['error'])) {
                error_log("Getting conditions for {$data['coords']} on {$data['time']} failed: {$data['error']}");
                continue;
            }
            if ($granularity === self::GRANULARITY_DAILY) {
                $dailyData = (array) $data['daily']->data[0];
                $this->saveData(
                    $data['latitude'],
                    $data['longitude'],
                    date('Y-m-d', $dailyData['time']),
                    $dailyData,
                    $conditions
                );
            } elseif ($granularity === self::GRANULARITY_HOURLY) {
                foreach ($data['hourly']->data as $hourlyData) {
                    $this->saveData(
                        $data['latitude'],
                        $data['longitude'],
                        date('Y-m-d H:i:s', $hourlyData->time),
                        (array) $hourlyData,
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
    protected function buildQuery($q, $units, $granularity)
    {
        if ($q[self::COL_LATITUDE] === null || $q[self::COL_LONGITUDE] === null || (!$q[self::COL_LATITUDE] && !$q[self::COL_LONGITUDE]) || !is_numeric($q[self::COL_LATITUDE]) || !is_numeric($q[self::COL_LONGITUDE])) {
            throw new Exception("Value '{$q[self::COL_LATITUDE]} {$q[self::COL_LONGITUDE]}' is not valid coordinate");
        }

        $result = [
            'latitude' => $q[self::COL_LATITUDE],
            'longitude' => $q[self::COL_LONGITUDE],
            'units' => $units
        ];

        switch ($granularity) {
            case self::GRANULARITY_DAILY:
                $result['exclude'] = 'currently,minutely,hourly,alerts,flags';
                break;
            case self::GRANULARITY_HOURLY:
                $result['exclude'] = 'currently,minutely,daily,alerts,flags';
                break;
            default:
                throw new Exception("Granularity {$granularity} is not valid.");
        }

        if (!empty($q[self::COL_DATE])) {
            if (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $q[self::COL_DATE])) {
                if ($q[self::COL_DATE] > date('Y-m-d')) {
                    throw new Exception("Date '{$q[self::COL_DATE]}' for coordinate '{$q[self::COL_LATITUDE]} {$q[self::COL_LONGITUDE]}' lies in future");
                }
                $result['time'] = "{$q[self::COL_DATE]}T12:00:00";
            } else {
                throw new Exception("Date '{$q[self::COL_DATE]}' for coordinate '{$q[self::COL_LATITUDE]} {$q[self::COL_LONGITUDE]}' is not valid");
            }
        } else {
            $result['time'] = $this->actualTime;
        }

        return $result;
    }
}
