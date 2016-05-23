<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\DriverManager;

class CacheStorage
{
    /** @var Connection */
    protected $db;

    const TABLE_NAME = 'forecastio_cache';
    const CALLS_COUNT_TABLE_NAME = 'forecastio_calls_count';

    public function __construct($params)
    {
        $this->db = DriverManager::getConnection($params);
    }

    public static function roundCoordinate($value)
    {
        return round($value, 1);
    }

    public static function getCacheTimeFormat($date, $daily = false)
    {
        $format = $daily ? 'Ymd:' : 'Ymd:H';
        return date($format, strtotime($date));
    }

    public static function getCacheKey($lat, $lon, $date, $daily = false)
    {
        return sprintf(
            '%s:%s:%s',
            self::roundCoordinate($lat),
            self::roundCoordinate($lon),
            self::getCacheTimeFormat($date, $daily)
        );
    }

    public function getMissingKeys($keys)
    {
        $result = $this->db->fetchAll(
            'SELECT location FROM forecastio_cache WHERE location IN (?) GROUP BY location',
            [$keys],
            [Connection::PARAM_STR_ARRAY]
        );
        $foundKeys = [];
        foreach ($result as $r) {
            $foundKeys[] = $r['location'];
        }
        return array_values(array_diff($keys, $foundKeys));
    }

    public function get($keys, $conditions = [])
    {
        if (count($conditions)) {
            $query = $this->db->fetchAll(
                'SELECT * FROM ' . self::TABLE_NAME . ' AS t WHERE t.location IN (?) AND t.key IN (?)',
                [$keys, $conditions],
                [Connection::PARAM_STR_ARRAY, Connection::PARAM_STR_ARRAY]
            );
        } else {
            $query = $this->db->fetchAll(
                'SELECT * FROM ' . self::TABLE_NAME . ' AS t WHERE t.location IN (?)',
                [$keys],
                [Connection::PARAM_STR_ARRAY]
            );
        }
        $result = [];
        foreach ($query as $q) {
            if (!isset($result[$q['location']])) {
                $result[$q['location']] = [];
            }
            $result[$q['location']][] = $q;
        }
        return $result;
    }

    public function save($lat, $lon, $date, $key, $value, $daily = false)
    {
        try {
            $this->db->insert(self::TABLE_NAME, [
                'location' => self::getCacheKey($lat, $lon, $date, $daily),
                '`key`' => $key,
                'value' => $value
            ]);
        } catch (DBALException $e) {
            // Ignore
        }
    }

    public function saveBulk($data)
    {
        if (count($data)) {
            $sqlValues = [];
            foreach ($data as $d) {
                $sqlValues[] = sprintf(
                    '(%s, %s, %s)',
                    $this->db->quote($d['location']),
                    $this->db->quote($d['key']),
                    $this->db->quote($d['value'])
                );
            }
            $sql = 'REPLACE INTO ' . self::TABLE_NAME . ' (`location`, `key`, `value`) VALUES '
                . implode(', ', $sqlValues) . ';';

            $this->db->executeQuery($sql);
        }
    }

    public function logApiCallsCount($projectId, $projectName, $tokenId, $tokenDesc, $count)
    {
        $this->db->insert(self::CALLS_COUNT_TABLE_NAME, [
            'project_id' => $projectId,
            'project_name' => $projectName,
            'token_id' => $tokenId,
            'token_desc' => $tokenDesc,
            'count' => $count
        ]);
    }
}
