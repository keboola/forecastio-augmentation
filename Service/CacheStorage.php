<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;

class CacheStorage
{
    /**
     * @var \Doctrine\DBAL\Connection
     */
    protected $db;
    const TABLE_NAME = 'forecastio_cache';

    public function __construct(Connection $db)
    {
        $this->db = $db;
    }

    public static function getCacheTimeFormat($date, $daily = false)
    {
        $format = $daily ? 'Ymd:' : 'Ymd:H';
        return date($format, strtotime($date));
    }

    public static function getCacheKey($lat, $lon, $date, $daily = false)
    {
        return sprintf('%s:%s:%s', round($lat, 2), round($lon, 2), self::getCacheTimeFormat($date, $daily));
    }

    public function missing($keys)
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
}
