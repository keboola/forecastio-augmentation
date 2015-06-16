<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;

class SharedStorage
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

    public static function getCacheTimeFormat($date)
    {
        return date('Ymd:H', strtotime($date));
    }

    public function get($coordinates, $conditions = [])
    {
        $locations = array();
        foreach ($coordinates as $c) {
            $locations[] = round($c[0], 2) . ':' . round($c[1], 2) . ':' . self::getCacheTimeFormat($c[2]);
        }
        if (count($conditions)) {
            $query = $this->db->fetchAll(
                'SELECT * FROM ' . self::TABLE_NAME . ' AS t WHERE t.location IN (?) AND t.key IN (?)',
                [$locations, $conditions],
                [Connection::PARAM_STR_ARRAY, Connection::PARAM_STR_ARRAY]
            );
        } else {
            $query = $this->db->fetchAll(
                'SELECT * FROM ' . self::TABLE_NAME . ' AS t WHERE t.location IN (?)',
                [$locations],
                [Connection::PARAM_STR_ARRAY]
            );
        }
        $result = [];
        foreach ($query as $q) {
            if (!isset($result[$q['location']])) {
                $result[$q['location']] = array();
            }
            $result[$q['location']][] = $q;
        }
        return $result;
    }

    public function save($lat, $lon, $date, $key, $value)
    {
        try {
            $this->db->insert(self::TABLE_NAME, [
                'location' => round($lat, 2) . ':' . round($lon, 2) . ':' . self::getCacheTimeFormat($date),
                '`key`' => $key,
                'value' => $value
            ]);
        } catch (DBALException $e) {
            // Ignore
        }
    }
}
