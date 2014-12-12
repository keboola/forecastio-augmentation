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

	public function get($coordinates, $date, $conditions=null)
	{
		$locations = array();
		foreach ($coordinates as $c) {
			$locations[] = round($c[0], 2) . ':' . round($c[1], 2);
		}
		if ($conditions) {
			$query = $this->db->fetchAll('SELECT * FROM (SELECT * FROM ' . self::TABLE_NAME . ' WHERE location IN (?) AND date=?) AS t WHERE t.key IN (?)',
				array($locations, date('Y-m-d H:00:00', strtotime($date)), $conditions),
				array(Connection::PARAM_STR_ARRAY, \PDO::PARAM_STR, Connection::PARAM_STR_ARRAY));
		} else {
			$query = $this->db->fetchAll('SELECT * FROM ' . self::TABLE_NAME . ' WHERE location IN (?) AND date=?',
				array($locations, date('Y-m-d H:00:00', strtotime($date))),
				array(Connection::PARAM_STR_ARRAY, \PDO::PARAM_STR));
		}
		$result = array();
		foreach ($query as $q) {
			if (!isset($result[$q['location']]))
				$result[$q['location']] = array();
			$result[$q['location']][] = $q;
		}
		return $result;
	}

	public function save($lat, $lon, $date, $key, $value)
	{
		try {
			$this->db->insert(self::TABLE_NAME, array(
				'location' => round($lat, 2) . ':' . round($lon, 2),
				'date' => date('Y-m-d H:00:00', strtotime($date)),
				'`key`' => $key,
				'value' => $value
			));
		} catch (DBALException $e) {
			// Ignore
		}
	}
} 