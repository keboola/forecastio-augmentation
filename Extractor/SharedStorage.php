<?php
/**
 * Created by IntelliJ IDEA.
 * User: JakubM
 * Date: 04.09.14
 * Time: 14:32
 */

namespace Keboola\ForecastIoExtractorBundle\Extractor;

use Doctrine\DBAL\Connection;

class SharedStorage
{
	/**
	 * @var \Doctrine\DBAL\Connection
	 */
	protected $db;

	public function __construct(\Doctrine\Bundle\DoctrineBundle\Registry $doctrine)
	{
		$this->db = $doctrine->getConnection();
	}

	public function getSavedLocations($locations)
	{
		$query = $this->db->fetchAll('SELECT * FROM locations WHERE name IN (?)', array($locations), array(Connection::PARAM_STR_ARRAY));
		$result = array();
		foreach ($query as $q) {
			$result[$q['name']] = array('latitude' => $q['latitude'], 'longitude' => $q['longitude']);
		}
		return $result;
	}

	public function saveLocation($name, $lat, $lon)
	{
		$this->db->insert('locations', array(
			'name' => $name,
			'latitude' => $lat,
			'longitude' => $lon
		));
	}

	public function getSavedConditions($coordinates, $date, $conditions)
	{
		$locations = array();
		foreach ($coordinates as $c) {
			$locations[] = $c['latitude'] . ':' . $c['longitude'];
		}
		$query = $this->db->fetchAll('SELECT * FROM (SELECT * FROM conditions WHERE location IN (?) AND date=?) AS t WHERE t.key IN (?)',
			array($locations, date('Y-m-d H:00:00', strtotime($date)), $conditions),
			array(Connection::PARAM_STR_ARRAY, \PDO::PARAM_STR, Connection::PARAM_STR_ARRAY));
		$result = array();
		foreach ($query as $q) {
			if (!isset($result[$q['location']]))
				$result[$q['location']] = array();
			$result[$q['location']][] = $q;
		}
		return $result;
	}

	public function saveCondition($lat, $lon, $date, $key, $value)
	{
		$this->db->insert('conditions', array(
			'location' => $lat . ':' . $lon,
			'date' => date('Y-m-d H:00:00', strtotime($date)),
			'`key`' => $key,
			'value' => $value
		));
	}
} 