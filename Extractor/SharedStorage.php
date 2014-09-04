<?php
/**
 * Created by IntelliJ IDEA.
 * User: JakubM
 * Date: 04.09.14
 * Time: 14:32
 */

namespace Keboola\ForecastIoExtractorBundle\Extractor;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

class SharedStorage
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApiClient;

	const BUCKET_ID = 'in.c-ex-forecastio';
	const LOCATIONS_TABLE_NAME = 'locations';
	const FORECASTS_TABLE_NAME = 'forecasts';

	public $tables = array(
		self::LOCATIONS_TABLE_NAME => array(
			'columns' => array('name', 'latitude', 'longitude'),
			'primaryKey' => 'name',
			'indices' => array()
		),
		self::FORECASTS_TABLE_NAME => array(
			'columns' => array('key', 'date', 'latitude', 'longitude', 'temperature', 'weather'),
			'primaryKey' => 'key',
			'indices' => array()
		)
	);

	public function __construct(StorageApiClient $storageApiClient)
	{
		$this->storageApiClient = $storageApiClient;
	}

	public function getTableRows($tableName, $whereColumn, $whereValue, $options=array())
	{
		if (!isset($this->tables[$tableName])) {
			throw new \Exception('Storage table ' . $tableName . ' not found');
		}

		$exportOptions = array();
		if ($whereColumn) {
			$exportOptions = array_merge($exportOptions, array(
				'whereColumn' => $whereColumn,
				'whereValues' => !is_array($whereValue) ? array($whereValue) : $whereValue
			));
		}
		if (count($options)) {
			$exportOptions = array_merge($exportOptions, $options);
		}

		$csv = $this->storageApiClient->exportTable(self::BUCKET_ID . '.' . $tableName, null, $exportOptions);
		return StorageApiClient::parseCsv($csv, true);
	}

	public function updateTable($tableName, $data)
	{
		if (!isset($this->tables[$tableName])) {
			throw new \Exception('Storage table ' . $tableName . ' not found');
		}

		$table = new StorageApiTable($this->storageApiClient, self::BUCKET_ID . '.' . $tableName, null, $this->tables[$tableName]['primaryKey']);
		$table->setHeader($this->tables[$tableName]['columns']);
		$table->setFromArray($data);
		$table->setIncremental(true);
		$table->save();
	}


	public function getSavedLocations($locations)
	{
		$savedLocations = array();
		foreach($this->getTableRows(SharedStorage::LOCATIONS_TABLE_NAME, 'name', $locations) as $row) {
			$savedLocations[$row['name']] = array('latitude' => $row['latitude'], 'longitude' => $row['longitude']);
		}
		return $savedLocations;
	}

	public function getSavedForecasts($coords, $date)
	{
		$dateHour = date('YmdH', strtotime($date));
		$savedKeys = array();
		foreach ($coords as $c) {
			$savedKeys[] = md5(sprintf('%s.%s.%s', $dateHour, $c['latitude'], $c['longitude']));
		}

		$savedForecasts = array();
		foreach($this->getTableRows(SharedStorage::FORECASTS_TABLE_NAME, 'key', $savedKeys) as $row) {
			$savedForecasts[$row['key']] = array('date' => $row['date'], 'latitude' => $row['latitude'],
				'longitude' => $row['longitude'], 'temperature' => $row['temperature'], 'weather' => $row['weather']);
		}
		return $savedForecasts;
	}
} 