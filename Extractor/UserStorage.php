<?php
/**
 * Created by IntelliJ IDEA.
 * User: JakubM
 * Date: 08.09.14
 * Time: 13:46
 */

namespace Keboola\ForecastIoExtractorBundle\Extractor;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

class UserStorage
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApiClient;

	const BUCKET_NAME = 'ex-forecastio';
	const BUCKET_ID = 'in.c-ex-forecastio';
	const FORECASTS_TABLE_NAME = 'forecasts';

	public $tables = array(
		self::FORECASTS_TABLE_NAME => array(
			'columns' => array('address', 'date', 'latitude', 'longitude', 'temperature', 'weather'),
			'primaryKey' => null,
			'indices' => array()
		)
	);


	public function __construct(StorageApiClient $storageApiClient)
	{
		$this->storageApiClient = $storageApiClient;
	}

	public function saveForecasts($data)
	{
		$this->updateTable(self::FORECASTS_TABLE_NAME, $data);
	}

	public function updateTable($tableName, $data)
	{
		if (!isset($this->tables[$tableName])) {
			throw new \Exception('Storage table ' . $tableName . ' not found');
		}

		if (!$this->storageApiClient->bucketExists(self::BUCKET_ID)) {
			$this->storageApiClient->createBucket(self::BUCKET_NAME, 'in', 'Forecast.Io Extractor Data Storage');
		}
		$table = new StorageApiTable($this->storageApiClient, self::BUCKET_ID . '.' . $tableName, null, $this->tables[$tableName]['primaryKey']);
		$table->setHeader(array_keys($data[0]));
		$table->setFromArray($data);
		$table->setIncremental(true);
		$table->save();
	}
} 