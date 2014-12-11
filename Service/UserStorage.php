<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Keboola\StorageApi\Table as StorageApiTable;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Syrup\ComponentBundle\Filesystem\Temp;

class UserStorage
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApiClient;
	/**
	 * @var \Syrup\ComponentBundle\Filesystem\Temp
	 */
	protected $temp;

	const BUCKET_NAME = 'ex-forecastio';
	const BUCKET_ID = 'in.c-ex-forecastio';
	const CONDITIONS_TABLE_NAME = 'conditions';

	public $tables = array(
		self::CONDITIONS_TABLE_NAME => array(
			'columns' => array('address', 'latitude', 'longitude', 'date', 'key', 'value'),
			'primaryKey' => null,
			'indices' => array()
		)
	);


	public function __construct(Client $storageApi, Temp $temp)
	{
		$this->storageApiClient = $storageApi;
		$this->temp = $temp;
	}

	public function saveConditions($data)
	{
		$this->updateTable(self::CONDITIONS_TABLE_NAME, $data);
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

	public function getTableColumnData($tableId, $column)
	{
		$params = array(
			'format' => 'escaped',
			'columns' => array($column)
		);

		$file = $this->temp->createTmpFile();
		$fileName = $file->getRealPath();
		$exporter = new TableExporter($this->storageApiClient);
		$exporter->exportTable($tableId, $fileName, $params);

		return $fileName;
	}
} 