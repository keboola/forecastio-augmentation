<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;
use Keboola\StorageApi\Client;
use Syrup\ComponentBundle\Exception\SyrupComponentException;
use Syrup\ComponentBundle\Service\StorageApi\StorageApiService;

class ConfigurationException extends SyrupComponentException
{
	public function __construct($message, $previous = null)
	{
		parent::__construct(400, $message, $previous);
	}
}

class Configuration
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApiClient;

	const BUCKET_ID = 'sys.c-ag-forecastio';
	const TABLE_NAME = 'configuration';

	public function __construct(Client $storageApi)
	{
		$this->storageApiClient = $storageApi;
	}

	public function getConfiguration()
	{
		if (!$this->storageApiClient->bucketExists(self::BUCKET_ID)) {
			throw new ConfigurationException('Configuration bucket ' . self::BUCKET_ID . ' does not exist');
		}
		if (!$this->storageApiClient->tableExists(self::BUCKET_ID . '.' . self::TABLE_NAME)) {
			throw new ConfigurationException('Configuration table ' . self::BUCKET_ID . '.' . self::TABLE_NAME . ' does not exist');
		}

		$csv = $this->storageApiClient->exportTable(self::BUCKET_ID . '.' . self::TABLE_NAME);
		$table = StorageApiClient::parseCsv($csv, true);

		if (!count($table)) {
			throw new ConfigurationException('Configuration table ' . self::BUCKET_ID . '.' . self::TABLE_NAME . ' is empty');
		}

		if (!isset($table[0]['tableId']) || !isset($table[0]['latitudeCol']) || !isset($table[0]['longitudeCol'])
			|| !isset($table[0]['conditions']) || !isset($table[0]['units'])) {
			throw new ConfigurationException('Configuration table ' . self::BUCKET_ID . '.' . self::TABLE_NAME
				. ' should contain columns tableId,latitudeCol,longitudeCol,conditions,units');
		}

		$result = array();
		foreach ($table as $t) {
			try {
				if (!$this->storageApiClient->tableExists($t['tableId'])) {
					throw new ConfigurationException('Table ' . $t['tableId'] . ' does not exist');
				}
			} catch (\Keboola\StorageApi\ClientException $e) {
				if ($e->getCode() == 403) {
					throw new ConfigurationException('Table ' . $t['tableId'] . ' is not accessible with your token');
				} else {
					throw $e;
				}
			}
			$tableInfo = $this->storageApiClient->getTable($t['tableId']);

			if (!in_array($t['latitudeCol'], $tableInfo['columns'])) {
				throw new ConfigurationException('Column with latitudes ' . $t['latitudeCol'] . ' does not exist in that table');
			}
			if (!in_array($t['longitudeCol'], $tableInfo['columns'])) {
				throw new ConfigurationException('Column with latitudes ' . $t['longitudeCol'] . ' does not exist in that table');
			}

			$result[] = array(
				'tableId' => $t['tableId'],
				'latitudeCol' => $t['latitudeCol'],
				'longitudeCol' => $t['longitudeCol'],
				'conditions' => empty($t['conditions'])? null : explode(',', $t['conditions']),
				'units' => $t['units']? $t['units'] : 'si'
			);
		}
		return $result;
	}
} 