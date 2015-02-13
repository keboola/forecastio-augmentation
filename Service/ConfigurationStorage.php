<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Client;
use Keboola\Syrup\Exception\SyrupComponentException;

class ConfigurationException extends SyrupComponentException
{
	public function __construct($message, $previous = null)
	{
		parent::__construct(400, $message, $previous);
	}
}

class ConfigurationStorage
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApiClient;

	const BUCKET_ID = 'sys.c-ag-forecastio';

	public function __construct(Client $storageApi)
	{
		$this->storageApiClient = $storageApi;
	}

	public function getConfigurationsList()
	{
		if (!$this->storageApiClient->bucketExists(self::BUCKET_ID)) {
			throw new ConfigurationException(sprintf('Configuration bucket %s does not exist', self::BUCKET_ID));
		}
		$result = array();
		foreach ($this->storageApiClient->listTables(self::BUCKET_ID) as $table) {
			$result[] = $table['name'];
		}
		return $result;
	}

	public function getConfiguration($config)
	{
		$configTableId = sprintf('%s.%s', self::BUCKET_ID, $config);

		if (!$this->storageApiClient->bucketExists(self::BUCKET_ID)) {
			throw new ConfigurationException(sprintf('Configuration bucket %s does not exist', self::BUCKET_ID));
		}
		if (!$this->storageApiClient->tableExists($configTableId)) {
			throw new ConfigurationException(sprintf('Configuration table %s does not exist', $configTableId));
		}

		$csv = $this->storageApiClient->exportTable($configTableId);
		$table = StorageApiClient::parseCsv($csv, true);

		$conditions = array();
		$units = null;
		$tableInfo = $this->storageApiClient->getTable($configTableId);
		foreach ($tableInfo['attributes'] as $attr) {
			switch ($attr['name']) {
				case 'conditions':
					$conditions = explode(',', $attr['value']);
					break;
				case 'units':
					$units = $attr['value'];
					break;
			}
		}

		if (!count($table)) {
			throw new ConfigurationException(sprintf('Configuration table %s is empty', $configTableId));
		}

		if (!isset($table[0]['tableId']) || !isset($table[0]['latitudeCol']) || !isset($table[0]['longitudeCol'])) {
			throw new ConfigurationException(sprintf('Configuration table %s should contain columns '
				.'tableId,latitudeCol,longitudeCol', $configTableId));
		}

		$result = array(
			'conditions' => $conditions,
			'units' => $units,
			'tables' => array()
		);
		foreach ($table as $t) {
			try {
				if (!$this->storageApiClient->tableExists($t['tableId'])) {
					throw new ConfigurationException(sprintf('Data table %s does not exist', $t['tableId']));
				}
			} catch (\Keboola\StorageApi\ClientException $e) {
				if ($e->getCode() == 403) {
					throw new ConfigurationException(sprintf('Data table %s is not accessible with your token', $t['tableId']));
				} else {
					throw $e;
				}
			}
			$tableInfo = $this->storageApiClient->getTable($t['tableId']);

			if (!in_array($t['latitudeCol'], $tableInfo['columns'])) {
				throw new ConfigurationException(sprintf('Column with latitudes %s does not exist in the table', $t['latitudeCol']));
			}
			if (!in_array($t['longitudeCol'], $tableInfo['columns'])) {
				throw new ConfigurationException(sprintf('Column with latitudes %s does not exist in the table', $t['longitudeCol']));
			}

			$result['tables'][] = array(
				'tableId' => $t['tableId'],
				'latitudeCol' => $t['latitudeCol'],
				'longitudeCol' => $t['longitudeCol']
			);
		}
		return $result;
	}
} 