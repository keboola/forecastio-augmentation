<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Table;

abstract class AbstractTest extends \Symfony\Bundle\FrameworkBundle\Test\WebTestCase
{
	/**
	 * @var StorageApiClient
	 */
	protected $storageApiClient;

	protected $bucketName;
	protected $tableId;

	public function setUp()
	{
		$this->storageApiClient = new StorageApiClient(array(
			'token' => STORAGE_API_TOKEN,
			'url' => STORAGE_API_URL
		));

		$this->bucketName = 't' . uniqid();
		$this->storageApiClient->createBucket($this->bucketName, 'out', 'Test');
		$this->tableId = 'out.c-' . $this->bucketName . '.' . uniqid();

		// Prepare data table
		$t = new Table($this->storageApiClient, $this->tableId);
		$t->setHeader(array('lat', 'lon'));
		$t->setFromArray(array(
			array('35.235', '57.453'),
			array('36.234', '56.443'),
			array('35.235', '57.453'),
			array('35.235', '57.553'),
			array('35.333', '57.333'),
			array('35.235', '57.453')
		));
		$t->save();

		if ($this->storageApiClient->tableExists('in.c-ag-forecastio.conditions')) {
			$this->storageApiClient->dropTable('in.c-ag-forecastio.conditions');
		}
	}

	public function tearDown()
	{
		$this->storageApiClient->dropTable($this->tableId);
		$this->storageApiClient->dropBucket('out.c-' . $this->bucketName);
	}

}