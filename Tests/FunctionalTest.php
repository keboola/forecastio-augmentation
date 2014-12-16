<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\ForecastIoAugmentation\JobExecutor;
use Keboola\ForecastIoAugmentation\Service\Configuration;
use Keboola\ForecastIoAugmentation\Service\SharedStorage;
use Keboola\ForecastIoAugmentation\Service\UserStorage;
use Keboola\StorageApi\Table;
use Keboola\StorageApi\Client as StorageApiClient;
use Monolog\Handler\NullHandler;
use Syrup\ComponentBundle\Job\Metadata\Job;

class FunctionalTest extends AbstractTest
{
	/**
	 * @var JobExecutor
	 */
	private $jobExecutor;

	public function setUp()
	{
		parent::setUp();

		// Cleanup
		$configTableId = sprintf('%s.%s', Configuration::BUCKET_ID, Configuration::TABLE_NAME);
		if ($this->storageApiClient->tableExists($configTableId)) {
			$this->storageApiClient->dropTable($configTableId);
		}

		$db = \Doctrine\DBAL\DriverManager::getConnection(array(
			'driver' => 'pdo_mysql',
			'host' => DB_HOST,
			'dbname' => DB_NAME,
			'user' => DB_USER,
			'password' => DB_PASSWORD,
		));

		$stmt = $db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
		$stmt->execute();

		$sharedStorage = new SharedStorage($db);

		$logger = new \Monolog\Logger('null');
		$logger->pushHandler(new NullHandler());

		$temp = new \Syrup\ComponentBundle\Filesystem\Temp(self::APP_NAME);

		$this->jobExecutor = new JobExecutor($sharedStorage, $temp, $logger, FORECASTIO_KEY);
		$this->jobExecutor->setStorageApi($this->storageApiClient);

		list($bucketStage, $bucketName) = explode('.', Configuration::BUCKET_ID);
		if (!$this->storageApiClient->bucketExists(Configuration::BUCKET_ID)) {
			$this->storageApiClient->createBucket(substr($bucketName, 2), $bucketStage, 'Forecast.io config');
		}

		$t = new Table($this->storageApiClient, $configTableId);
		$t->setHeader(array('tableId', 'latitudeCol', 'longitudeCol', 'conditions', 'units'));
		$t->setFromArray(array(
			array($this->dataTableId, 'lat', 'lon', 'pressure,humidity,temperature,cloudCover', 'si')
		));
		$t->save();
	}


	public function testAugmentation()
	{
		$this->jobExecutor->execute(new Job(array(
			'id' => uniqid(),
			'runId' => uniqid(),
			'token' => $this->storageApiClient->getLogData(),
			'component' => self::APP_NAME,
			'command' => 'run',
			'params' => array()
		)));

		$dataTableId = sprintf('%s.%s', UserStorage::BUCKET_ID, UserStorage::CONDITIONS_TABLE_NAME);
		$this->assertTrue($this->storageApiClient->tableExists($dataTableId));
		$export = $this->storageApiClient->exportTable($dataTableId);
		$csv = StorageApiClient::parseCsv($export, true);
		$this->assertGreaterThan(4, count($csv));
	}

}