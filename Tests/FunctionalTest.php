<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Doctrine\DBAL\Connection;
use Keboola\ForecastIoAugmentation\JobExecutor;
use Keboola\ForecastIoAugmentation\Service\ConfigurationStorage;
use Keboola\ForecastIoAugmentation\Service\CacheStorage;
use Keboola\ForecastIoAugmentation\Service\UserStorage;
use Keboola\StorageApi\Table;
use Keboola\StorageApi\Client as StorageApiClient;
use Monolog\Handler\NullHandler;
use Keboola\Syrup\Job\Metadata\Job;

class FunctionalTest extends AbstractTest
{
    /** @var JobExecutor */
    private $jobExecutor;
    /** @var  Connection */
    private $db;

    private $configId;
    private $historyConfigId;

    public function setUp()
    {
        parent::setUp();

        $this->configId = 'test';
        $this->historyConfigId = 'test_history';

        // Cleanup
        $configTableId = sprintf('%s.%s', ConfigurationStorage::BUCKET_ID, $this->configId);
        if ($this->storageApiClient->tableExists($configTableId)) {
            $this->storageApiClient->dropTable($configTableId);
        }
        $historyConfigTableId = sprintf('%s.%s', ConfigurationStorage::BUCKET_ID, $this->historyConfigId);
        if ($this->storageApiClient->tableExists($historyConfigTableId)) {
            $this->storageApiClient->dropTable($historyConfigTableId);
        }

        $this->db = \Doctrine\DBAL\DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => DB_HOST,
            'dbname' => DB_NAME,
            'user' => DB_USER,
            'password' => DB_PASSWORD,
        ]);

        $stmt = $this->db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
        $stmt->execute();
        $stmt->closeCursor();

        $cacheStorage = new CacheStorage($this->db);

        $logger = new \Monolog\Logger('null');
        $logger->pushHandler(new NullHandler());

        $temp = new \Keboola\Temp\Temp(self::APP_NAME);

        $this->jobExecutor = new JobExecutor($cacheStorage, $temp, $logger, FORECASTIO_KEY);
        $this->jobExecutor->setStorageApi($this->storageApiClient);

        list($bucketStage, $bucketName) = explode('.', ConfigurationStorage::BUCKET_ID);
        if (!$this->storageApiClient->bucketExists(ConfigurationStorage::BUCKET_ID)) {
            $this->storageApiClient->createBucket(substr($bucketName, 2), $bucketStage, 'Forecast.io config');
        }

        $t1 = new Table($this->storageApiClient, $configTableId);
        $t1->setHeader(['tableId', 'latitudeCol', 'longitudeCol']);
        $t1->setAttribute('conditions', 'pressure,humidity,temperature,icon');
        $t1->setAttribute('units', 'us');
        $t1->setFromArray([
            [$this->dataTableId, 'lat', 'lon']
        ]);
        $t1->save();

        $t2 = new Table($this->storageApiClient, $historyConfigTableId);
        $t2->setHeader(['tableId', 'latitudeCol', 'longitudeCol', 'timeCol', 'conditions', 'units']);
        $t2->setFromArray([
            [$this->dataTableId, 'lat', 'lon', 'time', 'temperature,summary', 'si']
        ]);
        $t2->save();
    }


    public function testAugmentation()
    {
        $this->jobExecutor->execute(new Job([
            'id' => uniqid(),
            'runId' => uniqid(),
            'token' => $this->storageApiClient->getLogData(),
            'component' => self::APP_NAME,
            'command' => 'run',
            'params' => [
                'config' => $this->configId
            ]
        ]));
        $dataTableId = sprintf('%s.%s', UserStorage::BUCKET_ID, $this->configId);
        $this->assertTrue($this->storageApiClient->tableExists($dataTableId));
        $export = $this->storageApiClient->exportTable($dataTableId);
        $csv = StorageApiClient::parseCsv($export, true);
        $this->assertCount(20, $csv, 'There should be 20 results (5 unique coords getting 4 conditions');


        // Once again for cached results
        $this->jobExecutor->execute(new Job([
            'id' => uniqid(),
            'runId' => uniqid(),
            'token' => $this->storageApiClient->getLogData(),
            'component' => self::APP_NAME,
            'command' => 'run',
            'params' => [
                'config' => $this->configId
            ]
        ]));
        $dataTableId = sprintf('%s.%s', UserStorage::BUCKET_ID, $this->configId);
        $this->assertTrue($this->storageApiClient->tableExists($dataTableId));
        $export = $this->storageApiClient->exportTable($dataTableId);
        $csv = StorageApiClient::parseCsv($export, true);
        $this->assertCount(20, $csv);

        // Get historical results
        $this->jobExecutor->execute(new Job([
            'id' => uniqid(),
            'runId' => uniqid(),
            'token' => $this->storageApiClient->getLogData(),
            'component' => self::APP_NAME,
            'command' => 'run',
            'params' => [
                'config' => $this->historyConfigId
            ]
        ]));
        $dataTableId = sprintf('%s.%s', UserStorage::BUCKET_ID, $this->historyConfigId);
        $this->assertTrue($this->storageApiClient->tableExists($dataTableId));
        $export = $this->storageApiClient->exportTable($dataTableId);
        $csv = StorageApiClient::parseCsv($export, true);
        $this->assertCount(12, $csv, 'There should be 12 results (6 unique coords getting 2 conditions');
    }
}
