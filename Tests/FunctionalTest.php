<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\ForecastIoAugmentation\JobExecutor;
use Keboola\ForecastIoAugmentation\Service\ConfigurationStorage;
use Keboola\ForecastIoAugmentation\Service\SharedStorage;
use Keboola\ForecastIoAugmentation\Service\UserStorage;
use Keboola\StorageApi\Table;
use Keboola\StorageApi\Client as StorageApiClient;
use Monolog\Handler\NullHandler;
use Keboola\Syrup\Job\Metadata\Job;

class FunctionalTest extends AbstractTest
{
    /**
     * @var JobExecutor
     */
    private $jobExecutor;
    private $configId;

    public function setUp()
    {
        parent::setUp();

        $this->configId = 'test';

        // Cleanup
        $configTableId = sprintf('%s.%s', ConfigurationStorage::BUCKET_ID, $this->configId);
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
        $stmt->closeCursor();

        $sharedStorage = new SharedStorage($db);

        $logger = new \Monolog\Logger('null');
        $logger->pushHandler(new NullHandler());

        $temp = new \Keboola\Temp\Temp(self::APP_NAME);

        $this->jobExecutor = new JobExecutor($sharedStorage, $temp, $logger, FORECASTIO_KEY);
        $this->jobExecutor->setStorageApi($this->storageApiClient);

        list($bucketStage, $bucketName) = explode('.', ConfigurationStorage::BUCKET_ID);
        if (!$this->storageApiClient->bucketExists(ConfigurationStorage::BUCKET_ID)) {
            $this->storageApiClient->createBucket(substr($bucketName, 2), $bucketStage, 'Forecast.io config');
        }

        $t = new Table($this->storageApiClient, $configTableId);
        $t->setHeader(array('tableId', 'latitudeCol', 'longitudeCol'));
        $t->setAttribute('conditions', 'pressure,humidity,temperature,cloudCover');
        $t->setAttribute('units', 'us');
        $t->setFromArray(array(
            array($this->dataTableId, 'lat', 'lon')
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
            'params' => array(
                'config' => $this->configId
            )
        )));

        $dataTableId = sprintf('%s.%s', UserStorage::BUCKET_ID, $this->configId);
        $this->assertTrue($this->storageApiClient->tableExists($dataTableId));
        $export = $this->storageApiClient->exportTable($dataTableId);
        $csv = StorageApiClient::parseCsv($export, true);
        $this->assertGreaterThan(4, count($csv));


        // Once again for cached results
        $this->jobExecutor->execute(new Job(array(
            'id' => uniqid(),
            'runId' => uniqid(),
            'token' => $this->storageApiClient->getLogData(),
            'component' => self::APP_NAME,
            'command' => 'run',
            'params' => array(
                'config' => $this->configId
            )
        )));

        $dataTableId = sprintf('%s.%s', UserStorage::BUCKET_ID, $this->configId);
        $this->assertTrue($this->storageApiClient->tableExists($dataTableId));
        $export = $this->storageApiClient->exportTable($dataTableId);
        $csv = StorageApiClient::parseCsv($export, true);
        $this->assertGreaterThan(4, count($csv));
    }
}
