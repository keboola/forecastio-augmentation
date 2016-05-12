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

    const APP_NAME = 'ag-forecastio';

    /**
     * @var StorageApiClient
     */
    protected $storageApiClient;

    protected $inBucket;
    protected $outBucket;
    protected $dataTableId;

    public function setUp()
    {
        $this->storageApiClient = new StorageApiClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL
        ]);

        $this->inBucket = sprintf('in.c-%s', self::APP_NAME);
        $this->outBucket = sprintf('out.c-%s', self::APP_NAME);
        $this->dataTableId = sprintf('%s.%s', $this->outBucket, uniqid());

        // Cleanup
        if ($this->storageApiClient->bucketExists($this->inBucket)) {
            foreach ($this->storageApiClient->listTables($this->inBucket) as $table) {
                $this->storageApiClient->dropTable($table['id']);
            }
            $this->storageApiClient->dropBucket($this->inBucket);
        }
        if ($this->storageApiClient->bucketExists($this->outBucket)) {
            foreach ($this->storageApiClient->listTables($this->outBucket) as $table) {
                $this->storageApiClient->dropTable($table['id']);
            }
            $this->storageApiClient->dropBucket($this->outBucket);
        }

        if (!$this->storageApiClient->bucketExists($this->outBucket)) {
            $this->storageApiClient->createBucket(self::APP_NAME, 'out', 'Test');
        }

        // Prepare data table
        $t = new Table($this->storageApiClient, $this->dataTableId);
        $t->setHeader(['lat', 'lon', 'time']);
        $t->setFromArray([
            ['35.235', '57.453', '2015-06-01 14:00:00'],
            ['35.235', '57.453', '2015-06-01 14:01:00'],
            ['35.235', '57.454', '2015-06-01 15:40:00'],
            ['35.235', '57.553', '2015-06-02 10:00:00'],
            ['36.234', '56.443', '2015-06-01 16:00:00'],
            ['35.333', '57.333', '2015-06-03 04:00:00'],
        ]);
        $t->save();
    }
}
