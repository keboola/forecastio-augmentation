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
        $this->storageApiClient = new StorageApiClient(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL
        ));

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
    }
}
