<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Symfony\Component\Process\Process;
use Keboola\Syrup\Exception\SyrupComponentException;
use Keboola\Syrup\Exception\UserException;
use Keboola\Temp\Temp;

class UserStorage
{
    /**
     * @var \Keboola\StorageApi\Client
     */
    protected $storageApiClient;
    /**
     * @var \Keboola\Temp\Temp
     */
    protected $temp;

    protected $files;

    const BUCKET_NAME = 'ag-forecastio';
    const BUCKET_ID = 'in.c-ag-forecastio';

    public $tables = array(
        'columns' => array('primary', 'latitude', 'longitude', 'date', 'key', 'value'),
        'primaryKey' => 'primary'
    );


    public function __construct(Client $storageApi, Temp $temp)
    {
        $this->storageApiClient = $storageApi;
        $this->temp = $temp;
        $this->files = [];
    }

    public function save($configId, $data)
    {
        if (!isset($this->files[$configId])) {
            $this->files[$configId] = new CsvFile($this->temp->createTmpFile());
            $this->files[$configId]->writeRow($this->tables['columns']);
        }
        $this->files[$configId]->writeRow($data);
    }

    public function uploadData()
    {
        if (!$this->storageApiClient->bucketExists(self::BUCKET_ID)) {
            $this->storageApiClient->createBucket(self::BUCKET_NAME, 'in', 'Forecast.io Data Storage');
        }

        foreach ($this->files as $configId => $file) {
            $tableId = self::BUCKET_ID . "." . $configId;
            try {
                $options = array('incremental' => 1);
                if (!empty($this->tables['primaryKey'])) {
                    $options['primaryKey'] = $this->tables['primaryKey'];
                }
                if (!$this->storageApiClient->tableExists($tableId)) {
                    $this->storageApiClient->createTableAsync(self::BUCKET_ID, $configId, $file, $options);
                } else {
                    $this->storageApiClient->writeTableAsync($tableId, $file, $options);
                }
            } catch (\Keboola\StorageApi\ClientException $e) {
                throw new UserException($e->getMessage(), $e);
            }
        }
    }

    public function getData($tableId, $columns)
    {
        // Get from SAPI
        $downloadedFile = $this->temp->createTmpFile();
        $params = array(
            'format' => 'rfc',
            'columns' => is_array($columns)? $columns : array($columns)
        );
        try {
            $exporter = new TableExporter($this->storageApiClient);
            $exporter->exportTable($tableId, $downloadedFile->getRealPath(), $params);
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                throw new UserException($e->getMessage(), $e);
            } else {
                throw $e;
            }
        }

        if (!file_exists($downloadedFile->getRealPath())) {
            $e = new SyrupComponentException(500, 'Download from SAPI failed');
            $e->setData(array(
                'tableId' => $tableId,
                'columns' => $columns
            ));
            throw $e;
        }

        // As a quickfix, we download rfc csv from SAPI and convert it to escaped csv
        $escapedFile = $this->temp->createTmpFile();
        $escapedCsv = fopen($escapedFile->getRealPath(), "w");
        $csv = new CsvFile($downloadedFile->getRealPath());
        foreach ($csv as $row) {
            fputcsv($escapedCsv, $row);
        }
        fclose($escapedCsv);

        // Deduplicate data
        $processedFile = $this->temp->createTmpFile();
        $process = new Process(sprintf('sed -e "1d" %s | sort | uniq > %s', $escapedFile->getRealPath(), $processedFile->getRealPath()));
        $process->setTimeout(null);
        $process->run();
        $error = $process->getErrorOutput();
        $output = $process->getOutput();

        if ($process->isSuccessful() && !$error && file_exists($processedFile->getRealPath())) {
            return $processedFile;
        } else {
            $e = new SyrupComponentException(500, 'Deduplication failed');
            $e->setData(array(
                'tableId' => $tableId,
                'columns' => $columns,
                'error' => $error,
                'output' => $output
            ));
            throw $e;
        }
    }
}
