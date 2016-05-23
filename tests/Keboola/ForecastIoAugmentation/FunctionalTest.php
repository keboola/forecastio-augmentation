<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Doctrine\DBAL\Connection;
use Keboola\Csv\CsvFile;
use Keboola\ForecastIoAugmentation\Augmentation;
use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class FunctionalTest extends \PHPUnit_Framework_TestCase
{

    public function testFunctional()
    {
        $outputTable = 't' . uniqid();
        $dbParams = [
            'driver' => 'pdo_mysql',
            'host' => DB_HOST,
            'dbname' => DB_NAME,
            'user' => DB_USER,
            'password' => DB_PASSWORD,
        ];

        $temp = new Temp();
        $temp->initRunFolder();

        $db = \Doctrine\DBAL\DriverManager::getConnection($dbParams);
        $stmt = $db->prepare(file_get_contents(__DIR__ . '/../../../db.sql'));
        $stmt->execute();
        $stmt->closeCursor();

        file_put_contents($temp->getTmpFolder() . '/config.yml', Yaml::dump([
            'image_parameters' => [
                '#api_token' => FORECASTIO_KEY,
                'database' => [
                    'driver' => 'pdo_mysql',
                    '#host' => DB_HOST,
                    '#name' => DB_NAME,
                    '#user' => DB_USER,
                    '#password' => DB_PASSWORD
                ],
            ],
            'parameters' => [
                'outputTable' => $outputTable,
                'inputTables' => [
                    [
                        'tableId' => 'out.c-main.coordinates',
                        'latitude' => 'lat',
                        'longitude' => 'lon'
                    ]
                ]
            ]
        ]));

        mkdir($temp->getTmpFolder().'/in');
        mkdir($temp->getTmpFolder().'/in/tables');
        copy(__DIR__ . '/data.csv', $temp->getTmpFolder().'/in/tables/out.c-main.coordinates.csv');
        copy(__DIR__ . '/data.csv.manifest', $temp->getTmpFolder().'/in/tables/out.c-main.coordinates.csv.manifest');

        $process = new Process("php ".__DIR__."/../../../src/run.php --data=".$temp->getTmpFolder());
        $process->setTimeout(null);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getOutput().PHP_EOL.$process->getErrorOutput());
        }

        $this->assertFileExists("{$temp->getTmpFolder()}/out/tables/$outputTable.csv");
    }
}
