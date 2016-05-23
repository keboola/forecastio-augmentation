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

class AugmentationTest extends \PHPUnit_Framework_TestCase
{
    /** @var  Temp */
    protected $temp;
    /** @var  Connection */
    protected $db;
    /** @var  Augmentation */
    protected $app;
    protected $outputFile;

    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        define('KBC_CONFIGID', uniqid());
    }

    public function setUp()
    {
        $outputTable = 't' . uniqid();
        $dbParams = [
            'driver' => 'pdo_mysql',
            'host' => DB_HOST,
            'dbname' => DB_NAME,
            'user' => DB_USER,
            'password' => DB_PASSWORD,
        ];

        $this->temp = new Temp();
        $this->temp->initRunFolder();

        $this->db = \Doctrine\DBAL\DriverManager::getConnection($dbParams);
        $stmt = $this->db->prepare(file_get_contents(__DIR__ . '/../../../db.sql'));
        $stmt->execute();
        $stmt->closeCursor();

        $this->app = new \Keboola\ForecastIoAugmentation\Augmentation(
            FORECASTIO_KEY,
            $dbParams,
            $this->temp->getTmpFolder(),
            $outputTable
        );

        $this->outputFile = "{$this->temp->getTmpFolder()}/$outputTable.csv";
        copy(__DIR__ . '/data.csv', $this->temp->getTmpFolder() . '/data1.csv');
    }

    public function testAugmentationForDefinedDates()
    {
        $this->app->process($this->temp->getTmpFolder() . '/data1.csv', 'lat', 'lon', 'time', ['temperature', 'windSpeed']);
        $this->assertFileExists($this->outputFile);
        $data = new CsvFile($this->outputFile);
        $this->assertCount(7, $data);
        $location1Count = 0;
        $location2Count = 0;
        foreach ($data as $row) {
            if ($row[1] == 49.191 && $row[2] == 16.611) {
                $location1Count++;
            }
            if ($row[1] == 50.071 && $row[2] == 14.423) {
                $location2Count++;
            }
        }
        $this->assertEquals(2, $location1Count);
        $this->assertEquals(4, $location2Count);
    }

    public function testAugmentationForToday()
    {
        $this->app->process($this->temp->getTmpFolder() . '/data1.csv', 'lat', 'lon', null, ['temperature', 'windSpeed']);
        $this->assertFileExists($this->outputFile);
        $data = new CsvFile($this->outputFile);
        $this->assertCount(5, $data);
        $location1Count = 0;
        $location2Count = 0;
        foreach ($data as $row) {
            if ($row[1] == 49.191 && $row[2] == 16.611) {
                $location1Count++;
            }
            if ($row[1] == 50.071 && $row[2] == 14.423) {
                $location2Count++;
            }
        }
        $this->assertEquals(2, $location1Count);
        $this->assertEquals(2, $location2Count);
    }

    public function testAugmentationWithPrefilledCache()
    {
        // Run and prefill cache
        $temperature = rand(125, 268);
        $this->db->insert('forecastio_cache', [
            'location' => '49.2:16.6:20160502:07',
            '`key`' => 'temperature',
            '`value`' => $temperature
        ]);
        $windSpeed = rand(500, 1000);
        $this->db->insert('forecastio_cache', [
            'location' => '49.2:16.6:20160502:07',
            '`key`' => 'windSpeed',
            '`value`' => $windSpeed
        ]);

        $this->app->process($this->temp->getTmpFolder() . '/data1.csv', 'lat', 'lon', 'time', ['temperature', 'windSpeed']);
        $this->assertFileExists($this->outputFile);
        $data = new CsvFile($this->outputFile);
        $this->assertCount(7, $data);
        $location1Count = 0;
        $location2Count = 0;
        foreach ($data as $row) {
            if ($row[1] == 49.191 && $row[2] == 16.611) {
                $location1Count++;
                if ($row[4] == 'temperature') {
                    $this->assertEquals($temperature, $row[5]);
                }
                if ($row[4] == 'windSpeed') {
                    $this->assertEquals($windSpeed, $row[5]);
                }
            }
            if ($row[1] == 50.071 && $row[2] == 14.423) {
                $location2Count++;
            }
        }
        $this->assertEquals(2, $location1Count);
        $this->assertEquals(4, $location2Count);
    }
}
