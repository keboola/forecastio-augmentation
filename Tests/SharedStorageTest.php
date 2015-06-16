<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\ForecastIoAugmentation\Service\SharedStorage;

class SharedStorageTest extends AbstractTest
{

    public function testSharedStorage()
    {
        $db = \Doctrine\DBAL\DriverManager::getConnection(array(
            'driver' => 'pdo_mysql',
            'host' => DB_HOST,
            'dbname' => DB_NAME,
            'user' => DB_USER,
            'password' => DB_PASSWORD,
            'port' => DB_PORT
        ));
        $stmt = $db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
        $stmt->execute();
        $stmt->closeCursor();

        $sharedStorage = new SharedStorage($db);

        $data = [
            [
                'lat' => 56.03001,
                'lon' => 49.38332,
                'time' => '2015-05-13 14:32:33',
                'conditions' => [
                    'temperature' => 14.5,
                    'humidity' => 34
                ]
            ],
            [
                'lat' => 56.13854,
                'lon' => 48.27278,
                'time' => '2015-06-10 23:05:12',
                'conditions' => [
                    'temperature' => 27.6,
                    'humidity' => 88
                ]
            ]
        ];

        foreach ($data as $d) {
            foreach ($d['conditions'] as $k => $v) {
                $sharedStorage->save($d['lat'], $d['lon'], $d['time'], $k, $v);
            }
        }

        // Get all conditions
        $result = $sharedStorage->get([[$data[1]['lat'], $data[1]['lon'], $data[1]['time']]]);
        $this->assertCount(1, $result);
        $cacheKey = SharedStorage::getCacheKey($data[1]['lat'], $data[1]['lon'], $data[1]['time']);
        $this->assertArrayHasKey($cacheKey, $result);
        $this->assertCount(2, $result[$cacheKey]);

        // Get one condition
        $result = $sharedStorage->get([[$data[1]['lat'], $data[1]['lon'], $data[1]['time']]], ['temperature']);
        $this->assertCount(1, $result);
        $cacheKey = SharedStorage::getCacheKey($data[1]['lat'], $data[1]['lon'], $data[1]['time']);
        $this->assertArrayHasKey($cacheKey, $result);
        $this->assertCount(1, $result[$cacheKey]);
    }
}
