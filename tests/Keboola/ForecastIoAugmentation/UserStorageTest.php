<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\ForecastIoAugmentation\UserStorage;
use Symfony\Component\Yaml\Yaml;

class UserStorageTest extends \PHPUnit_Framework_TestCase
{

    public function testSave()
    {
        $temp = new \Keboola\Temp\Temp();
        $temp->initRunFolder();
        $table = 'in.c-ag-forecastio.forecastio';

        $userStorage = new UserStorage($temp->getTmpFolder()."/$table");
        $userStorage->save([
            'primary' => 'key',
            'latitude' => '10.5',
            'longitude' => '13.4',
            'date' => '2016-01-01',
            'key' => 'temperature',
            'value' => '-12.5'
        ]);

        $this->assertTrue(file_exists("{$temp->getTmpFolder()}/$table"));
        if (($handle = fopen("{$temp->getTmpFolder()}/$table", "r")) !== false) {
            $row1 = fgetcsv($handle, 1000, ",");
            $this->assertEquals(["primary","latitude","longitude","date","key","value"], $row1);
            $row2 = fgetcsv($handle, 1000, ",");
            $this->assertEquals(["key","10.5","13.4","2016-01-01","temperature","-12.5"], $row2);
            fclose($handle);
        } else {
            $this->fail();
        }

        $this->assertTrue(file_exists("{$temp->getTmpFolder()}/$table.manifest"));
        $manifest = Yaml::parse(file_get_contents("{$temp->getTmpFolder()}/$table.manifest"));
        $this->assertArrayHasKey('incremental', $manifest);
        $this->assertEquals(true, $manifest['incremental']);
        $this->assertArrayHasKey('primary_key', $manifest);
        $this->assertEquals(["primary"], $manifest['primary_key']);
    }
}
