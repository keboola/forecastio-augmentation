<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\Csv\CsvFile;
use Keboola\ForecastIoAugmentation\Service\UserStorage;

class UserStorageTest extends AbstractTest
{

    public function testDownload()
    {
        $temp = new \Keboola\Temp\Temp(self::APP_NAME);
        $userStorage = new UserStorage($this->storageApiClient, $temp);

        $csv = new CsvFile($userStorage->getData($this->dataTableId, ['lat', 'lon']));

        $data = [];
        foreach ($csv as $r) {
            $data[] = [$r[0], $r[1]];
        }
        $this->assertEquals([
            ["35.235","57.453"],
            ["35.235","57.454"],
            ["35.235","57.553"],
            ["35.333","57.333"],
            ["36.234","56.443"]
        ], $data);
    }
}
