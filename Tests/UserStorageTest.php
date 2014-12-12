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
		$temp = new \Syrup\ComponentBundle\Filesystem\Temp('ag-geocoding');
		$userStorage = new UserStorage($this->storageApiClient, $temp);

		$csv = new CsvFile($userStorage->getData($this->tableId, array('lat', 'lon')));

		$data = array();
		foreach ($csv as $r) {
			$data[] = array($r[0], $r[1]);
		}
		$this->assertEquals(array(
			array("35.235","57.453"),
			array("35.235","57.553"),
			array("35.333","57.333"),
			array("36.234","56.443")), $data);
	}

}