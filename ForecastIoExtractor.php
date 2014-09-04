<?php
/**
 * Created by IntelliJ IDEA.
 * User: JakubM
 * Date: 26.08.14
 * Time: 15:00
 */

namespace Keboola\ForecastIoExtractorBundle;

use Keboola\ExtractorBundle\Extractor\Extractors\JsonExtractor;

class ForecastIoExtractor extends JsonExtractor
{
	protected $name = 'forecastio';

	protected function run($config)
	{
		$forecast = new \Keboola\ForecastIoExtractorBundle\ForecastTools\Forecast('695f92cee04b6a962501f7f4db1d89e2');
		$response = $forecast->getData(37.770452, -122.424923);echo 'x' . PHP_EOL.PHP_EOL;print_r($response);die();
		$curr = $response->getCurrently();
		echo $curr->getTime() . PHP_EOL.PHP_EOL;
		echo $curr->getTemperature() . PHP_EOL.PHP_EOL;
		die();
	}
} 