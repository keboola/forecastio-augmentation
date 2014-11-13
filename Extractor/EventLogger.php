<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 26.06.14
 * Time: 10:01
 */

namespace Keboola\ForecastIoExtractorBundle\Extractor;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent;


class EventLogger
{
	private $appConfiguration;
	private $storageApiClient;
	private $jobId;

	const TYPE_INFO = StorageApiEvent::TYPE_INFO;
	const TYPE_ERROR = StorageApiEvent::TYPE_ERROR;
	const TYPE_SUCCESS = StorageApiEvent::TYPE_SUCCESS;
	const TYPE_WARN = StorageApiEvent::TYPE_WARN;

	public function __construct(AppConfiguration $appConfiguration, StorageApiClient $storageApiClient, $jobId)
	{
		$this->appConfiguration = $appConfiguration;
		$this->storageApiClient = $storageApiClient;
		$this->jobId = $jobId;
	}

	public function log($message, $params=array(), $duration=null, $type=self::TYPE_INFO)
	{
		$event = new StorageApiEvent();
		$event
			->setType($type)
			->setMessage($message)
			->setComponent('ex-forecastio') //@TODO load from config
			->setConfigurationId($this->jobId)
			->setRunId($this->storageApiClient->getRunId());
		if (count($params)) {
			$event->setParams($params);
		}
		if ($duration)
			$event->setDuration($duration);
		$this->storageApiClient->createEvent($event);
	}

}