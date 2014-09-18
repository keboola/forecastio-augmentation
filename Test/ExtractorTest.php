<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\ForecastIoExtractorBundle\Tests;

use Symfony\Bundle\FrameworkBundle\Test\WebTestCase,
	Symfony\Bundle\FrameworkBundle\Console\Application,
	Symfony\Component\Console\Tester\CommandTester;

abstract class ExtractorTest extends WebTestCase
{
	protected $storageApiToken;
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $storageApi;
	/**
	 * @var \Symfony\Bundle\FrameworkBundle\Client
	 */
	protected $httpClient;
	/**
	 * @var CommandTester
	 */
	protected $commandTester;


	/**
	 * Setup called before every test
	 */
	protected function setUp()
	{
		$this->httpClient = static::createClient();
		$container = $this->httpClient->getContainer();

		if (!$this->storageApiToken)
			$this->storageApiToken = $container->getParameter('storage_api.test.token');
		$this->httpClient->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $this->storageApiToken
		));

		/*$this->storageApi = new StorageApiClient(array(
				'token' => $this->storageApiToken,
				'url' => $container->getParameter('storage_api.url'))
		);*/

		// Init job processing
		$application = new Application($this->httpClient->getKernel());
		//$application->add(new ExecuteBatchCommand());
		//$command = $application->find('gooddata-writer:execute-batch');
		//$this->commandTester = new CommandTester($command);
	}


	/**
	 * Request to Writer API
	 * @param $url
	 * @param string $method
	 * @param array $params
	 * @return mixed
	 */
	protected function callApi($url, $method = 'POST', $params = array())
	{
		$this->httpClient->request($method, '/ex-forecastio' . $url, array(), array(), array(), json_encode($params));
		$response = $this->httpClient->getResponse();
		/* @var \Symfony\Component\HttpFoundation\Response $response */

		$this->assertGreaterThanOrEqual(200, $response->getStatusCode(), sprintf("HTTP status of writer call '%s' should be greater than or equal to 200 but is %s", $url, $response->getStatusCode()));
		$this->assertLessThan(300, $response->getStatusCode(), sprintf("HTTP status of writer call '%s' should be less than 300 but is %s", $url, $response->getStatusCode()));
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson, sprintf("Response for writer call '%s' should not be empty.", $url));

		return $responseJson;
	}

}
