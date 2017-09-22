<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Tests;

use Keboola\Temp\Temp;
use Symfony\Component\Process\Process;

class FunctionalTest extends \PHPUnit_Framework_TestCase
{

    public function testFunctional()
    {
        $temp = new Temp();
        $temp->initRunFolder();

        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode([
            'image_parameters' => [
                '#api_token' => FORECASTIO_KEY
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.coordinates',
                            'destination' => 'coordinates.csv'
                        ]
                    ]
                ],
                'output' => [
                    'tables' => [
                        [
                            'source' => 'conditions.csv',
                            'destination' => 'in.c-main.conditions'
                        ]
                    ]
                ]
            ],
            'parameters' => [
                'conditions' => ['windSpeed']
            ]
        ]));

        mkdir($temp->getTmpFolder().'/in');
        mkdir($temp->getTmpFolder().'/in/tables');
        copy(__DIR__ . '/data.csv', $temp->getTmpFolder().'/in/tables/coordinates.csv');
        copy(__DIR__ . '/data.csv.manifest', $temp->getTmpFolder().'/in/tables/coordinates.csv.manifest');

        $process = new Process("php ".__DIR__."/../../../src/run.php --data=".$temp->getTmpFolder());
        $process->setTimeout(null);
        $process->run();
        if (!$process->isSuccessful()) {
            $this->fail($process->getOutput().PHP_EOL.$process->getErrorOutput());
        }

        $this->assertFileExists("{$temp->getTmpFolder()}/out/tables/conditions.csv");
    }
}
