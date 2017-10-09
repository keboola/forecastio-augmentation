<?php
/**
 * @package forecastio-augmentation
 * @copyright Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

set_error_handler(
    function ($errno, $errstr, $errfile, $errline, array $errcontext) {
        if (0 === error_reporting()) {
            return false;
        }
        throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
    }
);

require_once(dirname(__FILE__) . "/../vendor/autoload.php");
$arguments = getopt("d::", array("data::"));
if (!isset($arguments['data'])) {
    print "Data folder not set.";
    exit(1);
}

$config = (new JsonDecode(true))->decode(
    file_get_contents("{$arguments['data']}/config.json"),
    JsonEncoder::FORMAT
);

if (!file_exists("{$arguments['data']}/out")) {
    mkdir("{$arguments['data']}/out");
}
if (!file_exists("{$arguments['data']}/out/tables")) {
    mkdir("{$arguments['data']}/out/tables");
}

try {
    \Keboola\ForecastIoAugmentation\ParametersValidation::validate($config);

    $app = new \Keboola\ForecastIoAugmentation\Augmentation(
        $config['parameters']['#apiToken'],
        "{$arguments['data']}/out/tables/forecast.csv"
    );

    foreach ($config['storage']['input']['tables'] as $table) {
        if (!file_exists("{$arguments['data']}/in/tables/{$table['destination']}")) {
            throw new Exception("File '{$table['destination']}' was not injected to the app");
        }

        $app->process(
            "{$arguments['data']}/in/tables/{$table['destination']}",
            isset($config['parameters']['conditions']) ? $config['parameters']['conditions'] : [],
            isset($config['parameters']['units']) ? $config['parameters']['units'] : null,
            isset($config['parameters']['granularity']) ? $config['parameters']['granularity'] : \Keboola\ForecastIoAugmentation\Augmentation::GRANULARITY_DAILY
        );
    }

    exit(0);
} catch (\Keboola\ForecastIoAugmentation\Exception $e) {
    print $e->getMessage();
    exit(1);
} catch (\Exception $e) {
    print $e->getMessage();
    print $e->getTraceAsString();
    exit(2);
}
