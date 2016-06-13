<?php
/**
 * @package forecastio-augmentation
 * @copyright Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

use Symfony\Component\Yaml\Yaml;

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
$config = Yaml::parse(file_get_contents("{$arguments['data']}/config.yml"));

if (!file_exists("{$arguments['data']}/out")) {
    mkdir("{$arguments['data']}/out");
}
if (!file_exists("{$arguments['data']}/out/tables")) {
    mkdir("{$arguments['data']}/out/tables");
}

try {
    \Keboola\ForecastIoAugmentation\ParametersValidation::validate($config);

    $app = new \Keboola\ForecastIoAugmentation\Augmentation(
        $config['image_parameters']['#api_token'],
        "{$arguments['data']}/out/tables/{$config['storage']['output']['tables'][0]['source']}",
        $config['storage']['output']['tables'][0]['destination']
    );

    foreach ($config['storage']['input']['tables'] as $table) {
        if (!file_exists("{$arguments['data']}/in/tables/{$table['destination']}")) {
            throw new Exception("File '{$table['destination']}' was not injected to the app");
        }

        $app->process(
            "{$arguments['data']}/in/tables/{$table['destination']}",
            isset($config['parameters']['conditions']) ? $config['parameters']['conditions'] : [],
            isset($config['parameters']['units']) ? $config['parameters']['units'] : null
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
