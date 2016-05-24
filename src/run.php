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

try {
    \Keboola\ForecastIoAugmentation\ParametersValidation::validate($config);

    if (!file_exists("{$arguments['data']}/out")) {
        mkdir("{$arguments['data']}/out");
    }
    if (!file_exists("{$arguments['data']}/out/tables")) {
        mkdir("{$arguments['data']}/out/tables");
    }
    
    $app = new \Keboola\ForecastIoAugmentation\Augmentation(
        $config['image_parameters']['#api_token'],
        [
            'host' => $config['image_parameters']['database']['#host'],
            'dbname' => $config['image_parameters']['database']['#name'],
            'user' => $config['image_parameters']['database']['#user'],
            'password' => $config['image_parameters']['database']['#password'],
        ],
        "{$arguments['data']}/out/tables/{$config['parameters']['outputTable']}",
        $config['storage']['output']['tables'][0]['destination']
    );

    foreach ($config['parameters']['inputTables'] as $row => $table) {
        \Keboola\ForecastIoAugmentation\ParametersValidation::validateTable($table);

        if (!file_exists("{$arguments['data']}/in/tables/{$table['filename']}")) {
            print("File '{$table['tableId']}' was not injected to the app");
            exit(1);
        }
        $manifest = $config = Yaml::parse(file_get_contents("{$arguments['data']}/in/tables/{$table['filename']}.manifest"), true);

        \Keboola\ForecastIoAugmentation\ParametersValidation::validateTableManifest($table, $manifest);

        $app->process(
            "{$arguments['data']}/in/tables/{$table['filename']}",
            $table['latitude'],
            $table['longitude'],
            isset($table['time']) ? $table['time'] : null,
            isset($config['parameters']['conditions']) ? $config['parameters']['conditions'] : [],
            isset($config['parameters']['units']) ? $config['parameters']['units'] : null
        );
    }

    exit(0);
} catch (\Keboola\ForecastIoAugmentation\Exception $e) {
    print $e->getMessage();
    exit(1);
}
