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

if (!isset($config['image_parameters']['#api_token'])) {
    print("Missing image parameter '#api_token'");
    exit(1);
}

if (!isset($config['image_parameters']['database']['driver'])) {
    print("Missing image parameter 'database.driver'");
    exit(1);
}

if (!isset($config['image_parameters']['database']['#host'])) {
    print("Missing image parameter 'database.#host'");
    exit(1);
}

if (!isset($config['image_parameters']['database']['#name'])) {
    print("Missing image parameter 'database.#name'");
    exit(1);
}

if (!isset($config['image_parameters']['database']['#user'])) {
    print("Missing image parameter 'database.#user'");
    exit(1);
}

if (!isset($config['image_parameters']['database']['#password'])) {
    print("Missing image parameter 'database.#password'");
    exit(1);
}

if (!isset($config['parameters']['inputTables'])) {
    print("Missing parameter 'inputTables'");
    exit(1);
}
if (!isset($config['parameters']['outputTable'])) {
    print "Missing parameter outputTable";
    exit(1);
}

if (!file_exists("{$arguments['data']}/out")) {
    mkdir("{$arguments['data']}/out");
}
if (!file_exists("{$arguments['data']}/out/tables")) {
    mkdir("{$arguments['data']}/out/tables");
}

defined('KBC_CONFIGID') || define('KBC_CONFIGID', getenv('KBC_CONFIGID') ? getenv('KBC_CONFIGID') : 'ag_forecastio');

try {
    $app = new \Keboola\ForecastIoAugmentation\Augmentation(
        $config['image_parameters']['#api_token'],
        [
            'driver' => $config['image_parameters']['database']['driver'],
            'host' => $config['image_parameters']['database']['#host'],
            'dbname' => $config['image_parameters']['database']['#name'],
            'user' => $config['image_parameters']['database']['#user'],
            'password' => $config['image_parameters']['database']['#password'],
        ],
        "{$arguments['data']}/out/tables",
        $config['parameters']['outputTable']
    );

    foreach ($config['parameters']['inputTables'] as $row => $table) {
        if (!isset($table['tableId'])) {
            print("Missing 'tableId' key of parameter 'tables' on row $row");
            exit(1);
        }
        if (!isset($table['latitude'])) {
            print("Missing 'latitude' key of parameter 'tables' on row $row");
            exit(1);
        }
        if (!isset($table['longitude'])) {
            print("Missing 'longitude' key of parameter 'tables' on row $row");
            exit(1);
        }
        if (!file_exists("{$arguments['data']}/in/tables/{$table['tableId']}.csv")) {
            print("Table '{$table['tableId']}' was not injected to the app");
            exit(1);
        }
        $manifest = json_decode(file_get_contents("{$arguments['data']}/in/tables/{$table['tableId']}.csv.manifest"), true);

        if (!in_array($table['latitude'], $manifest['columns'])) {
            print("Column with latitudes '{$table['latitude']}' is missing from table '{$table['tableId']}'");
            exit(1);
        }
        if (!in_array($table['longitude'], $manifest['columns'])) {
            print("Column with longitudes '{$table['longitude']}' is missing from table '{$table['tableId']}'");
            exit(1);
        }
        if (!empty($table['time']) && !in_array($table['time'], $manifest['columns'])) {
            print("Column with times '{$table['time']}' is missing from table '{$table['tableId']}'");
            exit(1);
        }
        if (isset($config['parameters']['conditions']) && !is_array(isset($config['parameters']['conditions']))) {
            print("Parameter 'conditions' must be array");
            exit(1);
        }

        $app->process(
            "{$arguments['data']}/in/tables/{$table['tableId']}.csv",
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
