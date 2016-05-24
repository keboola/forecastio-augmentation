<?php
/**
 * @package forecastio-augmentation
 * @copyright Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\ForecastIoAugmentation;

class ParametersValidation
{
    public static function validate($config)
    {
        if (!isset($config['image_parameters']['#api_token'])) {
            throw new Exception("Missing image parameter '#api_token'");
        }

        if (!isset($config['image_parameters']['database']['#host'])) {
            throw new Exception("Missing image parameter 'database.#host'");
        }

        if (!isset($config['image_parameters']['database']['#name'])) {
            throw new Exception("Missing image parameter 'database.#name'");
        }

        if (!isset($config['image_parameters']['database']['#user'])) {
            throw new Exception("Missing image parameter 'database.#user'");
        }

        if (!isset($config['image_parameters']['database']['#password'])) {
            throw new Exception("Missing image parameter 'database.#password'");
        }

        if (!isset($config['parameters']['inputTables'])) {
            throw new Exception("Missing parameter 'inputTables'");
        }
        if (!isset($config['parameters']['outputTable'])) {
            throw new Exception("Missing parameter outputTable");
        }

        if (!isset($config['storage']['output']['tables'][0]['destination'])) {
            throw new Exception("Destination table is not connected to output mapping");
        }

        if ($config['parameters']['outputTable'] != $config['storage']['output']['tables'][0]['source']) {
            throw new Exception("Parameter 'outputTable' with value '{$config['parameters']['outputTable']}' does not "
                . "correspond to table connected using output mapping: "
                . "'{$config['storage']['output']['tables'][0]['source']}' for table "
                . "({$config['storage']['output']['tables'][0]['destination']})");
        }
    }

    public static function validateTable($table)
    {
        if (!isset($table['filename'])) {
            throw new Exception("Missing 'filename' key of parameter 'inputTables' on some row");
        }
        if (!isset($table['latitude'])) {
            throw new Exception("Missing 'latitude' key of parameter 'inputTables' for table {$table['filename']}");
        }
        if (!isset($table['longitude'])) {
            throw new Exception("Missing 'longitude' key of parameter 'inputTables' for table {$table['filename']}");
        }
    }

    public static function validateTableManifest($table, $manifest)
    {
        if (!in_array($table['latitude'], $manifest['columns'])) {
            throw new Exception("Column with latitudes '{$table['latitude']}' is missing from table "
                . "'{$table['tableId']}'");
        }
        if (!in_array($table['longitude'], $manifest['columns'])) {
            throw new Exception("Column with longitudes '{$table['longitude']}' is missing from table "
                . "'{$table['tableId']}'");
        }
        if (!empty($table['time']) && !in_array($table['time'], $manifest['columns'])) {
            throw new Exception("Column with times '{$table['time']}' is missing from table '{$table['tableId']}'");
        }
        if (isset($config['parameters']['conditions']) && !is_array(isset($config['parameters']['conditions']))) {
            throw new Exception("Parameter 'conditions' must be array");
        }
    }
}
