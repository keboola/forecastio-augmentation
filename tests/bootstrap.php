<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

defined('FORECASTIO_KEY') || define('FORECASTIO_KEY', getenv('FORECASTIO_KEY') ? getenv('FORECASTIO_KEY') : 'forecastio_api_key');

require_once __DIR__ . '/../vendor/autoload.php';
