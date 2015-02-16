<?php
/**
 * Tests configuration
 */

// Url to Storage API
if (!defined('STORAGE_API_URL')) {
    define('STORAGE_API_URL', 'https://connection.keboola.com');
}

// Storage API token
if (!defined('STORAGE_API_TOKEN')) {
    define('STORAGE_API_TOKEN', '');
}

// Forecast.io API key
if (!defined('FORECASTIO_KEY')) {
    define('FORECASTIO_KEY', '');
}

// DB host
if (!defined('DB_HOST')) {
    define('DB_HOST', '127.0.0.1');
}

// DB name
if (!defined('DB_NAME')) {
    define('DB_NAME', 'ag_geocoding_test');
}

// DB user
if (!defined('DB_USER')) {
    define('DB_USER', '');
}

// DB password
if (!defined('DB_PASSWORD')) {
    define('DB_PASSWORD', '');
}
