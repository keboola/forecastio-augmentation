<?php
/**
 * @package forecastio-augmentation
 * @copyright 2014 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\ForecastIoAugmentation\Service;

use Keboola\Syrup\Exception\SyrupComponentException;

class ConfigurationException extends SyrupComponentException
{
    public function __construct($message, $previous = null)
    {
        parent::__construct(400, $message, $previous);
    }
}
