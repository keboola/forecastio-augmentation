<?php
namespace Keboola\ForecastIoAugmentation;

use Keboola\ForecastIoAugmentation\DependencyInjection\Extension;
use Symfony\Component\HttpKernel\Bundle\Bundle;

class KeboolaForecastIoAugmentation extends Bundle
{

    public function getContainerExtension()
    {
        return new Extension();
    }
}
