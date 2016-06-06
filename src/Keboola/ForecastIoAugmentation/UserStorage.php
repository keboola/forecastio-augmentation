<?php
/**
 * @package forecastio-augmentation
 * @copyright Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\ForecastIoAugmentation;

use Keboola\Csv\CsvFile;
use Symfony\Component\Yaml\Yaml;

class UserStorage
{
    protected static $columns = ['primary', 'latitude', 'longitude', 'date', 'key', 'value'];
    protected static $primaryKey = ['primary'];

    protected $outputFile;
    protected $destination;
    protected $file;

    public function __construct($outputFile, $destination)
    {
        $this->outputFile = $outputFile;
        $this->destination = $destination;
    }

    public function save($data)
    {
        if (!$this->file) {
            $this->file = new CsvFile($this->outputFile);
            $this->file->writeRow(self::$columns);

            file_put_contents("$this->outputFile.manifest", Yaml::dump([
                'destination' => $this->destination,
                'incremental' => true,
                'primary_key' => self::$primaryKey
            ]));
        }

        if (!is_array($data)) {
            $data = (array)$data;
        }
        $dataToSave = [];
        foreach (self::$columns as $c) {
            $dataToSave[$c] = isset($data[$c]) ? $data[$c] : null;
        }

        $this->file->writeRow($dataToSave);
    }
}
