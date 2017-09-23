<?php
/**
 * @package forecastio-augmentation
 * @copyright Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\ForecastIoAugmentation;

use Keboola\Csv\CsvFile;

class UserStorage
{
    protected static $columns = ['primary', 'latitude', 'longitude', 'date', 'key', 'value'];
    protected static $primaryKey = ['primary'];

    protected $outputFile;
    protected $file;

    public function __construct($outputFile)
    {
        $this->outputFile = $outputFile;
    }

    public function save($data)
    {
        if (!$this->file) {
            $this->file = new CsvFile($this->outputFile);
            $this->file->writeRow(self::$columns);

            file_put_contents("$this->outputFile.manifest", json_encode([
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
