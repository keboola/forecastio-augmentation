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
    protected static $primaryKey = 'primary';

    protected $path;
    protected $outputTable;
    protected $file;

    public function __construct($path, $outputTable)
    {
        $this->path = $path;
        $this->outputTable = $outputTable;
    }

    public function save($data)
    {
        if (!file_exists("$this->path/$this->outputTable.csv")) {
            $this->file = new CsvFile("$this->path/$this->outputTable.csv");
            $this->file->writeRow(self::$columns);

            file_put_contents("$this->path/$this->outputTable.csv.manifest", Yaml::dump([
                'destination' => $this->outputTable,
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
