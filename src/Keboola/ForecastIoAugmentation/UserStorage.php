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
    protected $bucket;

    protected $files = [];

    public function __construct($path, $bucket)
    {
        $this->path = $path;
        $this->bucket = $bucket;
    }

    public function save($table, $data)
    {
        if (!isset($this->files[$table])) {
            $file = new CsvFile("$this->path/$this->bucket.$table.csv");
            $file->writeRow(self::$columns);
            $this->files[$table] = $file;

            file_put_contents("$this->path/$this->bucket.$table.csv.manifest", Yaml::dump([
                'destination' => "$this->bucket.$table",
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

        /** @var CsvFile $file */
        $file = $this->files[$table];
        $file->writeRow($dataToSave);
    }

    public function createManifest($fileName, $table, array $primary = [])
    {
        if (!file_exists("$fileName.manifest")) {
            file_put_contents("$fileName.manifest", Yaml::dump([
                'destination' => "$this->bucket.$table",
                'incremental' => true,
                'primary_key' => $primary
            ]));
        }
    }
}
