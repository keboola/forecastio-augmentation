DROP TABLE IF EXISTS `forecastio_cache`;
CREATE TABLE `forecastio_cache` (
  `location` varchar(255) NOT NULL DEFAULT '',
  `key` varchar(100) NOT NULL DEFAULT '',
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`location`,`date`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
