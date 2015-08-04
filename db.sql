DROP TABLE IF EXISTS `forecastio_cache`;
CREATE TABLE `forecastio_cache` (
  `location` varchar(255) NOT NULL DEFAULT '',
  `key` varchar(100) NOT NULL DEFAULT '',
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`location`,`key`),
  KEY `idx_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `forecastio_calls_count`;
CREATE TABLE `forecastio_calls_count` (
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `project_id` int(10) unsigned NOT NULL DEFAULT '0',
  `project_name` varchar(128) DEFAULT NULL,
  `token_id` int(10) unsigned NOT NULL DEFAULT '0',
  `token_desc` varchar(128) DEFAULT NULL,
  `count` int(10) unsigned NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8