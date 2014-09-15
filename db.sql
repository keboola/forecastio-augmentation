CREATE TABLE `locations` (
  `name` varchar(255) NOT NULL DEFAULT '',
  `latitude` float NOT NULL,
  `longitude` float NOT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `conditions` (
  `location` varchar(255) NOT NULL DEFAULT '',
  `date` datetime NOT NULL,
  `key` varchar(100) NOT NULL DEFAULT '',
  `value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`location`,`date`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;