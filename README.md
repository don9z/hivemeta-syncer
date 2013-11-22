
# Hive Metadata Syncer #

This tool is used to sync hive database/table information from its metastore to a mysql database.

Before use, you must create below tables on the mysql database.

``` mysql
    CREATE TABLE `hivemeta_database` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `db_name` varchar(128) NOT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8;

    CREATE TABLE `hivemeta_table` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `tbl_name` varchar(128) NOT NULL,
      `db_id` int(11) NOT NULL,
      PRIMARY KEY (`id`),
      KEY `hivemeta_table_b7cea04a` (`db_id`),
      CONSTRAINT `db_id_refs_id_7340b8e3` FOREIGN KEY (`db_id`) REFERENCES `hivemeta_database` (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=223 DEFAULT CHARSET=utf8;

    CREATE TABLE `hivemeta_column` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `col_name` varchar(128) NOT NULL,
      `col_type` varchar(4000) NOT NULL,
      `table_id` int(11) NOT NULL,
      PRIMARY KEY (`id`),
      KEY `hivemeta_column_80be7a7a` (`table_id`),
      CONSTRAINT `table_id_refs_id_0a39662a` FOREIGN KEY (`table_id`) REFERENCES `hivemeta_table` (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=601 DEFAULT CHARSET=utf8;
```
