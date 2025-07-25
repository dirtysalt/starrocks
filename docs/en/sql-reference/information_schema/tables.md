---
displayed_sidebar: docs
---

# tables

`tables` provides information about tables.

The following fields are provided in `tables`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | Name of the catalog that stores the table.                   |
| TABLE_SCHEMA    | Name of the database that stores the table.                  |
| TABLE_NAME      | Name of the table.                                           |
| TABLE_TYPE      | Type of the table. Valid values: `TABLE`(`BASE TABLE`), `VIEW` and `SYSTEM VIEW` (supported only when `mysql_server_version` is `8.0.33`).     |
| ENGINE          | Engine type of the table. Valid values: `StarRocks`, `MySQL`, `MEMORY` or an empty string. |
| VERSION         | Applies to a feature not available in StarRocks.             |
| ROW_FORMAT      | Applies to a feature not available in StarRocks.             |
| TABLE_ROWS      | Row count of the table.                                      |
| AVG_ROW_LENGTH  | Average row length (size) of the table. It is equivalent to `DATA_LENGTH`/`TABLE_ROWS`. Unit: Byte. |
| DATA_LENGTH     | The data length of the table is determined by summing the data length of the table across all replicas. Unit: Byte.|
| MAX_DATA_LENGTH | Applies to a feature not available in StarRocks.             |
| INDEX_LENGTH    | Applies to a feature not available in StarRocks.             |
| DATA_FREE       | Applies to a feature not available in StarRocks.             |
| AUTO_INCREMENT  | Applies to a feature not available in StarRocks.             |
| CREATE_TIME     | The time when the table was created.                          |
| UPDATE_TIME     | The last time when the table was updated.                     |
| CHECK_TIME      | The last time when a consistency check was performed on the table. |
| TABLE_COLLATION | The default collation of the table.                          |
| CHECKSUM        | Applies to a feature not available in StarRocks.             |
| CREATE_OPTIONS  | Applies to a feature not available in StarRocks.             |
| TABLE_COMMENT   | Comment on the table.                                        |
