---
displayed_sidebar: docs
---

# CREATE TABLE

Create a new table in StarRocks.

:::tip
This operation requires the CREATE TABLE privilege on the destination database.
:::

## Syntax

```SQL
CREATE [EXTERNAL] [TEMPORARY] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive|hudi|iceberg|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
[distribution_desc]
[rollup_index]
[ORDER BY (column_name1,...)]
[PROPERTIES ("key"="value", ...)]
```

:::tip

- The table name, partition name, column name, and index name you create must follow the naming conventions in [System limits](../../System_limit.md).
- When you specify database name, table name, column name, or partition name, note that some literals are used as reserved keywords in StarRocks. Do not directly use these keywords in SQL statements. If you want to use such a keyword in an SQL statement, enclose it in a pair of backticks (`). See [Keywords](../keywords.md) for these reserved keywords.

:::

## Keywords

### `EXTERNAL`

:::caution
The `EXTERNAL` keyword is deprecated.

We recommend that you use [external catalogs](../../../data_source/catalog/catalog_overview.md) to query data from Hive, Iceberg, Hudi, and JDBC data sources instead of using the `EXTERNAL` keyword to create external tables.

:::

:::tip 
**Recommendation**

From v3.1 onwards, StarRocks supports creating Parquet-formatted tables in Iceberg catalogs, and supports sinking data to these Parquet-formatted Iceberg tables by using INSERT INTO.

From v3.2 onwards, StarRocks supports creating Parquet-formatted tables in Hive catalogs, and supports sinking data to these Parquet-formatted Hive tables by using INSERT INTO. From v3.3 onwards, StarRocks supports creating ORC- and Textfile-formatted tables in Hive catalogs, and supports sinking data to these ORC- and Textfile-formatted Hive tables by using INSERT INTO.
:::

If you would like to use the deprecated `EXTERNAL` keyword, please expand **`EXTERNAL` keyword details**

<details>

<summary>`EXTERNAL` keyword details</summary>

To create an external table to query external data sources, specify `CREATE EXTERNAL TABLE` and set `ENGINE` to any of these values. You can refer to [External table](../../../data_source/External_table.md) for more information.

- For MySQL external tables, specify the following properties:

    ```plaintext
    PROPERTIES (
        "host" = "mysql_server_host",
        "port" = "mysql_server_port",
        "user" = "your_user_name",
        "password" = "your_password",
        "database" = "database_name",
        "table" = "table_name"
    )
    ```

    Note:

    "table_name" in MySQL should indicate the real table name. In contrast, "table_name" in CREATE TABLE statement indicates the name of this MySQL table on StarRocks. They can either be different or the same.

    The aim of creating MySQL tables in StarRocks is to access MySQL database. StarRocks itself does not maintain or store any MySQL data.

- For Elasticsearch external tables, specify the following properties:

    ```plaintext
    PROPERTIES (
    "hosts" = "http://192.168.xx.xx:8200,http://192.168.xx0.xx:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

  - `hosts`: the URL that is used to connect your Elasticsearch cluster. You can specify one or more URLs.
  - `user`: the account of the root user that is used to log in to your Elasticsearch cluster for which basic authentication is enabled.
  - `password`: the password of the preceding root account.
  - `index`: the index of the StarRocks table in your Elasticsearch cluster. The index name is the same as the StarRocks table name. You can set this parameter to the alias of the StarRocks table.
  - `type`: the type of index. The default value is `doc`.

- For Hive external tables, specify the following properties:

    ```plaintext
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
    )
    ```

    Here, database is the name of the corresponding database in Hive table. Table is the name of Hive table. `hive.metastore.uris` is the server address.

- For JDBC external tables, specify the following properties:

    ```plaintext
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` is the JDBC resource name and `table` is the destination table.

- For Iceberg external tables, specify the following properties:

   ```plaintext
    PROPERTIES (
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table"
    )
    ```

    `resource` is the Iceberg resource name. `database` is the Iceberg database. `table` is the Iceberg table.

- For Hudi external tables, specify the following properties:

  ```plaintext
    PROPERTIES (
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
    )
    ```

</details>

### `TEMPORARY`

Creates a temporary table. From v3.3.1, StarRocks supports creating temporary tables in the Default Catalog. For more information, see [Temporary Table](../../../table_design/StarRocks_table_design.md#temporary-table).

:::note
When creating a temporary table, you must set `ENGINE` to `olap`.
:::


## Column definition

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

### col_name

Note that normally you cannot create a column whose name is initiated with `__op` or `__row` because these name formats are reserved for special purposes in StarRocks and creating such columns may result in undefined behavior. If you do need to create such column, set the FE dynamic parameter [`allow_system_reserved_names`](../../../administration/management/FE_configuration.md#allow_system_reserved_names) to `TRUE`.

### col_type

Specific column information, such as types and ranges:

- TINYINT (1 byte): Ranges from -2^7 + 1 to 2^7 - 1.
- SMALLINT (2 bytes): Ranges from -2^15 + 1 to 2^15 - 1.
- INT (4 bytes): Ranges from -2^31 + 1 to 2^31 - 1.
- BIGINT (8 bytes): Ranges from -2^63 + 1 to 2^63 - 1.
- LARGEINT (16 bytes): Ranges from -2^127 + 1 to 2^127 - 1.
- FLOAT (4 bytes): Supports scientific notation.
- DOUBLE (8 bytes): Supports scientific notation.
- DECIMAL[(precision, scale)] (16 bytes)

  - Default value: DECIMAL(10, 0)
  - precision: 1 ~ 38
  - scale: 0 ~ precision
  - Integer part: precision - scale

    Scientific notation is not supported.

- DATE (3 bytes): Ranges from 0000-01-01 to 9999-12-31.
- DATETIME (8 bytes): Ranges from 0000-01-01 00:00:00 to 9999-12-31 23:59:59.
- CHAR[(length)]: Fixed length string. Range: 1 ~ 255. Default value: 1.
- VARCHAR[(length)]: A variable-length string. The default value is 1. Unit: bytes. In versions earlier than StarRocks 2.1, the value range of `length` is 1–65533. [Preview] In StarRocks 2.1 and later versions, the value range of `length` is 1–1048576.
- HLL (1~16385 bytes): For HLL type, there's no need to specify length or default value. The length will be controlled within the system according to data aggregation. HLL column can only be queried or used by [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md), [Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md), and [hll_hash](../../sql-functions/scalar-functions/hll_hash.md).
- BITMAP: Bitmap type does not require specified length or default value. It represents a set of unsigned bigint numbers. The largest element could be up to 2^64 - 1.

### agg_type

Aggregation type. If not specified, this column is a key column.
If specified, it is a value column. The aggregation types supported are as follows:

- `SUM`, `MAX`, `MIN`, `REPLACE`
- `HLL_UNION` (only for `HLL` type)
- `BITMAP_UNION` (only for `BITMAP`)
- `REPLACE_IF_NOT_NULL`: This means the imported data will only be replaced when it is of non-null value. If it is of null value, StarRocks will retain the original value.

:::note
- When the column of aggregation type BITMAP_UNION is imported, its original data types must be TINYINT, SMALLINT, INT, and BIGINT.
- If NOT NULL is specified by REPLACE_IF_NOT_NULL column when the table was created, StarRocks will still convert the data to NULL without sending an error report to the user. With this, the user can import selected columns.
:::

This aggregation type applies ONLY to the Aggregate table whose key_desc type is AGGREGATE KEY. Since v3.1.9, `REPLACE_IF_NOT_NULL` newly supports the columns of the BITMAP type.

**NULL | NOT NULL**: Whether the column is allowed to be `NULL`. By default, `NULL` is specified for all columns in a table that uses the Duplicate Key, Aggregate, or Unique Key table. In a table that uses the Primary Key table, by default, value columns are specified with `NULL`, whereas key columns are specified with `NOT NULL`. If `NULL` values are included in the raw data, present them with `\N`. StarRocks treats `\N` as `NULL` during data loading.

**DEFAULT "default_value"**: the default value of a column. When you load data into StarRocks, if the source field mapped onto the column is empty, StarRocks automatically fills the default value in the column. You can specify a default value in one of the following ways:

- **DEFAULT current_timestamp**: Use the current time as the default value. For more information, see [current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md).
- **DEFAULT `<default_value>`**: Use a given value of the column data type as the default value. For example, if the data type of the column is VARCHAR, you can specify a VARCHAR string, such as beijing, as the default value, as presented in `DEFAULT "beijing"`. Note that default values cannot be any of the following types: ARRAY, BITMAP, JSON, HLL, and BOOLEAN.
- **DEFAULT (\<expr\>)**: Use the result returned by a given function as the default value. Only the [uuid()](../../sql-functions/utility-functions/uuid.md) and [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md) expressions are supported.

**AUTO_INCREMENT**: specifies an `AUTO_INCREMENT` column. The data types of `AUTO_INCREMENT` columns must be BIGINT. Auto-incremented IDs start from 1 and increase at a step of 1. For more information about `AUTO_INCREMENT` columns, see [AUTO_INCREMENT](auto_increment.md). Since v3.0, StarRocks supports `AUTO_INCREMENT` columns.

**AS generation_expr**: specifies the generated column and its expression. [The generated column](../generated_columns.md) can be used to precompute and store the results of expressions, which significantly accelerates queries with the same complex expressions. Since v3.1, StarRocks supports generated columns.

## Index definition

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

For more information about parameter descriptions and usage notes, see [Bitmap indexing](../../../table_design/indexes/Bitmap_index.md#create-an-index).

## `ENGINE`

Default value: `olap`. If this parameter is not specified, an OLAP table (StarRocks native table) is created by default.

Optional value: `mysql`, `elasticsearch`, `hive`, `jdbc`, `iceberg`, and `hudi`. 

## Key

Syntax:

```SQL
key_type(k1[,k2 ...])
```

Data is sequenced in specified key columns and has different attributes for different key types:

- AGGREGATE KEY: Identical content in key columns will be aggregated into value columns according to the specified aggregation type. It usually applies to business scenarios such as financial statements and multi-dimensional analysis.
- UNIQUE KEY/PRIMARY KEY: Identical content in key columns will be replaced in value columns according to the import sequence. It can be applied to make addition, deletion, modification and query on key columns.
- DUPLICATE KEY: Identical content in key columns, which also exists in StarRocks at the same time. It can be used to store detailed data or data with no aggregation attributes. 

  :::note
  DUPLICATE KEY is the default type. Data will be sequenced according to key columns.
  :::

:::note
Value columns do not need to specify aggregation types when other key_type is used to create tables with the exception of AGGREGATE KEY.
:::

## COMMENT

You can add a table comment when you create a table, optional. Note that COMMENT must be placed after `key_desc`. Otherwise, the table cannot be created.

From v3.1 onwards, you can modify the table comment suing `ALTER TABLE <table_name> COMMENT = "new table comment"`.

## Partition

Partitions can be managed in the following ways:

### Create partitions dynamically

[Dynamic partitioning](../../../table_design/data_distribution/dynamic_partitioning.md) provides a time-to-live (TTL) management for partitions. StarRocks automatically creates new partitions in advance and removes expired partitions to ensure data freshness. To enable this feature, you can configure Dynamic partitioning related properties at table creation.

### Create partitions one by one

#### Specify only the upper bound for a partition

Syntax:

```sql
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
  PARTITION <partition1_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  [ ,
  PARTITION <partition2_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  , ... ] 
)
```

:::note
Please use specified key columns and specified value ranges for partitioning.
:::

- For the naming conventions of partitions, see [System limits](../../System_limit.md).
- Before v3.3.0, columns for the range partitioning only support the following types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, and DATETIME. Since v3.3.0, three specific time functions can be used as columns for the range partitioning. For detailed usage, see [Data distribution](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions).
- Partitions are left closed and right open. The left boundary of the first partition is of minimum value.
- NULL value is stored only in partitions that contain minimum values. When the partition containing the minimum value is deleted, NULL values can no longer be imported.
- Partition columns can either be single columns or multiple columns. The partition values are the default minimum values.
- When only one column is specified as the partitioning column, you can set `MAXVALUE` as the upper bound for the partitioning column of the most recent partition.

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

:::note
- Partitions are often used for managing data related to time.
- When data backtracking is needed, you may want to consider emptying the first partition for adding partitions later when necessary.
:::

#### Specify both the lower and upper bounds for a partition

Syntax:

```SQL
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
(
    PARTITION <partition_name1> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1?" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
    [,
    PARTITION <partition_name2> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
    , ...]
)
```

:::note
- Fixed Range is more flexible than LESS THAN. You can customize the left and right partitions.
- Fixed Range is the same as LESS THAN in the other aspects.
- When only one column is specified as the partitioning column, you can set `MAXVALUE` as the upper bound for the partitioning column of the most recent partition.
:::

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p202101 VALUES [("20210101"), ("20210201")),
    PARTITION p202102 VALUES [("20210201"), ("20210301")),
    PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
  )
  ```

### Create multiple partitions in a batch

Syntax

- If the partitioning column is of a date type.

    ```sql
    PARTITION BY RANGE (<partitioning_column>) (
        START ("<start_date>") END ("<end_date>") EVERY (INTERVAL <N> <time_unit>)
    )
    ```

- If the partitioning column is of an integer type.

    ```sql
    PARTITION BY RANGE (<partitioning_column>) (
        START ("<start_integer>") END ("<end_integer>") EVERY (<partitioning_granularity>)
    )
    ```

Description

You can specify the start and end values in `START()` and `END()` and the time unit or partitioning granularity in `EVERY()` to create multiple partitions in a batch.

- Before v3.3.0, columns for the range partitioning only support the following types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, and DATETIME. Since v3.3.0, three specific time functions can be used as columns for the range partitioning. For detailed usage, see [Data distribution](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions).
- If the partitioning column is of a date type, you need to use the `INTERVAL` keyword to specify the time interval. You can specify the time unit as hour (since v3.0), day, week, month, or year. The naming conventions of partitions are the same as those for dynamic partitions.

For more information, see [Data distribution](../../../table_design/data_distribution/Data_distribution.md).


## Distribution

StarRocks supports hash bucketing and random bucketing. If you do not configure bucketing, StarRocks uses random bucketing and automatically sets the number of buckets by default.

- Random bucketing (since v3.1)

  For data in a partition, StarRocks distributes the data randomly across all buckets, which is not based on specific column values. And if you want StarRocks to automatically set the number of buckets, you do not need to specify any bucketing configurations. If you choose to manually specify the number of buckets, the syntax is as follows:

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```
  
  However, note that the query performance provided by random bucketing may not be ideal when you query massive amounts of data and frequently use certain columns as conditional columns. In this scenario, it is recommended to use hash bucketing. Because only a small number of buckets need to be scanned and computed, significantly improving query performance.

  **Precautions**
  - You can only use random bucketing to create Duplicate Key tables.
  - You can not specify a [Colocation Group](../../../using_starrocks/Colocate_join.md) for a table bucketed randomly.
  - Spark Load cannot be used to load data into tables bucketed randomly.
  - Since StarRocks v2.5.7, you do not need to set the number of buckets when you create a table. StarRocks automatically sets the number of buckets. If you want to set this parameter, see [Set the number of buckets](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets).

  For more information, see [Random bucketing](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31).

- Hash bucketing

  Syntax:

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  Data in partitions can be subdivided into buckets based on the hash values of the bucketing columns and the number of buckets. We recommend that you choose the column that meets the following two requirements as the bucketing column.

  - High cardinality column such as ID
  - Column that is often used as a filter in queries

  If such a column does not exist, you can determine the bucketing column according to the complexity of queries.

  - If the query is complex, we recommend that you select a high cardinality column as the bucketing column to ensure balanced data distribution among buckets and improve cluster resource utilization.
  - If the query is relatively simple, we recommend that you select the column that is often used as the query condition as the bucketing column to improve query efficiency.

  If partition data cannot be evenly distributed into each bucket by using one bucketing column, you can choose multiple bucketing columns (at most three). For more information, see [Choose bucketing columns](../../../table_design/data_distribution/Data_distribution.md#hash-bucketing).

  **Precautions**:
  - **When you create a table, you must specify its bucketing columns**.
  - The values of bucketing columns cannot be updated.
  - Bucketing columns cannot be modified after they are specified.
  - Since StarRocks v2.5.7, you do not need to set the number of buckets when you create a table. StarRocks automatically sets the number of buckets. If you want to set this parameter, see [Set the number of buckets](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets).

## Rollup index

You can create rollups in bulk when you create a table.

Syntax:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## ORDER BY

Since v3.0, Primary Key tables support defining sort keys using `ORDER BY`. Since v3.3, Duplicate Key tables, Aggregate tables, and Unique Key tables support defining sort keys using `ORDER BY`.

For more descriptions of sort keys, see [Sort keys and prefix indexes](../../../table_design/indexes/Prefix_index_sort_key.md).


## PROPERTIES

### Storage and replicas

If the engine type is `OLAP`, you can specify initial storage medium (`storage_medium`), automatic storage cooldown time (`storage_cooldown_time`) or time interval (`storage_cooldown_ttl`), and replica number (`replication_num`) when you create a table.

The scope where the properties take effect: If the table has only one partition, the properties belong to the table. If the table is divided into multiple partitions, the properties belong to each partition. And when you need to configure different properties for specified partitions, you can execute [ALTER TABLE ... ADD PARTITION or ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md) after table creation.

#### Set initial storage medium and automatic storage cooldown time

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

**Properties**

- `storage_medium`: the initial storage medium, which can be set to `SSD` or `HDD`. Make sure that the type of storage medium you explicitly specified is consistent with the BE disk types for your StarRocks cluster specified in the BE static parameter `storage_root_path`.<br />

    If the FE configuration item `enable_strict_storage_medium_check` is set to `true`, the system strictly checks BE disk type when you create a table. If the storage medium you specified in CREATE TABLE is inconsistent with BE disk type, an error "Failed to find enough host in all backends with storage medium is SSD|HDD." is returned and table creation fails. If `enable_strict_storage_medium_check` is set to `false`, the system ignores this error and forcibly creates the table. However, cluster disk space may be unevenly distributed after data is loaded.<br />

    From v2.3.6, v2.4.2, v2.5.1, and v3.0 onwards, the system automatically infers storage medium based on BE disk type if `storage_medium` is not explicitly specified.<br />

  - The system automatically sets this parameter to SSD in the following scenarios:

    - The disk types reported by BEs (`storage_root_path`) contain only SSD.
    - The disk types reported by BEs (`storage_root_path`) contain both SSD and HDD. Note that from v2.3.10, v2.4.5, v2.5.4, and v3.0 onwards, the system sets `storage_medium` to SSD when `storage_root_path` reported by BEs contain both SSD and HDD and the property `storage_cooldown_time` is specified.

  - The system automatically sets this parameter to HDD in the following scenarios:

    - The disk types reported by BEs (`storage_root_path`) contain only HDD.
    - From 2.3.10, 2.4.5, 2.5.4, and 3.0 onwards,  the system sets `storage_medium` to HDD when `storage_root_path` reported by BEs contain both SSD and HDD and the property `storage_cooldown_time` is not specified.

- `storage_cooldown_ttl` or `storage_cooldown_time`: the automatic storage cooldown time or time interval. Automatic storage cooldown refers to automatically migrate data from SSD to HDD. This feature is only effective when the initial storage medium is SSD.

  - `storage_cooldown_ttl`: the **time interval** of automatic storage cooldown for the partitions in this table. If you need to retain the most recent partitions on SSD and automatically cool down older partitions to HDD after a certain time interval, you can use this parameter. The automatic storage cooldown time for each partition is calculated using the value of this parameter plus the upper time bound of the partition.

  The supported values are `<num> YEAR`, `<num> MONTH`, `<num> DAY`, and `<num> HOUR`. `<num>` is a non-negative integer. The default value is null, indicating that storage cooldown is not automatically performed.

  For example, you specify the value as `"storage_cooldown_ttl"="1 DAY"` when creating the table, and the partition `p20230801` with a range of `[2023-08-01 00:00:00,2023-08-02 00:00:00)` exists. The automatic storage cooldown time for this partition is `2023-08-03 00:00:00`, which is `2023-08-02 00:00:00 + 1 DAY`. If you specify the value as `"storage_cooldown_ttl"="0 DAY"` when creating the table, the automatic storage cooldown time for this partition is `2023-08-02 00:00:00`.

  - `storage_cooldown_time`: the automatic storage cooldown time (**absolute time**) when the table is cooled down from SSD to HDD. The specified time needs to be later than the current time. Format: "yyyy-MM-dd HH:mm:ss". When you need to configure different properties for specified partitions, you can execute [ALTER TABLE ... ADD PARTITION or ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md).

##### Usage

- The comparison between the parameters related to automatic storage cooldown is as follows:
  - `storage_cooldown_ttl`: A table property that specifies the time interval of automatic storage cooldown for partitions in the table. The system automatically cools down a partition at the time `the value of this parameter plus the upper time bound of the partition`. So automatic storage cooldown is performed at the partition granularity, which is more flexible.
  - `storage_cooldown_time`: A table property that specifies the automatic storage cooldown time (**absolute time**) for this table. Also, you can configure different properties for specified partitions after table creation.
  - `storage_cooldown_second`: A static FE parameter that specifies the automatic storage cooldown latency for all tables within the cluster.

- The table property `storage_cooldown_ttl` or `storage_cooldown_time` takes precedence over the FE static parameter `storage_cooldown_second`.
- When configuring these parameters, you need to specify `"storage_medium = "SSD"`.
- If you do not configure these parameters, automatic storage cooldown is not be automatically performed.
- Execute `SHOW PARTITIONS FROM <table_name>` to view the automatic storage cooldown time for each partition.

##### Limits

- Expression and List partitioning are not supported.
- The partition column need to be of date type.
- Multiple partition columns are not supported.
- Primary Key tables are not supported.

#### Set the number of replicas for each tablet in partitions

`replication_num`: number of replicas for each table in the partitions. Default number: `3`.

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

### Bloom filter indexes

If the Engine type is `olap`, you can specify a column to adopt bloom filter indexes.

The following limits apply when you use bloom filter index:

- You can create bloom filter indexes for all columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create bloom filter indexes for key columns.
- TINYINT, FLOAT, DOUBLE, and DECIMAL columns do not support creating bloom filter indexes.
- Bloom filter indexes can only improve the performance of queries that contain the `in` and `=` operators, such as `Select xxx from table where x in {}` and `Select xxx from table where column = xxx`. More discrete values in this column will result in more precise queries.

For more information, see [Bloom filter indexing](../../../table_design/indexes/Bloomfilter_index.md)

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

### Colocate Join

If you want to use Colocate Join attributes, specify it in `properties`.

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

### Dynamic partitions

If you want to use dynamic partition attributes, please specify it in properties.

```SQL
PROPERTIES (
    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partition.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
```

**`PROPERTIES`**

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| dynamic_partition.enable    | No       | Whether to enable dynamic partitioning. Valid values: `TRUE` and `FALSE`. Default value: `TRUE`. |
| dynamic_partition.time_unit | Yes      | The time granularity for dynamically created  partitions. It is a required parameter. Valid values: `DAY`, `WEEK`, and `MONTH`. The time granularity determines the suffix format for dynamically created partitions.<br/>  - If the value is `DAY`, the suffix format for dynamically created partitions is `yyyyMMdd`. An example partition name suffix is `20200321`.<br/>  - If the value is `WEEK`, the suffix format for dynamically created partitions is `yyyy_ww`, for example `2020_13` for the 13th week of 2020.<br/>  - If the value is `MONTH`, the suffix format for dynamically created partitions is `yyyyMM`, for example `202003`. |
| dynamic_partition.start     | No       | The starting offset of dynamic partitioning. The value of this parameter must be a negative integer. The partitions before this offset will be deleted based on the current day, week, or month which is determined by `dynamic_partition.time_unit`. The default value is `Integer.MIN_VALUE`, namely, -2147483648, which means that historical partitions will not be deleted. |
| dynamic_partition.end       | Yes      | The end offset of dynamic partitioning. The value of this parameter must be a positive integer. The partitions from the current day, week, or month to the end offset will be created in advance. |
| dynamic_partition.prefix    | No       | The prefix added to the names of dynamic partitions. Default value: `p`. |
| dynamic_partition.buckets   | No       | The number of buckets per dynamic partition. The default value is the same as the number of buckets determined by the reserved word `BUCKETS` or automatically set by StarRocks. |

:::note

When the partition column is the INT type, its format must be `yyyyMMdd`, regardless of the partition time granularity.

:::

### Bucket size with random bucketing

Since v3.2, for tables configured with random bucketing, you can specify the bucket size by using the `bucket_size` parameter in `PROPERTIES` at table creation to enable the on-demand and dynamic increase of the number of buckets. Unit: B.

```sql
PROPERTIES (
    "bucket_size" = "1073741824"
)
```

### Data compression algorithm

You can specify a data compression algorithm for a table by adding property `compression` when you create a table.

The valid values of `compression` are:

- `LZ4`: the LZ4 algorithm.
- `ZSTD`: the Zstandard algorithm.
- `ZLIB`: the zlib algorithm.
- `SNAPPY`: the Snappy algorithm.

From v3.3.2 onwards, StarRocks supports specifying the compression level for zstd compression format during table creation.

Syntax:

```sql
PROPERTIES ("compression" = "zstd(<compression_level>)")
```

`compression_level`: the compression level for ZSTD compression format. Type: Integer. Range: [1,22]. Default: `3` (Recommended). The greater the number, the higher the compression ratio. The higher the compression level, the more time consumption for compression and decompression.

Example:

```sql
PROPERTIES ("compression" = "zstd(3)")
```

For more information about how to choose a suitable data compression algorithm, see [Data compression](../../../table_design/data_compression.md).

### Write quorum for data loading

If your StarRocks cluster has multiple data replicas, you can set different write quorum for tables, that is, how many replicas are required to return loading success before StarRocks can determine the loading task is successful. You can specify write quorum by adding the property `write_quorum` when you create a table. This property is supported from v2.5.

The valid values of `write_quorum` are:

- `MAJORITY`: Default value. When the **majority** of data replicas return loading success, StarRocks returns loading task success. Otherwise, StarRocks returns loading task failed.
- `ONE`: When **one** of the data replicas returns loading success, StarRocks returns loading task success. Otherwise, StarRocks returns loading task failed.
- `ALL`: When **all** of the data replicas return loading success, StarRocks returns loading task success. Otherwise, StarRocks returns loading task failed.

:::caution
- Setting a low write quorum for loading increases the risk of data inaccessibility and even loss. For example, you load data into a table with one write quorum in a StarRocks cluster of two replicas, and the data was successfully loaded into only one replica. Despite that StarRocks determines the loading task succeeded, there is only one surviving replica of the data. If the server which stores the tablets of loaded data goes down, the data in these tablets becomes inaccessible. And if the disk of the server is damaged, the data is lost.
- StarRocks returns the loading task status only after all data replicas have returned the status. StarRocks will not return the loading task status when there are replicas whose loading status is unknown. In a replica, loading timeout is also considered as loading failed.
:::

### Replica data writing and replication mode

If your StarRocks cluster has multiple data replicas, you can specify the `replicated_storage` parameter in `PROPERTIES` to configure the data writing and replication mode among replicas.

- `true` (default in v3.0 and later) indicates "single leader replication", which means data is written only to the primary replica. Other replicas synchronize data from the primary replica. This mode significantly reduces CPU cost caused by data writing to multiple replicas. It is supported from v2.5.
- `false` (default in v2.5) indicates "leaderless replication", which means data is directly written to multiple replicas, without differentiating primary and secondary replicas. The CPU cost is multiplied by the number of replicas.

In most cases, using the default value gains better data writing performance. If you want to change the data writing and replication mode among replicas, run the ALTER TABLE command. Example:

```sql
ALTER TABLE example_db.my_table
SET ("replicated_storage" = "false");
```

### Delta Join unique and foreign key constraints

To enable query rewrite in the View Delta Join scenario, you must define the Unique Key constraints `unique_constraints` and Foreign Key constraints `foreign_key_constraints` for the table to be joined in the Delta Join. See [Asynchronous materialized view - Rewrite queries in View Delta Join scenario](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#query-delta-join-rewrite) for further information.

```SQL
PROPERTIES (
    "unique_constraints" = "<unique_key>[, ...]",
    "foreign_key_constraints" = "
    (<child_column>[, ...]) 
    REFERENCES 
    [catalog_name].[database_name].<parent_table_name>(<parent_column>[, ...])
    [;...]
    "
)
```

- `child_column`: the Foreign Key of the table. You can define multiple `child_column`.
- `catalog_name`: the name of the catalog where the table to join resides. The default catalog is used if this parameter is not specified.
- `database_name`: the name of the database where the table to join resides. The current database is used if this parameter is not specified.
- `parent_table_name`: the name of the table to join.
- `parent_column`: the column to be joined. They must be the Primary Keys or Unique Keys of the corresponding tables.

:::caution
- `unique_constraints` and `foreign_key_constraints` are only used for query rewrite. Foreign Key constraints checks are not guaranteed when data is loaded into the table. You must ensure the data loaded into the table meets the constraints.
- The primary keys of a Primary Key table or the unique keys of a Unique Key table are, by default, the corresponding `unique_constraints`. You do not need to set it manually.
- The `child_column` in a table's `foreign_key_constraints` must be referenced to a `unique_key` in another table's `unique_constraints`.
- The number of `child_column` and `parent_column` must agree.
- The data types of the `child_column` and the corresponding `parent_column` must match.
:::

### Cloud-native tables for shared-data clusters

To use your StarRocks Shared-data cluster, you must create cloud-native tables with the following properties:

```SQL
PROPERTIES (
    "storage_volume" = "<storage_volume_name>",
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>"
)
```

- `storage_volume`: The name of the storage volume used to store the cloud-native table you want to create. If this property is not specified, the default storage volume is used. This property is supported from v3.1 onwards.

- `datacache.enable`: Whether to enable the local disk cache. Default: `true`.

  - When this property is set to `true`, the data to be loaded is simultaneously written into the object storage and the local disk (as the cache for query acceleration).
  - When this property is set to `false`, the data is loaded only into the object storage.

  :::note
  To enable the local disk cache, you must specify the directory of the disk in the BE configuration item `storage_root_path`.
  :::

- `datacache.partition_duration`: The validity duration of the hot data. When the local disk cache is enabled, all data is loaded into the cache. When the cache is full, StarRocks deletes the less recently used data from the cache. When a query needs to scan the deleted data, StarRocks checks if the data is within the duration of validity. If the data is within the duration, StarRocks loads the data into the cache again. If the data is not within the duration, StarRocks does not load it into the cache. This property is a string value that can be specified with the following units: `YEAR`, `MONTH`, `DAY`, and `HOUR`, for example, `7 DAY` and `12 HOUR`. If it is not specified, all data is cached as the hot data.

  :::note
  This property is available only when `datacache.enable` is set to `true`.
  :::

### Fast schema evolution

`fast_schema_evolution`: Whether to enable fast schema evolution for the table. Valid values are `TRUE` or `FALSE` (default). Enabling fast schema evolution can increase the speed of schema changes and reduce resource usage when columns are added or dropped. Currently, this property can only be enabled at table creation, and it cannot be modified using [ALTER TABLE](ALTER_TABLE.md) after table creation.

  :::note
  - Fast schema evolution is supported for shared-nothing clusters since v3.2.0.
  - Fast schema evolution is supported for shared-data clusters since v3.3 and is enabled by default. You do not need to specify this property when creating cloud-native tables in shared-data clusters. The FE dynamic parameter `enable_fast_schema_evolution` (Default: true) controls this behavior.
  :::

### Forbid Base Compaction

`base_compaction_forbidden_time_ranges`: The time range within which Base Compaction is forbidden for the table. When this property is set, the system performs Base Compaction on eligible tablets only outside the specified time range. This property is supported from v3.2.13.

:::note
Make sure that the number of data loading to the table does not exceed 500 during the period when Base Compaction is forbidden.
:::

The value of `base_compaction_forbidden_time_ranges` follows the [Quartz cron syntax](https://productresources.collibra.com/docs/collibra/latest/Content/Cron/co_quartz-cron-syntax.htm), and only supports these fields: `<minute> <hour> <day-of-the-month> <month> <day-of-the-week>`, where `<minute>` must be `*`.

```SQL
crontab_param_value ::= [ "" | crontab ]

crontab ::= * <hour> <day-of-the-month> <month> <day-of-the-week>
```

- When this property is not set or set to `""` (an empty string), Base Compaction is not forbidden at any time.
- When this property is set to `* * * * *`, Base Compaction is always forbidden.
- Other values follow the Quartz cron syntax.
  - An independent value indicates the unit time of a field. For example, `8` in the `<hour>` field means 8:00-8:59.
  - A value range indicates the time range of a field. For example, `8-9` in the `<hour>` field means 8:00-9:59.
  - Multiple value ranges separated by commas indicate multiple time ranges of the field.
  - `<day of the week>` has a starting value of `1` for Sunday, and `7` stands for Saturday.

Example:

```SQL
-- Forbid Base Compaction from 8:00 am to 9:00 pm every day.
'base_compaction_forbidden_time_ranges' = '* 8-20 * * *'

-- Forbid Base Compaction from 0:00 am to 5:00 am and from 9:00 pm to 11:00 pm every day.
'base_compaction_forbidden_time_ranges' = '* 0-4,21-22 * * *'

-- Forbid Base Compaction from Monday to Friday (that is, allow it on Saturday and Sunday).
'base_compaction_forbidden_time_ranges' = '* * * * 2-6'

-- Forbid Base Compaction from 8:00 am to 9:00 pm every working day (that is, Monday to Friday).
'base_compaction_forbidden_time_ranges' = '* 8-20 * * 2-6'
```

### Specify Common Partition Expression TTL

From v3.5.0 onwards, StarRocks native tables support Common Partition Expression TTL.

`partition_retention_condition`: The expression that declares the partitions to be retained dynamically. Partitions that do not meet the condition in the expression will be dropped regularly.
- The expression can only contain partition columns and constants. Non-partition columns are not supported.
- Common Partition Expression applies to List partitions and Range partitions differently:
  - For tables with List partitions, StarRocks supports deleting partitions filtered by the Common Partition Expression.
  - For tables with Range partitions, StarRocks can only filter and delete partitions using the partition pruning capability of FE. Partitions correspond to predicates that are not supported by partition pruning cannot be filtered and deleted.

Example:

```SQL
-- Retain the data from the last three months. Column `dt` is the partition column of the table.
"partition_retention_condition" = "dt >= CURRENT_DATE() - INTERVAL 3 MONTH"
```

To disable this feature, you can use the ALTER TABLE statement to set this property as an empty string:

```SQL
ALTER TABLE tbl SET('partition_retention_condition' = '');
```

### Configure flat json config (only support on shared-nothing clusters now)

If you want to use flat json attributes, please specify it in properties. See [ Flat JSON ](../../../using_starrocks/Flat_json.md) for further information

```SQL
PROPERTIES (
    "flat_json.enable" = "true|false",
    "flat_json.null.factor" = "0-1",
    "flat_json.sparsity.factor" = "0-1",
    "flat_json.column.max" = "${integer_value}"
)
```

**Properties**

| Property                    | Required | Description                                                                                                                                                                                                                                                       |
| --------------------------- |----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `flat_json.enable`    | No       | Whether to enable the Flat JSON feature. After this feature is enabled, newly loaded JSON data will be automatically flattened, improving JSON query performance.                                                                                                 |
| `flat_json.null.factor` | No      | The proportion of NULL values in the column to extract for Flat JSON. A column will not be extracted if its proportion of NULL value is higher than this threshold. This parameter takes effect only when `flat_json.enable` is set to true.  Default value: 0.3. |
| `flat_json.sparsity.factor`     | No      | The proportion of columns with the same name for Flat JSON. Extraction is not performed if the proportion of columns with the same name is lower than this value. This parameter takes effect only when `flat_json.enable` is set to true. Default value: 0.9.    |
| `flat_json.column.max`       | No      | The maximum number of sub-fields that can be extracted by Flat JSON. This parameter takes effect only when `flat_json.enable` is set to true.  Default value: 100. |

## Examples

### Aggregate table with Hash bucketing and columnar storage

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Aggregate table with storage medium and cooldown time set

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
UNIQUE KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

### Duplicate Key table with Range partition, Hash bucketing, column-based storage, storage medium, and cooldown time

LESS THAN

```SQL
CREATE TABLE example_db.table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD", 
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

Note:

This statement will create three data partitions:

```SQL
( {    MIN     },   {"2014-01-01"} )
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )
```

Data outside these ranges will be not be loaded.

Fixed Range

```SQL
CREATE TABLE table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1, k2, k3)
(
    PARTITION p1 VALUES [("2014-01-01", "10", "200"), ("2014-01-01", "20", "300")),
    PARTITION p2 VALUES [("2014-06-01", "100", "200"), ("2014-07-01", "100", "300"))
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD"
);
```

### MySQL external table

```SQL
CREATE EXTERNAL TABLE example_db.table_mysql
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
)
```

### Table with HLL columns

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Table using BITMAP_UNION aggregation type

The original data type of `v1` and `v2` columns must be TINYINT, SMALLINT, or INT.

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Tables that support Colocate Join

```SQL
CREATE TABLE `t1` 
(
     `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);

CREATE TABLE `t2` 
(
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);
```

### Table with bitmap index

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Dynamic partition table

The dynamic partitioning function must be enabled ("dynamic_partition.enable" = "true") in FE configuration. For more information, see [Configure dynamic partitions](#configure-dynamic-partitions).

This example creates partitions for the next three days and deletes partitions created three days ago. For example, if today is 2020-01-08, partitions with the following names will be created: p20200108, p20200109, p20200110, p20200111, and their ranges are:

```plaintext
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```

```SQL
CREATE TABLE example_db.dynamic_partition
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
```

### Table with multiple partitions created in a batch, and an integer column as a partitioning column

  In the following example, the partitioning column `datekey` is of the INT type. All the partitions are created by only one simple partition clause  `START ("1") END ("5") EVERY (1)`. The range of all the partitions starts from `1` and ends at `5`, with a partition granularity of `1`:
  > **NOTE**
  >
  > The partitioning column values in **START()** and **END()** need to be wrapped in quotation marks, while the partition granularity in the **EVERY()** does not need to be wrapped in quotation marks.

  ```SQL
  CREATE TABLE site_access (
      datekey INT,
      site_id INT,
      city_code SMALLINT,
      user_name VARCHAR(32),
      pv BIGINT DEFAULT '0'
  )
  ENGINE=olap
  DUPLICATE KEY(datekey, site_id, city_code, user_name)
  PARTITION BY RANGE (datekey) (START ("1") END ("5") EVERY (1)
  )
  DISTRIBUTED BY HASH(site_id)
  PROPERTIES ("replication_num" = "3");
  ```

### Hive external table

Before you create a Hive external table, you must have created a Hive resource and database. For more information, see [External table](../../../data_source/External_table.md#deprecated-hive-external-table).

```SQL
CREATE EXTERNAL TABLE example_db.table_hive
(
    k1 TINYINT,
    k2 VARCHAR(50),
    v INT
)
ENGINE=hive
PROPERTIES
(
    "resource" = "hive0",
    "database" = "hive_db_name",
    "table" = "hive_table_name"
);
```

### Primary Key table with specific sort key

Suppose that you need to analyze user behavior in real time from dimensions such as users' address and last active time. When you create a table, you can define the `user_id` column as the primary key and define the combination of the `address` and `last_active` columns as the sort key.

```SQL
create table users (
    user_id bigint NOT NULL,
    name string NOT NULL,
    email string NULL,
    address string NULL,
    age tinyint NULL,
    sex tinyint NULL,
    last_active datetime,
    property0 tinyint NOT NULL,
    property1 tinyint NOT NULL,
    property2 tinyint NOT NULL,
    property3 tinyint NOT NULL
) 
PRIMARY KEY (`user_id`)
DISTRIBUTED BY HASH(`user_id`)
ORDER BY(`address`,`last_active`)
PROPERTIES(
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

### Partitioned temporary table

```SQL
CREATE TEMPORARY TABLE example_db.temp_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2);
```

### Table supporting flat JSON

:::note
Flat JSON is only support on shared-nothing clusters now.
:::

```SQL
CREATE TABLE example_db.example_table
(
    k1 DATE,
    k2 INT,
    v1 VARCHAR(2048),
    v2 JSON
)
ENGINE=olap
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY HASH(k2)
PROPERTIES (
    "flat_json.enable" = "true",
    "flat_json.null.factor" = "0.5",
    "flat_json.sparsity.factor" = "0.5",
    "flat_json.column.max" = "50"
);
```

## References

- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [USE](../Database/USE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [DROP TABLE](DROP_TABLE.md)
