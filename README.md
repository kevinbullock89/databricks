# Databricks

## Types of Clusters in Databricks

A Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning.

### All-purpose cluster

These types of Clusters are used to analyze data collaboratively via interactive notebooks. They are created using the CLI, UI, or REST API. An All-purpose Cluster can be terminated and restarted manually. They can also be shared by multiple users to do collaborative tasks interactively.

### Job Clusters

These types of clusters are used for running fast and robust automated tasks. They are created when you run a job on your new Job Cluster and terminate the Cluster once the job ends. A Job Cluster cannot be restarted.

## Modes in Databricks Cluster

Based on the cluster usage, there are three modes of clusters that Databricks supports. Which are:

### Standard cluster

Standard cluster mode is also called as No Isolation shared cluster, Which means these clusters can be shared by multiple users with no isolation between the users. In the case of single users, the standard mode is suggested. Workload supports in these modes of clusters are in Python, SQL, R, and Scala can all be run on standard clusters.

### High Concurrency Clusters

A managed cloud resource is a high-concurrency cluster. High-concurrency clusters have the advantage of fine-grained resource sharing for maximum resource utilisation and low query latencies.

Workloads written in SQL, Python, and R can be run on high-concurrency clusters. Running user code in separate processes, which is not possible in Scala, improves the performance and security of High Concurrency clusters.

### Single Node clusters

Single node clusters as the name suggests will only have one node i.e for the driver. There would be no worker node available in this mode. In this mode, the spark job runs on the driver note itself. This mode is more helpful in the case of small data analysis and Single-node machine learning workloads that use Spark to load and save data.

## Delta Lake

Delta Lake is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Lakehouse Platform. Delta Lake is open source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling. 

### Creating a Delta Table

Required:

- A CREATE TABLE statement
- A table name (below students)
- A schema

```sh
CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
```
NOTE: In Databricks Runtime 8.0 and above, Delta Lake is the default format and you don’t need USING DELTA.

Add additional argument, IF NOT EXISTS which checks if the table exists. This will overcome our error:

```sh
CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)
```

### Inserting Data

```sh
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);
```
Above, we completed three separate INSERT statements. Each of these is processed as a separate transaction with its own ACID guarantees. Most frequently, we'll insert many records in a single transaction:

```sh
INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)
```

### Querying a Delta Table

Delta Lake guarantees that any read against a table will always return the most recent version of the table, and that never encounter a state of deadlock due to ongoing operations.

```sh
SELECT * FROM students
```

Table reads can never conflict with other operations, and the newest version of the data is immediately available to all clients that can query the lakehouse. Because all transaction information is stored in cloud object storage alongside the data files, concurrent reads on Delta Lake tables is limited only by the hard limits of object storage on cloud vendors.

### Updating Records

Updating records provides atomic guarantees as well: we perform a snapshot read of the current version of our table, find all fields that match our WHERE clause, and then apply the changes as described.

```sh
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"
```

### Deleting Records

Deletes are also atomic, so there's no risk of only partially succeeding when removing data from your data lakehouse.

```sh
DELETE FROM students 
WHERE value > 6
```

### Merge

Some SQL systems have the concept of an upsert, which allows updates, inserts, and other data manipulations to be run as a single command. Databricks uses the MERGE keyword to perform this operation.

MERGE statements must have at least one field to match on, and each WHEN MATCHED or WHEN NOT MATCHED clause can have any number of additional conditional statements.

```sh
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *
```

### Dropping a table

```sh
DROP TABLE students
```

### Examine Table Details

Databricks uses a Hive metastore by default to register databases, tables, and views. Using DESCRIBE EXTENDED allows to see important metadata about a table.

```sh
DESCRIBE EXTENDED students
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_EXTENDED.JPG)

DESCRIBE DETAIL is another command that allows us to explore table metadata.

```sh
DESCRIBE DETAIL students
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_DETAIL.JPG)

### Explore Delta Lake Files

It is possible to see the files backing the Delta Lake table by using a Databricks Utilities function:

```sh
%python
display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/EXPLORE.JPG)

Note that the directory contains a number of Parquet data files and a directory named _delta_log.

```sh
%python
display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))
```

```sh
%python
display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))
```

### Compacting Small Files and Indexing

Small files can occur for a variety of reasons; in our case, we performed a number of operations where only one or several records were inserted. Files will be combined toward an optimal size (scaled based on the size of the table) by using the OPTIMIZE command.

OPTIMIZE will replace existing data files by combining records and rewriting the results.

When executing OPTIMIZE, users can optionally specify one or several fields for ZORDER indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

```sh
OPTIMIZE students
ZORDER BY id
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/OPTIMIZE.JPG)

### Reviewing Delta Lake Transactions

Returns provenance information, including the operation, user, and so on, for each write to a table. Table history is retained for 30 days.

```sh
DESCRIBE HISTORY students
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_HISTORY.JPG)

Time travel queries can be performed by specifying either the integer version or a timestamp

```sh
SELECT * 
FROM students VERSION AS OF 3
```

### Rollback Versions

```sh
DELETE FROM students
```
```sh
RESTORE TABLE students TO VERSION AS OF 8 
```

### Cleaning Up Stale Files

By default, VACUUM will prevent deleting files less than 7 days old, just to ensure that no long-running operations are still referencing any of the files to be deleted. Run VACUUM on a Delta table, you lose the ability time travel back to a version older than the specified data retention period. 

```sh
VACUUM students
```

Use the DRY RUN version of vacuum to print out all records to be deleted

```sh
VACUUM students RETAIN 0 HOURS DRY RUN
```

## Databases and Tables on Databricks

### Using Hive Variables

```sh
SELECT "${da.db_name}" AS db_name,
       "${da.paths.working_dir}" AS working_dir;    
```

### Databases

```sh
CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;
CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';   
```

```sh
DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;   
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_DATABASE_EXTENDED.JPG)

```sh
DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;   
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_DATABASE_EXTENDED_CUSTOM.JPG)

Default location:

```sh
USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;
```


```sh
USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;
```

```sh
USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;
```

### Tables

```sh
USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;
```

## Views and CTEs

### Views

A Temp View is available across the context of a Notebook.

```sh
-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flight_delays',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;
```

```sh
SHOW TABLES;
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/SHOW_TABLES.JPG)

```sh
CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/SHOW_TABLES_II.JPG)

### External View

```sh
CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/SHOW_TABLES_III.JPG)

### Global Temp Views

A Global Temp View is available to all Notebooks running on that Databricks Cluster.

```sh
CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;
```

```sh
SHOW TABLES IN global_temp;
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/SHOW_TABLES_IN.JPG)

## Common Table Expressions

CTEs can be used in a variety of contexts. Below, are a few examples of the different ways a CTE can be used in a query. First, an example of making multiple column aliases using a CTE.

```sh
WITH flight_delays(
  total_delay_time,
  origin_airport,
  destination_airport
) AS (
  SELECT
    delay,
    origin,
    destination
  FROM
    external_table
)
SELECT
  *
FROM
  flight_delays
WHERE
  total_delay_time > 120
  AND origin_airport = "ATL"
  AND destination_airport = "DEN";
```

## Querying Files Directly 

### Quere a single File:

```sh
SELECT * FROM file_format.`/path/to/file`;
SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`

```

### Query a Directory of Files:

```sh
SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka`
```

### Create References to Files

This ability to directly query files and directories means that additional Spark logic can be chained to queries against files.

```sh
CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;

SELECT * FROM events_temp_view
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/JSON.JPG)

### Extract Text Files as Raw Strings

When working with text-based files (which include JSON, CSV, TSV, and TXT formats), you can use the text format to load each line of the file as a row with one string column named value.

```sh
SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/TEXT.JPG)

### Extract the Raw Bytes and Metadata of a File

Some workflows may require working with entire files, such as when dealing with images or unstructured data. Using binaryFile to query a directory will provide file metadata alongside the binary representation of the file contents.

```sh
SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/BINARY.JPG)

## Providing Options for External Sources

While directly querying files works well for self-describing formats, many data sources require additional configurations or schema declaration to properly ingest records.

### Registering Tables on External Data with Read Options

While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options. While there are many additional configurations you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.

```sh
CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"
```

### Extracting Data from SQL Databased

SQL databases are an extremely common data source, and Databricks has a standard JDBC driver for connecting with many flavors of SQL.

```sh
DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)
```

## Creating Delta Tables

After extracting data from external data sources, load data into the Lakehouse to ensure that all of the benefits of the Databricks platform can be fully leveraged. While different organizations may have varying policies for how data is initially loaded into Databricks, we typically recommend that early tables represent a mostly raw version of the data, and that validation and enrichment occur in later stages. This pattern ensures that even if data doesn't match expectations with regards to data types or column names, no data will be dropped, meaning that programmatic or manual intervention can still salvage data in a partially corrupted or invalid state.

### Create Table as Select (CTAS)

CREATE TABLE AS SELECT statements create and populate Delta tables using data retrieved from an input query.

```sh
CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`;

DESCRIBE EXTENDED sales;
```

CTAS statements automatically infer schema information from query results and do not support manual schema declaration. This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables. CTAS statements also do not support specifying additional file options.

```sh
CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta
```

### Filtering and Renaming Columns from Existing Tables

Simple transformations like changing column names or omitting columns from target tables can be easily accomplished during table creation.

```sh
CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases
```

### Declare Schema with Generated Columns

Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table.

```sh
CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")
```

Because date is a generated column, if we write to purchase_dates without providing values for the date column, Delta Lake automatically computes them.

### Add a Table Constraint

Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.

Databricks currently support two types of constraints:

  - NOT NULL constraints
  - CHECK constraints

```sh
ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/CONSTRAINT.JPG)

### Enrich Tables with Additional Options and Metadata

```sh
CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/raw/users-historical/`;
  
SELECT * FROM users_pii;
```

### Cloning Delta Lake Tables

DEEP CLONE fully copies data and metadata from a source table to a target. This copy occurs incrementally, so executing this command again can sync changes from the source to the target location.


```sh
CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases
```

Because all the data files must be copied over, this can take quite a while for large datasets. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move.

```sh
CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases
```

## Complete Overwrites

We can use overwrites to atomically replace all of the data in a table. There are multiple benefits to overwriting tables instead of deleting and recreating tables:

  - Overwriting a table is much faster because it doesn’t need to list the directory recursively or delete any files.
  - The old version of the table still exists; can easily retrieve the old data using Time Travel.
  - It’s an atomic operation. Concurrent queries can still read the table while you are deleting the table.
  - Due to ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.

```sh
CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/events-historical`
```

INSERT OVERWRITE provides a nearly identical outcome as above: data in the target table will be replaced by data from the query. INSERT OVERWRITE:

  - Can only overwrite an existing table, not create a new one like our CRAS statement
  - Can overwrite only with new records that match the current table schema -- and thus can be a "safer" technique for overwriting an existing table without disrupting     downstream consumers
  - Can overwrite individual partition

```sh
INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`
```

### Append Rows

We can use INSERT INTO to atomically append new rows to an existing Delta table. This allows for incremental updates to existing tables, which is much more efficient than overwriting each time.

Append new sale records to the sales table using INSERT INTO.

```sh
INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-30m`
```

### Merge Updates

The main benefits of MERGE:

 - updates, inserts, and deletes are completed as a single transaction
 - multiple conditionals can be added in addition to matching fields
 - provides extensive options for implementing custom logic

```sh
MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *
```

### Load Incrementally

COPY INTO provides SQL engineers an idempotent option to incrementally ingest data from external systems.

```sh
COPY INTO sales
FROM "${da.paths.datasets}/raw/sales-30m"
FILEFORMAT = PARQUET
```

## Cleaning Data

- count(col) skips NULL values when counting specific columns or expressions.
- count(*) is a special case that counts the total number of rows (including rows that are only NULL values).
- To count null values, use the count_if function or WHERE clause to provide a condition that filters for records where the value IS NULL
- Distinct Records count(DISNTINCT(*))
- Distinct Records count(DISNTINCT(col))

## Advanced SQL Transformations

### Interacting with JSON Data

The events_raw table was registered against data representing a Kafka payload.

```sh
CREATE OR REPLACE TEMP VIEW events_strings AS
  SELECT string(key), string(value) 
  FROM events_raw;
  
SELECT * FROM events_strings
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/KAFKA_JSON.JPG)

Spark SQL has built-in functionality to directly interact with JSON data stored as strings. We can use the : syntax to traverse nested data structures.

```sh
SELECT value:device, value:geo:city 
FROM events_strings
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/KAFKA_VALUES.JPG)

Spark SQL also has the ability to parse JSON objects into struct types (a native Spark type with nested attributes).

```sh
SELECT value 
FROM events_strings 
WHERE value:event_name = "finalize" 
ORDER BY key
LIMIT 1
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/KAFKA_EVENT.JPG)

Spark SQL also has a schema_of_json function to derive the JSON schema from an example

```sh
CREATE OR REPLACE TEMP VIEW parsed_events AS
  SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
  FROM events_strings;
  
SELECT * FROM parsed_events
```

Once a JSON string is unpacked to a struct type, Spark supports * (star) unpacking to flatten fields into columns.

```sh
CREATE OR REPLACE TEMP VIEW new_events_final AS
  SELECT json.* 
  FROM parsed_events;
  
SELECT * FROM new_events_final
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/JSON_FINALE.JPG)

### Explore Arrays

The items field in the events table is an array of structs. Spark SQL has a number of functions specifically to deal with arrays. The explode function lets us put each element in an array on its own row.

```sh
CREATE OR REPLACE TEMP VIEW new_events_final AS
  SELECT json.* 
  FROM parsed_events;
  
SELECT * FROM new_events_final
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/EXPLORE_ARRAY.JPG)

### Collect Arrays

  - The collect_set function can collect unique values for a field, including fields within arrays.

  - The flatten function allows multiple arrays to be combined into a single array.

  - The array_distinct function removes duplicate elements from an array.

```sh
SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM events
GROUP BY user_id
```

### Join Tables

Spark SQL supports standard join operations (inner, outer, left, right, anti, cross, semi).

### Set Operators

  - Spark SQL supports UNION, MINUS, and INTERSECT set operators. 
  - UNION returns the collection of two queries.
  - INTERSECT returns all rows found in both relations.

### Pivot Tables

The PIVOT clause is used for data perspective. We can get the aggregated values based on specific column values, which will be turned to multiple columns used in SELECT clause. The PIVOT clause can be specified after the table name or subquery.

SELECT * FROM (): The SELECT statement inside the parentheses is the input for this table.

PIVOT: The first argument in the clause is an aggregate function and the column to be aggregated. Then, we specify the pivot column in the FOR subclause. The IN operator contains the pivot column values.

```sh
CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    email,
    order_id,
    transaction_timestamp,
    total_item_quantity,
    purchase_revenue_in_usd,
    unique_items,
    item.item_id AS item_id,
    item.quantity AS quantity
  FROM sales_enriched
) PIVOT (
  sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  )
);

SELECT * FROM transactions
```

### Higher Order Functions

Higher order functions in Spark SQL allow you to work directly with complex data types. When working with hierarchical data, records are frequently stored as array or map type objects. Higher-order functions allow you to transform data while preserving the original structure.

Higher order functions include:

  - FILTER filters an array using the given lambda function.
  - EXIST tests whether a statement is true for one or more elements in an array.
  - TRANSFORM uses the given lambda function to transform all elements in an array.
  - REDUCE takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer.

Filter:

```sh
-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/FILTER.JPG)

Transform:

```sh
-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/TRANSFORM.JPG)

Sources: 
- https://sparkbyexamples.com/spark/types-of-clusters-in-databricks/
- https://hevodata.com/learn/databricks-clusters/
- https://docs.databricks.com/clusters/index.html 
- https://learn.microsoft.com/en-us/azure/databricks/delta/
- https://docs.databricks.com/sql/language-manual/delta-describe-history.html
- https://community.databricks.com/s/question/0D53f00001GHVPFCA5/whats-the-difference-between-a-global-view-and-a-temp-view
