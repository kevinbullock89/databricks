# Databricks

## Databricks Architecture

Azure Databricks is structured to enable secure cross-functional team collaboration while keeping a significant amount of backend services managed by Azure Databricks so you can stay focused on your data science, data analytics, and data engineering tasks.

Azure Databricks operates out of a control plane and a data plane.

The control plane includes the backend services that Azure Databricks manages in its own Azure account. Notebook commands and many other workspace configurations are stored in the control plane and encrypted at rest.

Your Azure account manages the data plane, and is where your data resides. This is also where data is processed. Use Azure Databricks connectors to connect clusters to external data sources outside of your Azure account to ingest data, or for storage. You can also ingest data from external streaming data sources, such as events data, streaming data, IoT data, and more.

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/ARCHITECTURE.JPG)

## Clusters

Clusters are made up of one ore more virtual machine (VM) instances.

  - Driver coordinates activities of executors
  - Executors run tasks composing a Spark job

### Types of Clusters in Databricks

A Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning.

### All-purpose cluster

These types of Clusters are used to analyze data collaboratively via interactive notebooks. They are created using the CLI, UI, or REST API. An All-purpose Cluster can be terminated and restarted manually. They can also be shared by multiple users to do collaborative tasks interactively. Retains up to 70 clusters for up to 30 days.

### Job Clusters

These types of clusters are used for running fast and robust automated tasks. They are created when you run a job on your new Job Cluster and terminate the Cluster once the job ends. A Job Cluster cannot be restarted. Retains up to 30 clusters.

## Modes in Databricks Cluster

Based on the cluster usage, there are three modes of clusters that Databricks supports. Which are:

### Standard cluster

Standard cluster mode is also called as No Isolation shared cluster, Which means these clusters can be shared by multiple users with no isolation between the users. In the case of single users, the standard mode is suggested. Workload supports in these modes of clusters are in Python, SQL, R, and Scala can all be run on standard clusters.

### High Concurrency Clusters

A managed cloud resource is a high-concurrency cluster. High-concurrency clusters have the advantage of fine-grained resource sharing for maximum resource utilisation and low query latencies.

Workloads written in SQL, Python, and R can be run on high-concurrency clusters. Running user code in separate processes, which is not possible in Scala, improves the performance and security of High Concurrency clusters.

### Single Node clusters

Single node clusters as the name suggests will only have one node i.e for the driver. There would be no worker node available in this mode. In this mode, the spark job runs on the driver note itself. This mode is more helpful in the case of small data analysis and Single-node machine learning workloads that use Spark to load and save data.

## Data objects in the Lakehouse

The Databricks Lakehouse architecture combines data stored with the Delta Lake protocol in cloud object storage with metadata registered to a metastore. 

### metastore

The metastore contains all of the metadata that defines data objects in the lakehouse.

### Catalog

A catalog is the highest abstraction (or coarsest grain) in the Databricks Lakehouse relational model. Every database will be associated with a catalog. Catalogs exist as objects within a metastore.

### Database

A database is a collection of data objects, such as tables or views (also called “relations”), and functions. In Azure Databricks, the terms “schema” and “database” are used interchangeably (whereas in many relational systems, a database is a collection of schemas).

### Table

An Azure Databricks table is a collection of structured data. A Delta table stores data as a directory of files on cloud object storage and registers table metadata to the metastore within a catalog and schema.

What is a managed table?

Azure Databricks manages both the metadata and the data for a managed table; when you drop a table, you also delete the underlying data. Managed tables are the default when creating a table. The data for a managed table resides in the LOCATION of the database it is registered to.

What is an unmanaged table?

Azure Databricks only manages the metadata for unmanaged (external) tables; when you drop a table, you do not affect the underlying data. Unmanaged tables will always specify a LOCATION during table creation. Because data and metadata are managed independently, you can rename a table or register it to a new database without needing to move any data.

### View

A view stores the text for a query typically against one or more data sources or tables in the metastore. In Databricks, a view is equivalent to a Spark DataFrame persisted as an object in a database. Creating a view does not process or write any data; only the query text is registered to the metastore in the associated database.

What is a temporary view?

A temporary view has a limited scope and persistence and is not registered to a schema or catalog.

### Functions

Functions allow you to associate user-defined logic with a database. Functions can return either scalar values or sets of rows. Functions are used to aggregate data. 

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

DESCRIBE DETAIL allows us to see some other details about our Delta table, including the number of files.

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

By default, managed tables in a schema without the location specified will be created in the dbfs:/user/hive/warehouse/<schema_name>.db/ directory.

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

A Temp View is available across the context of a Notebook. Temporary views skip persisting the definition in the underlying metastore. Views have metadata that can be accessed in the view’s directory. Temporary views are session-scoped and dropped when the Spark session ends. Views can be accessed after the session ends.

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

CTEs only alias the results of a query while that query is being planned and executed. As such, the following cell with throw an error when executed.

```sh
SELECT COUNT(*) FROM cte_json
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

While views can be used to persist direct queries against files between sessions, this approach has limited utility. CSV files are one of the most common file formats, but a direct query against these files rarely returns the desired results.

```sh
SELECT * FROM csv.`${DA.paths.sales_csv}`
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/EXTERNAL_SOURCES_CSV.JPG)

### Registering Tables on External Data with Read Options

While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options. While there are many additional configurations you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.

```sh
CREATE TABLE table_identifier (col_name1 col_type1, ...)
USING data_source
OPTIONS (key1 = val1, key2 = val2, ...)
LOCATION = path
```
Spark supports many data sources with custom options, and additional systems may have unofficial support through external libraries. 

Data Sources: https://docs.databricks.com/external-data/index.html, 
Libraries:    https://docs.databricks.com/libraries/index.html

The cell below demonstrates using Spark SQL DDL to create a table against an external CSV source, specifying:

1. The column names and types
2. The file format
3. The delimiter used to separate fields
4. The presence of a header
5. The path to where this data is stored

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

To create a table against an external source in PySpark, you can wrap this SQL code with the spark.sql() function.

```sh
%python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "{DA.paths.sales_csv}"
""")
```

Similar to when we directly queried our files and created a view, we are still just pointing to files stored in an external location. 

```sh
SELECT * FROM sales_csv
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/EXTERNAL_SOURCES_CSV_FORMATED.JPG)

All the metadata and options passed during table declaration will be persisted to the metastore, ensuring that data in the location will always be read with these options.

### Limits of Tables with External Data Sources

Note that whenever we're defining tables or queries against external data sources, we cannot expect the performance guarantees associated with Delta Lake and Lakehouse. For example: while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.

Manually refresh the cache of the data by running the REFRESH TABLE command.

```sh
REFRESH TABLE sales_csv
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

Query this table:

```sh
SELECT * FROM users_jdbc
```

Note that some SQL systems such as data warehouses will have custom drivers. Spark will interact with various external databases differently, but the two basic approaches can be summarized as either:

1) Moving the entire source table(s) to Databricks and then executing logic on the currently active cluster
2) Pushing down the query to the external SQL database and only transferring the results back to Databricks

In either case, working with very large datasets in external SQL databases can incur significant overhead because of either:

1) Network transfer latency associated with moving all data over the public internet
2) Execution of query logic in source systems not optimized for big data queries

## Manage Data with Delta Lake

Delta Lake is not:

  - Propretary technology
  - Storage format
  - Storage medium
  - Database service or data warehouse

Delta Lake is:

  - Open source
  - Builds upon standard data formats
  - Optimized for cloud object storage
  - Built for scalable metadata handling

Delta Lake brings ACID to object storage

  - Atomicity
    Atomicity means that you guarantee that either all of the transaction succeeds or none of it does.
  - Consistency
    This ensures that you guarantee that all data will be consistent.
  - Isolation
    Guarantees that all transactions will occur in isolation. No transaction will be affected by any other transaction.
  - Durability
    Durability means that, once a transaction is committed, it will remain in the system – even if there’s a system crash immediately following the transaction.
    
Problems solved by ACID:

1) Hard to append data
2) Modification of existing data difficult
3) Jobs failing mid way
4) Real-time operations hard
5) Costly to keep historical data versions


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

Many standard SQL query commands (e.g. DISTINCT, WHERE, GROUP BY, etc.) are available in Spark SQL to express transformations.

- count(col) skips NULL values when counting specific columns or expressions.
- count(*) is a special case that counts the total number of rows (including rows that are only NULL values).
- To count null values, use the count_if function or WHERE clause to provide a condition that filters for records where the value IS NULL
- Distinct Records count(DISNTINCT(*))
- Distinct Records count(DISNTINCT(col))

### Deduplicate Rows Based on Specific Columns

```sh
CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users
```

### Date Format and Regex

Uses regexp_extract to extract the domains from the email column using regex:

```sh
SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)
```
![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/REGEX.JPG)

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

-> schema_of_json() returns the schema derived from an example JSON string.
-> from_json() parses a column containing a JSON string into a struct type using the specified schema.
-> * unpacking can be used to flattens structs; col_name.* pulls out the subfields of col_name into their own columns.

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

  - explode() separates the elements of an array into multiple rows; this creates a new row for each element.
  - size() provides a count for the number of elements in an array for each row.

```sh
CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2
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

The PIVOT clause is used for data perspective. We can get the aggregated values based on specific column values, which will be turned to multiple columns used in SELECT clause. The PIVOT clause can be specified after the table name or subquery. Pivoting tables can rotate the data from long to wide format using the .pivot() function.

Therefore, the correct approach is:

The data engineer can rotate the data from long to wide format using the .pivot() function.

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

## SQL UDFs and Control FLow

Databricks added support for User Defined Functions (UDFs) registered natively in SQL starting in DBR 9.1. 

This feature allows users to register custom combinations of SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions leverage Spark SQL directly, maintaining all of the optimizations of Spark when applying your custom logic to large datasets.

### SQL UDFs

At minimum, a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.

Below, a simple function named yelling takes one parameter named text. It returns a string that will be in all uppercase letters with three exclamation points added to the end.

```sh
SELECT yelling(food) FROM foods
```

```sh
CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text), "!!!")
```

### Scoping and Permissions of SQL UDFs

Note that SQL UDFs will persist between execution environments (which can include notebooks, DBSQL queries, and jobs).

```sh
DESCRIBE FUNCTION yelling
```

The Body contains the script of the function itself.

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_FUNCTION.JPG)

```sh
DESCRIBE FUNCTION EXTENDED yelling
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DESCRIBE_FUNCTION_EXTENDED.JPG)

## Python User-Defined Functions (UDF)

A custom column transformation function

  - Can’t be optimized by Catalyst Optimizer
  - Function is serialized and sent to executors
  - Row data is deserialized from Spark's native binary format to pass to the UDF, and the results are serialized back into Spark's native format
  - For Python UDFs, additional interprocess communication overhead between the executor and a Python interpreter running on each worker node
  - In a Function type hints can be used to clarify the input and return types of the function.

Define a function (on the driver) to get the first letter of a string from the email field.

```sh
def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")
```

### Create Create and apply UDF

```sh
first_letter_udf = udf(first_letter_function)
```
```sh
from pyspark.sql.functions import col

display(sales_df.select(first_letter_udf(col("email"))))
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/PYTHON_UDF.JPG)

### Register UDF to use in SQL

Register the UDF using spark.udf.register to also make it available for use in the SQL namespace.

```sh
sales_df.createOrReplaceTempView("sales")

first_letter_udf = spark.udf.register("sql_udf", first_letter_function)
```
```sh
# You can still apply the UDF from Python
display(sales_df.select(first_letter_udf(col("email"))))
```

```sh
%sql
-- You can now also apply the UDF from SQL
SELECT sql_udf(email) AS first_letter FROM sales
```

### Use Decorator Syntax (Python only)

Alternatively, you can define and register a UDF using Python decorator syntax. The @udf decorator parameter is the Column datatype the function returns.

```sh
# Our input/output is a string
@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]
```

### Pandas/Vectorized UDFs

Pandas UDFs are available in Python to improve the efficiency of UDFs. Pandas UDFs utilize Apache Arrow to speed up computation. Apache Arrow, an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes with near-zero (de)serialization cost

```sh
import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")
```

## Incremental Data Ingestion with Auto Loader

Incremental ETL is important since it allows us to deal solely with new data that has been encountered since the last ingestion. Reliably processing only the new data reduces redundant processing and helps enterprises reliably scale data pipelines.

### Using Auto Loader

Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup. Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage. Auto Loader can ingest JSON, CSV, PARQUET, AVRO, ORC, TEXT, and BINARYFILE file formats. Auto Loader provides a Structured Streaming source called cloudFiles.

### Configure schema inference and evolution in Auto Loader

```sh
def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query
```

- data_source: The directory of the source data
- source_format: The format of the source data
- table_name: the name of the target table
- checkpoint_directory: The location for storing metadata about the stream

```sh
query = autoload_to_table(data_source = f"{DA.paths.working_dir}/tracker",
                          source_format = "json",
                          table_name = "target_table",
                          checkpoint_directory = f"{DA.paths.checkpoints}/target_table")
```

## Helper Function for Streaming Lessons

```sh
def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

block_until_stream_is_ready(query)
```

## Multi-Hop in the Lakehouse 

Delta Lake allows users to easily combine streaming and batch workloads in a unified multi-hop pipeline. Each stage of the pipeline represents a state of our data valuable to driving core use cases within the business. Because all data and metadata lives in object storage in the cloud, multiple users and applications can access data in near-real time, allowing analysts to access the freshest data as it's being processed.

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/MULT_HOP.png)

  - Bronze tables contain raw data ingested from various sources (JSON files, RDBMS data, IoT data, to name a few examples).

  - Silver tables provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based     on recent activity.

  - Gold tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly     sales per store, or gross revenue per quarter by department.

Bronze:

```sh
%sql
CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
  SELECT *, current_timestamp() receipt_time, input_file_name() source_file
  FROM recordings_raw_temp
)
```

Silver:

```sh
%sql
CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
  SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
  FROM bronze_tmp a
  INNER JOIN pii b
  ON a.mrn = b.mrn
  WHERE heartrate > 0)
```

Gold:

```sh
%sql
CREATE OR REPLACE TEMP VIEW patient_avg AS (
  SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
  FROM recordings_enriched_temp
  GROUP BY mrn, name, date_trunc("DD", time))
```

## Delta Live Tables

Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling.

Instead of defining your data pipelines using a series of separate Apache Spark tasks, you define streaming tables and materialized views that the system should create and keep up to date. Delta Live Tables manages how your data is transformed based on queries you define for each processing step.

### What is a Live Table?

Live Tables are materialized views for the lakehouse. A live table is: Defined by a SQL query, Created and kept up-to-date by a pipeline

Live tables provides tools to:

  - Manage dependencies
  - Control qualtity
  - Automate operations
  - Simplify collaboration
  - Save costs
  - Reduce latency

```sh
CREATE LIVE TABLE report
AS
SELECT sum(profit)
from prod.sales
```

### What is a Streaming Live Table?

Based on Spark Structured Streaming

A streaming live table is "statufl":
  - Ensure exactly-once processing of input rows
  - Inputs are onlu read once

Streaming Live tables compute results over append-only streams such as Kafka, Kinesis, or Auto Loader. Streaming live tables allow to reduce costs and latency by avoiding reprocessing of old data


```sh
CREATE STREAMING LIVE TABLE report
AS
SELECT sum(profit)
FROM cloud_files(prod.sales)
```
### Creating Live Tables

  1. Write create live table
  2. Create a pipeline
  3. Click start

### Development vs Productioln (BEST PRACTICE)

Fast iteration or enterprise grade reliability

Development Mode:

  - Reuses a long-running cluster running for fast iteration
  - No retries on errors enabling faster debugging

Production Mode:

  - Cuts costs by turning off clusters as soon as they are done (within 5 minutes)
  - Escalating retries, including cluster restarts, ensure reliability in the face of transient issues

### Declare LIVE Dependencies

Using the LIVE virtual schema

```sh
CREATE LIVE TABLE events
AS
SELECT ... from prod.raw_data

CREATE LIVE TABLE report
AS
SELECT .... FROM LIVE.events
```

  - Depenencies owned by other producers are just read from the catalog or spark data sources as normal
  - LIVE dependencies, from the same pipeline, are read from the LIVE schema
  - DLT detects LIVE dependencies and executes all operations in correct order
  - DLT handles parallelism and capture the lineage of the data

### Ensure correctness with Expectations

Expectations are tests that ensure data qualtiy in production

```sh
CONSTRAINT valid_timestamp
EXPECT (timestamp > '2012-01-01')
ON VIOLATION DROP
```

```sh
@dlt.expect_or_drop(
"valid_timestamp",
col("timestamp") > '2012-01-01'
)
```

Expectations are true/false expressions that are used to validate each row during processing.

DLT offers flexible policies on how to handle records that violate expectations:

  - Track number of bad records
  - Drop bad records
  - Abort processing for a single bad record

### Pipeline UI

  - Visualize data flows between
  - Discover metadata and quality of each table
  - Access to historical updates
  - Control operations

### The Event Log

The event log automatically records all pipelines operations

  - Operational Statistics
  - Provenance
  - Data Qualtity


### SQL for Delta STREAM Live Tables

Using Spark Structured Streaming for ingesting

This example creates a table with all the json data stored in "/data":

  - cloud_files keeps track of which files have been read to avoid duplication and wasted work
  - Supports both listing and notifications for arbitrary scale
  - Configurable schema inference and schema evolution

```sh
CREATE STREAMING LIVE TABLE raw_data
AS
SELECT * 
FROM cloud_files("/data", "json")
```

Bronze:

```sh
CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))
```

```sh
CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");
```

Silver:

Quality Control

The CONSTRAINT keyword introduces quality control. Similar in function to a traditional WHERE clause, CONSTRAINT integrates with DLT, enabling it to collect metrics on constraint violations. Constraints provide an optional ON VIOLATION clause, specifying an action to take on records that violate the constraint. The three modes currently supported by DLT include:

- Fail Update: 	Pipeline failure when constraint is violated
- Drop Row:     Discard records that contraints
- Omitted:      Records violating constraints will be included (but violations will be reported in metrics)

```sh
CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
AS
  SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
         timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
         date(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
         f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name
```

Gold:

```sh
CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Los Angeles')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr
```

### Using the SQL STREAM() function

  - STREAM(my_table) reads a stream of new records, instead of a snapshot
  - Streaming tables must be an append-only table
  - Anny append-only delta table can read as a stream

```sh
CREATE STREAMING LIVE TABLE mystream
AS
SELECT * 
FROM STREAM(my_table)
```

### Parameters in Delta Live Tables

A pipeline's configuration is a map of key value pairs that can be used to parameterize the code:

  - Improve code 
  - Reuse code in multiple pipelines for different data

### Change Data Capture (CDC)

Apply changes INTO for CDC

```sh
APPLY CHANGES INTO LIVE.cities
FROM STREAM(LIVE.city_updates)
KEYS (ID)
SEQUENCE BY ts
```

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/CDC.JPG)l

### Automated Data Management

  - Best Practices
    DLT encodes Delta best practices automatically when creating DLT tables
  - Physical Data
    DLT automatically manages the physical data to minimize cost and optimimize performance
  - Schema Evolution

### Delta Live Tables: Python VS SQL

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/DLT_PYTHON_SQL.JPG)

### Query Event Log

The event log is managed as a Delta Lake table with some of the more important fields stored as nested JSON data. The query below shows how simple it is to read this table and created a DataFrame and temporary view for interactive querying.

```sh
event_log_path = f"{DA.paths.storage_location}/system/events"

event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(event_log)
```

Set Latest Update ID

```sh
latest_update_id = spark.sql("""
    SELECT origin.update_id
    FROM event_log_raw
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1""").first().update_id

print(f"Latest Update ID: {latest_update_id}")

# Push back into the spark config so that we can use it in a later query.
spark.conf.set('latest_update.id', latest_update_id)
```

Perform Audit Logging

```sh
%sql
SELECT timestamp, details:user_action:action, details:user_action:user_name
FROM event_log_raw 
WHERE event_type = 'user_action'
```

Examine Lineage

```sh
%sql
SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
FROM event_log_raw 
WHERE event_type = 'flow_definition' AND 
      origin.update_id = '${latest_update.id}'
```

Examine Data Quality Metrics

```sh
%sql
SELECT row_expectations.dataset as dataset,
       row_expectations.name as expectation,
       SUM(row_expectations.passed_records) as passing_records,
       SUM(row_expectations.failed_records) as failing_records
FROM
  (SELECT explode(
            from_json(details :flow_progress :data_quality :expectations,
                      "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
          ) row_expectations
   FROM event_log_raw
   WHERE event_type = 'flow_progress' AND 
         origin.update_id = '${latest_update.id}'
  )
GROUP BY row_expectations.dataset, row_expectations.name
```

### Fundamentals of DLT SQL Syntax

Tables as Query Results.

Delta Live Tables adapts standard SQL queries to combine DDL (data definition language) and DML (data manipulation language) into a unified declarative syntax. There are two distinct types of persistent tables that can be created with DLT:

  - Live tables are materialized views for the lakehouse; they will return the current results of any query with each refresh
  - Streaming live tables are designed for incremental, near-real time data processing

Streaming Ingestion with Auto Loader

Databricks has developed the Auto Loader functionality to provide optimized execution for incrementally loading data from cloud object storage into Delta Lake. Using Auto Loader with DLT is simple: just configure a source data directory, provide a few configuration settings, and write a query against your source data. Auto Loader will automatically detect new data files as they land in the source cloud object storage location, incrementally processing new records without the need to perform expensive scans and recomputing results for infinitely growing datasets.

Validating, Enriching, and Transforming Data

DLT allows users to easily declare tables from results of any standard Spark transformations. DLT leverages features used elsewhere in Spark SQL for documenting datasets, while adding new functionality for data quality checks.

The Select Statement
The select statement contains the core logic of your query. In this example, we:

Cast the field order_timestamp to the timestamp type
Select all of the remaining fields (except a list of 3 we're not interested in, including the original order_timestamp)
Note that the FROM clause has two constructs that you may not be familiar with:

The LIVE keyword is used in place of the database name to refer to the target database configured for the current DLT pipeline
The STREAM method allows users to declare a streaming data source for SQL queries

Data Quality Constraints
DLT uses simple boolean statements to allow quality enforcement checks on data.

Table Comments
Table comments are standard in SQL, and can be used to provide useful information to users throughout your organization.

Table Properties
The TBLPROPERTIES field can be used to pass any number of key/value pairs for custom tagging of data. 

Live Tables vs. Streaming Live Tables

Live Tables
  - Always "correct", meaning their contents will match their definition after any update.
  - Return same results as if table had just been defined for first time on all data.
  - Should not be modified by operations external to the DLT Pipeline.

Streaming Live Tables
  - Only supports reading from "append-only" streaming sources.
  - Only reads each input batch once, no matter what.
  - Can perform operations on the table outside the managed DLT Pipeline.

### Fundamentals about Fundamentals of DLT Python Syntax

DLT syntax is not intended for interactive execution in a notebook. This notebook will need to be scheduled as part of a DLT pipeline for proper execution. At the time this notebook was written, the current Databricks runtime does not include the dlt module, so trying to execute any DLT commands in a notebook will fail. During the configuration of the DLT pipeline, a number of options were specified. One of these was a key-value pair added to the Configurations field.

Configurations in DLT pipelines are similar to parameters in Databricks Jobs, but are actually set as Spark configurations.

In Python, we can access these values usings spark.conf.get().

Throughout these lessons, we'll set the Python variable source early in the notebook and then use this variable as necessary in the code. The dlt module should be explicitly imported into your Python notebook libraries.

Here, we should importing pyspark.sql.functions as F.

Some developers import *, while others will only import the functions they need in the present notebook.

There are two distinct types of persistent tables that can be created with DLT:

  - Live tables are materialized views for the lakehouse; they will return the current results of any query with each refresh
  - Streaming live tables are designed for incremental, near-real time data processing

### Streaming Ingestion with Auto Loader

The query below returns a streaming DataFrame from a source configured with Auto Loader.

In addition to passing cloudFiles as the format, here we specify:

  - The option cloudFiles.format as json (this indicates the format of the files in the cloud object storage location)
  - The option cloudFiles.inferColumnTypes as True (to auto-detect the types of each column)
  - The path of the cloud object storage to the load method
  - A select statement that includes a couple of pyspark.sql.functions to enrich the data alongside all the source fields
 
 By default, @dlt.table will use the name of the function as the name for the target table.

```sh
@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )
```

## Orchestrating Jobs with Databricks

The Jobs API allows you to create, edit, and delete jobs.

You can use a Databricks job to run a data processing or data analysis task in a Databricks cluster with scalable resources. Your job can consist of a single task or can be a large, multi-task workflow with complex dependencies. Databricks manages the task orchestration, cluster management, monitoring, and error reporting for all of your jobs. You can run your jobs immediately or periodically through an easy-to-use scheduling system. You can implement job tasks using notebooks, JARS, Delta Live Tables pipelines, or Python, Scala, Spark submit, and Java applications.

## Databricks SQL

Databricks SQL (DB SQL) is a serverless data warehouse on the Databricks Lakehouse Platform that lets you run all your SQL and BI applications at scale with up to 12x better price/performance, a unified governance model, open formats and APIs, and your tools of choice – no lock-in.

## Managing Permissions

### Configuring Permissions

By default, admins will have the ability to view all objects registered to the metastore and will be able to control permissions for other users in the workspace. Users will default to having no permissions on anything registered to the metastore, other than objects that they create in DBSQL; note that before users can create any databases, tables, or views, they must have create and usage privileges specifically granted to them.

Generally, permissions will be set using Groups that have been configured by an administrator, often by importing organizational structures from SCIM integration with a different identity provider. This lesson will explore Access Control Lists (ACLs) used to control permissions, but will use individuals rather than groups.

### Table ACLs

Databricks allows you to configure permissions for the following objects:

- CATALOG   -> controls access to the entire data catalog.
- DATABASE  -> controls access to a database.
- TABLE     -> controls access to a managed or external table.
- VIEW      -> controls access to SQL views.
- FUNCTION  -> controls access to a named function.
- ANY FILE  -> controls access to the underlying filesystem. Users granted access to ANY FILE can bypass the restrictions put on the catalog, databases, tables, and                  views by reading from the file system directly. NOTE: At present, the ANY FILE object cannot be set from Data Explorer.

### Granting Privileges

Databricks admins and object owners can grant privileges according to the following rules:

- Databricks administrator  -> All objects in the catalog and the underlying filesystem.
- Catalog owner             -> All objects in the catalog.
- Database owner            -> All objects in the database.
- Table owner               -> Only the table (similar options for views and functions).

NOTE: At present, Data Explorer can only be used to modify ownership of databases, tables, and views. Catalog permissions can be set interactively with the SQL Query Editor.

### Privileges

The following privileges can be configured in Data Explorer:

- ALL PRIVILEGES      -> gives all privileges (is translated into all the below privileges).
- SELECT              -> gives read access to an object.
- MODIFY              -> gives ability to add, delete, and modify data to or from an object.
- READ_METADATA       -> gives ability to view an object and its metadata.
- USAGE               -> does not give any abilities, but is an additional requirement to perform any action on a database object.
- CREATE              -> gives ability to create an object (for example, a table in a database).

### Admin Configuration

At present, users do not have any Table ACL permissions granted on the default catalog hive_metastore by default. To enable the ability to create databases and tables in the default catalog using Databricks SQL, have a workspace admin run the following command in the DBSQL query editor:

```sh
GRANT usage, create ON CATALOG `hive_metastore` TO `users`
```

To confirm this has run successfully, execute the following query:

```sh
SHOW GRANT ON CATALOG `hive_metastore`
```

## Data Governance Overview

  - Data Access Control (Control who has access to which data)
  - Data Access Audit (Capture and record all access to data)
  - Data Lineage (Capture upstream sources and downstream consumers)
  - Data Discovery (Ability to search and discover authorized asstes)

### Databricks Unity Catalog

  - Unify governance across clouds
  - Unify data and AI assets
  - Unify existing catalogs

### Key Concepts

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/KEY_CONCEPTS_GOVERNANCE.JPG)

Traditional SQL two-level namespace:
SELECT * FROM *schema.table*

Unity Catalog three-level namespace:
SELECT * FROM *catalog.schema.table*

### Unity Catalog Architecture

  - User (Person)
  - Account administrator (Admin role)
  - Service Principal 
  - Service Principal with adminstrative privileges (Admin role)
  - Groups
  - Nesting groups

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/UNITY_CATALOG_ARCHITECTURE.JPG)

### Unity Catalog Identities

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/IDENTITIES.JPG)

### Identies federation

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/IDENTITIES_FEDERATION.JPG)

### Clusters

Modes supporting Unity Catalog
  - Single user
  - User isoaltion

Modes not supporting Unity Catalog
  - None (No security)
  - Table ACL only
  - Passthrough only

Security mode feature matrix:

![image](https://github.com/kevinbullock89/databricks/blob/main/Databricks%20Data%20Engineer%20Associate/Screenshots/CLUSTER_SECURITY.JPG)



## Sources: 
- https://learn.microsoft.com/en-us/azure/databricks/getting-started/overview
- https://sparkbyexamples.com/spark/types-of-clusters-in-databricks/
- https://hevodata.com/learn/databricks-clusters/
- https://docs.databricks.com/clusters/index.html 
- https://learn.microsoft.com/en-us/azure/databricks/delta/
- https://docs.databricks.com/sql/language-manual/delta-describe-history.html
- https://community.databricks.com/s/question/0D53f00001GHVPFCA5/whats-the-difference-between-a-global-view-and-a-temp-view
- https://docs.databricks.com/ingestion/auto-loader/index.html
- https://docs.databricks.com/delta-live-tables/index.html#publish-tables
- https://docs.databricks.com/api-explorer/workspace/jobs
- https://www.databricks.com/product/databricks-sql
- https://learn.microsoft.com/en-us/azure/databricks/lakehouse/data-objects
- https://database.guide/what-is-acid-in-databases/
