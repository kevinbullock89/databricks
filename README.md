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

### Reviewing Delta Lake Transactions

Returns provenance information, including the operation, user, and so on, for each write to a table. Table history is retained for 30 days.

```sh
DESCRIBE HISTORY students
```

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

Sources: 
- https://sparkbyexamples.com/spark/types-of-clusters-in-databricks/
- https://hevodata.com/learn/databricks-clusters/
- https://docs.databricks.com/clusters/index.html 
- https://learn.microsoft.com/en-us/azure/databricks/delta/
- https://docs.databricks.com/sql/language-manual/delta-describe-history.html
