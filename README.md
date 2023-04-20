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

Sources: 
- https://sparkbyexamples.com/spark/types-of-clusters-in-databricks/
- https://hevodata.com/learn/databricks-clusters/
- https://docs.databricks.com/clusters/index.html 
