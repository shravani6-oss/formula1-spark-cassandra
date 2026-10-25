# Viva questions and answers

**1. What is Apache Spark?**
An open-source engine for distributed data processing. It splits data into partitions, processes them in parallel on many cores or machines, and keeps intermediate data in memory where possible. Here it runs in `local[*]` mode (all cores of one computer), but the same code would run on a cluster.

**2. What is the difference between an RDD and a DataFrame?**
An RDD is Spark's low-level collection of objects: no schema, you write the functions yourself and Spark cannot look inside them. A DataFrame is a table with named, typed columns; Spark's Catalyst optimiser can reorder and push down operations, and Tungsten stores data compactly. DataFrames (and Spark SQL) are faster and easier for tabular data such as this project's CSV files, so this project uses DataFrames. Every DataFrame still sits on RDDs underneath.

**3. What is PySpark?**
The Python API for Spark. Python code builds a plan of DataFrame operations; a JVM (Java) process executes it. That is why Java must be installed.

**4. What are lazy evaluation, transformations and actions?**
Transformations (`filter`, `join`, `groupBy`) only build a plan. Nothing runs until an action (`count`, `collect`, `toPandas`, `show`) asks for a result. `cache()` keeps a computed DataFrame in memory for reuse (the dashboard caches the joined results and the cleaned lap times).

**5. What is distributed processing?**
Splitting one big job into many small tasks that run in parallel on different partitions of the data, then combining the partial results (e.g. each partition counts wins, then Spark adds the counts). It lets you handle data bigger than one machine and finish faster.

**6. What is Apache Cassandra?**
A distributed, wide-column NoSQL database built for high write throughput and availability with no single point of failure. Data is spread over nodes by a hash of the partition key. There are no joins; you design each table around the query you need.

**7. What is a keyspace?**
The top-level container for tables in Cassandra, like a database/schema in SQL. It also stores the replication settings. This project uses the keyspace `f1_analysis` with `SimpleStrategy` and replication factor 1 (single development node).

**8. What is a partition key?**
The column(s) whose hash decides which node stores a row. All rows with the same partition key live together, so a query that gives the partition key is fast. Example: `results_by_race` has partition key `race_id`, so "all results of one race" reads one partition.

**9. What is a clustering column?**
A column that sorts rows inside a partition and makes the primary key unique. Example: in `lap_times_by_race_driver` the key is `((race_id, driver_id), lap)`: the partition is one driver in one race and `lap` orders the laps.

**10. Why does the project store results twice (`results_by_race` and `results_by_driver`)?**
Cassandra cannot join or efficiently filter on arbitrary columns, so you duplicate data into one table per query ("query-first" modelling). Storage is cheap; scans across partitions are not.

**11. How do you keep relationships without joins?**
By storing identifiers (`driver_id`, `constructor_id`, `race_id`) in every table. Spark joins the tables on those identifiers when it analyses the data.

**12. Why is the ingestion repeatable?**
Every row is written with `INSERT` on its full primary key. In Cassandra an insert is an upsert, so repeating it overwrites the same logical row instead of adding a duplicate. The script never truncates tables, and it logs each load in `ingestion_log`.

**13. How does this project use Spark and Cassandra together?**
CSV → validation and ingestion script → Cassandra tables → Spark reads them through the Spark Cassandra Connector as DataFrames → Spark joins and aggregates (driver performance, constructor and season statistics, pit stops per season) → results go to Streamlit/Plotly. The dashboard also compares the totals from Cassandra with the totals from the CSV pipeline as a control check.

**14. What does the Spark Cassandra Connector do?**
It lets Spark read and write Cassandra tables as DataFrames (`format("org.apache.spark.sql.cassandra")`). It splits a table by token ranges so Spark tasks read different partitions in parallel, and it pushes down filters on partition keys. Its version must match Spark and the Scala version (Spark 3.5 + Scala 2.12 → connector 3.5.x for 2.12).

**15. Why not use `COUNT(*)` on the lap-times table?**
It scans every partition on the coordinator and normally exceeds Cassandra's read timeout for ~885k rows. The dashboard reads the count from the ingestion log or lets Spark count it in parallel.

**16. What did you do about missing and dirty data?**
The CSVs use `\N` for missing values (read as null). Duplicate lap keys (1,979 rows) keep the first row; pit stops without a duration are dropped; `duration` is text, so the numeric `milliseconds` column is used; stops over 120 s are excluded from averages; unclassified or disqualified results do not count as wins or podiums.

**17. Why use Streamlit caching?**
Spark jobs and database queries are slow compared with a page redraw. `st.cache_data` stores results (with a time-to-live), `st.cache_resource` keeps the one SparkSession, and the Refresh buttons clear the caches.
