# Architecture

## Pipeline

```
  Kaggle CSVs               games.csv + recommendations.csv (~41M rows)
       |
       |  pandas: clean, join, sample
       v
  steam_reviews_clean.csv   500,000 rows
       |
       |  split into 500 chunks, uploaded one at a time
       v
  HDFS  /steam/streaming_input/   (500 CSV files)
       |                          |
       |  MapReduce               |  Spark
       v                          v
  rate per playtime         analysis + MLlib models
       |                          |
       +------> cross-check <-----+
                                  |
                                  |  write Parquet
                                  v
  HDFS  /steam/output/analysis/   (6 datasets)
                                  |
                                  |  mongo-spark-connector
                                  v
  MongoDB  steam_reviews          6 aggregates + reviews sample
                                  |
                                  v
                             pymongo queries
```

## Why each tool is used

**Ingestion is incremental.** The CSV is split into 500 chunks and sent to
HDFS one at a time, not all at once. This is how real data arrives: a bit at
a time, without a fixed batch window. It also helps the jobs later, because
Hadoop creates one map task per file.

**MapReduce and Spark do overlap, on purpose.** MapReduce computes the
recommendation rate per playtime bucket. Spark computes the same thing again
and compares the two results. If they match, both pipelines are correct.
After that, Spark does the work MapReduce is not good at: grouping by two
keys, time analysis, and training models.

**Parquet sits between Spark and MongoDB.** It is a column-based format, so
a job that needs two columns out of six reads only those two. It is also
compressed. And if MongoDB is rebuilt, the results are still on HDFS.

**MongoDB is the serving layer.** It does not compute anything that Spark
could not compute faster. What it gives is a database an application can
query directly, with a flexible schema and the aggregation pipeline.

## How this would scale

The project uses 500,000 rows (about 68 MB on HDFS). The full Kaggle dataset
has ~41M rows. Nothing in the design would have to be replaced to handle
that: what changes is the configuration and the hardware.

### Storage

HDFS here runs on one node with `dfs.replication = 1`. On a real cluster the
replication would be 3, so losing a node costs nothing: every block also
exists somewhere else. Files are split into blocks and spread across
machines, so you add capacity by adding machines.

One thing to watch is the number of files. HDFS keeps all file metadata in
the NameNode memory, so many small files is a known problem. The 500 small
chunks are fine at this size, but at scale the ingestion layer would write
larger files instead.

### Compute

The MapReduce job already runs 500 map tasks. On one node they run almost
one after the other, because there is only one machine. On a cluster the same
500 tasks would run in parallel on different nodes, with no change to the
code. The job is already parallel — it just has nowhere to spread.

Spark runs with `master("local[*]")`, using the local cores. Pointing it to
YARN or Kubernetes is only a configuration change. The operations used
(`groupBy`, `join`, `agg`, the MLlib pipeline) are all distributed.

Two things to keep in mind with more data. `cache()` works here because
500k rows fit in memory; with 41M rows it would spill to disk, so it would be
used more carefully. And `toPandas()` is only safe because the aggregates are
a few rows — collecting a large DataFrame to the driver does not scale, which
is why it is used only after grouping.

### Serving

MongoDB runs as a single instance. To scale it, you shard the collection:
the documents are split across several servers using a shard key. For
`reviews`, `app_id` would work well, because queries about one game are
common.

Indexes also become important. The phase 4 queries scan 50,000 documents,
which is fast without an index. With hundreds of millions, an index on the
fields used in `$match` would be needed.

Everything after ingestion — MapReduce, Spark, MLlib, the MongoDB load —
scales by adding nodes.
