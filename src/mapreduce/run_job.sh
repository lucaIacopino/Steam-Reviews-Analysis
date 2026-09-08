#!/bin/bash
#
# PHASE 2 - Run the MapReduce job on Hadoop Streaming
#
# Computes the recommendation rate per playtime bucket over the data
# ingested into HDFS in phase 1.
#
# Usage:
#   bash src/mapreduce/run_job.sh
#
# Requires HDFS and YARN to be running (see docs/setup.md).

set -e

HDFS_USER=$(whoami)
INPUT_PATH="/user/${HDFS_USER}/steam/streaming_input"
OUTPUT_PATH="/user/${HDFS_USER}/steam/output/recommendation_by_playtime"

MAPPER="src/mapreduce/mapper.py"
REDUCER="src/mapreduce/reducer.py"

# Hadoop refuses to run if the output directory already exists
echo "[job] removing previous output (if any)"
hdfs dfs -rm -r -f -skipTrash "${OUTPUT_PATH}"

echo "[job] submitting MapReduce job"
hadoop jar "${HADOOP_STREAMING}" \
  -files "${MAPPER}","${REDUCER}" \
  -input "${INPUT_PATH}" \
  -output "${OUTPUT_PATH}" \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py"

echo "[job] done. Result:"
echo "bucket        total   recommended   rate"
hdfs dfs -cat "${OUTPUT_PATH}/part-*"
