"""
PHASE 1 - Data ingestion into HDFS (simulated streaming)

The cleaned dataset produced in phase 0 is split into small chunks and then
uploaded to HDFS one at a time, with a delay between uploads, to simulate
data continuously arriving from a big-data source.
"""

import getpass
import os
import subprocess

import pandas as pd

# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
INPUT_CSV = "data/steam_reviews_clean.csv"
PARTS_DIR = "data/parts"

# Number of rows per chunk. Small chunks make the ingestion look like a
# realistic stream of incoming records instead of a single bulk load.
CHUNK_SIZE = 1000

# HDFS destination. Port 9000 is the one configured in core-site.xml.
HDFS_USER = getpass.getuser()
HDFS_PATH = f"hdfs://localhost:9000/user/{HDFS_USER}/steam/streaming_input"


def split_into_chunks(input_csv: str, parts_dir: str, chunk_size: int) -> int:
    """Split the cleaned CSV into smaller files, one per chunk.

    Each chunk keeps the header so that it can be read independently
    by downstream jobs (MapReduce, Spark).

    Returns the number of chunks created.
    """
    os.makedirs(parts_dir, exist_ok=True)

    count = 0
    for i, chunk in enumerate(pd.read_csv(input_csv, chunksize=chunk_size)):
        out_file = os.path.join(parts_dir, f"part_{i}.csv")
        chunk.to_csv(out_file, index=False, header=True)
        count += 1

    print(f"[split] created {count} chunks of {chunk_size} rows in '{parts_dir}'")
    return count


def prepare_hdfs_dir(hdfs_path: str, clean: bool = True):
    """Create the HDFS destination directory, optionally wiping it first.

    Wiping keeps re-runs idempotent: without it, chunks from a previous run
    would still be there and would be picked up by the downstream jobs.
    """
    if clean:
        # -f avoids an error if the directory does not exist yet
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", hdfs_path],
            check=False,
        )
        print(f"[hdfs] cleaned '{hdfs_path}'")

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_path], check=True)
    print(f"[hdfs] ready: '{hdfs_path}'")


if __name__ == "__main__":
    split_into_chunks(INPUT_CSV, PARTS_DIR, CHUNK_SIZE)
    prepare_hdfs_dir(HDFS_PATH)
