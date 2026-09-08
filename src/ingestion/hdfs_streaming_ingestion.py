"""
PHASE 1 - Data ingestion into HDFS (simulated streaming)

The cleaned dataset produced in phase 0 is split into small chunks and then
uploaded to HDFS one at a time, with a delay between uploads, to simulate
data continuously arriving from a big-data source.
"""

import os

import pandas as pd

# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
INPUT_CSV = "data/steam_reviews_clean.csv"
PARTS_DIR = "data/parts"

# Number of rows per chunk. Small chunks make the ingestion look like a
# realistic stream of incoming records instead of a single bulk load.
CHUNK_SIZE = 1000


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


if __name__ == "__main__":
    split_into_chunks(INPUT_CSV, PARTS_DIR, CHUNK_SIZE)
