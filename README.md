# Steam Reviews — Big Data Analysis

Project for the [course name] exam — analysis of a dataset simulating a
big-data source, using a scalable pipeline based on Hadoop MapReduce, Spark
and MongoDB.

## Research hypothesis

Does the playtime at the moment of the review affect the probability that a
player recommends the game, and does this effect change depending on the
game's price?

The dataset has no genre column, so the game-level dimension used throughout
the analysis is price (grouped into free / low / mid / high buckets),
alongside the store's own `rating` label.

## Dataset

[Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam)
(Kaggle) — ~41M user reviews + metadata for ~50k games.

## Architecture

_(diagram and details in `docs/architecture.md`, coming soon)_

1. **Preprocessing** (pandas) — cleaning and joining `games.csv` and `recommendations.csv`
2. **Ingestion** (HDFS) — simulating data arriving as a stream
3. **MapReduce** (Hadoop Streaming) — recommendation rate per playtime bucket
4. **Spark** — statistical aggregations + Machine Learning (MLlib)
5. **MongoDB** — aggregate analysis queries

## Setup

Environment setup (Hadoop, Spark, MongoDB) is documented in
[`docs/setup.md`](docs/setup.md).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How to run

_(instructions updated as new phases are added)_

### Phase 0 — Preprocessing

1. Create a `data/` folder in the project root:
   ```bash
   mkdir data
   ```
2. Download `games.csv` and `recommendations.csv` from the Kaggle link above
   and place them inside `data/`.
3. Run the script:
   ```bash
   python src/preprocessing/preprocessing.py
   ```

#### Results

- `games.csv`: 50,872 rows after cleaning
- `recommendations.csv`: 500,000 rows sampled from the first 5,000,000
  records of the file
- Final merged dataset: 500,000 rows, 21 columns

Final columns:

```
app_id, helpful, funny, date, is_recommended, hours, user_id, review_id,
title, date_release, win, mac, linux, rating, positive_ratio, user_reviews,
price_final, price_original, discount, steam_deck, price_bucket
```

The output is saved as `data/steam_reviews_clean.csv`, ready to be used as
input for Phase 1 (HDFS ingestion).

### Phase 1 — Ingestion into HDFS

The cleaned dataset is split into small chunks and uploaded to HDFS one file
at a time, with a delay between uploads. This simulates records continuously
arriving from a big-data source instead of a single bulk load.

Make sure HDFS and YARN are running first (they do **not** restart
automatically after a reboot):

```bash
start-dfs.sh
start-yarn.sh
jps
```

`jps` must list `NameNode`, `DataNode`, `SecondaryNameNode`,
`ResourceManager` and `NodeManager`. Then run:

```bash
python src/ingestion/hdfs_streaming_ingestion.py
```

The script splits the CSV into `data/parts/`, wipes and recreates the HDFS
destination directory (so re-runs are idempotent), uploads the chunks one by
one, and verifies the result.

#### Parameters

| Parameter | Value | Note |
|---|---|---|
| `CHUNK_SIZE` | 1,000 rows | 500 chunks out of 500,000 rows |
| `DELAY_SECONDS` | 0.5 | artificial delay between uploads |
| `HDFS_PATH` | `/user/<user>/steam/streaming_input` | destination on HDFS |

The delay is an arbitrary parameter used only to make the stream observable.
In a real deployment the ingestion rate would be dictated by the source
system, not by the loader.

#### Results

```
[split] created 500 chunks of 1000 rows in 'data/parts'
[stream] uploading 500 chunks with 0.5s delay
[stream] ingestion complete
[verify] files on HDFS: 500 (expected 500)
[verify] total size: 67.9 M
```

### Phase 2 — MapReduce job

Computes the **recommendation rate per playtime bucket**: does a player who
spent more time in a game end up recommending it more often?

- `mapper.py` maps each review's `hours` into a discrete bucket and emits
  `bucket \t 1` if the review recommends the game, `bucket \t 0` otherwise
- `reducer.py` aggregates each bucket into
  `bucket \t total \t recommended \t rate`

Run it with HDFS and YARN up:

```bash
bash src/mapreduce/run_job.sh
```

The script clears the previous output directory (Hadoop refuses to write
into an existing one), submits the job via Hadoop Streaming, and prints the
result.

#### Results

| Playtime | Reviews | Recommended | Rate |
|---|---:|---:|---:|
| 0-1h | 6,626 | 2,326 | 35.1% |
| 1-5h | 18,484 | 10,625 | 57.5% |
| 5-20h | 65,580 | 53,913 | 82.2% |
| 20-100h | 158,660 | 137,418 | 86.6% |
| 100h+ | 250,650 | 218,341 | 87.1% |

The recommendation rate increases monotonically with playtime, which
supports the hypothesis. The relationship is far from linear: the sharpest
jump is between the 1-5h and 5-20h buckets (+25 points), after which the
curve flattens — beyond roughly 20 hours, additional playtime barely moves
the rate.

Playtime is capped at 999.9 hours in the source data, so the last bucket is
effectively 100-999.9h.

#### A note on parallelism

Hadoop creates one map task per input file, so the 500 ingested chunks
produce 500 splits. On a single-node pseudo-distributed cluster these run
essentially serially and container startup dominates the actual work. On a
real cluster the same 500 tasks would be spread across nodes and executed in
parallel — this is precisely the property the architecture is designed for.
