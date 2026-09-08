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

### Phase 3a — Spark analysis

`notebooks/spark_analysis.ipynb` reads the ingested chunks back from HDFS and
extends the MapReduce result along two dimensions it does not cover: price
and time.

Run it with HDFS up:

```bash
jupyter notebook
```

#### Cross-check against MapReduce

Before adding anything new, the notebook recomputes the phase 2 aggregation
in Spark and joins it against the MapReduce output. All five buckets match
exactly (`rate_diff = 0.0`), confirming that the two engines agree on the
same input.

#### Playtime and price

Recommendation rate by playtime bucket and price bucket:

| Playtime | free | low (<€10) | mid (€10-30) | high (>€30) |
|---|---:|---:|---:|---:|
| 0-1h | 37.8% | 55.0% | 36.2% | 30.4% |
| 1-5h | 53.3% | 85.9% | 61.4% | 52.5% |
| 5-20h | 72.2% | 93.6% | 86.2% | 81.2% |
| 20-100h | 75.8% | 94.1% | 91.0% | 86.4% |
| 100h+ | 74.9% | 91.5% | 90.8% | 90.6% |

The effect of playtime is real but its size depends heavily on price. On
expensive games the rate climbs 60 points across the range (30% to 91%):
a player who paid a lot and bounced within the hour is the harshest
reviewer there is. Cheap games start much higher (55%) and gain 37 points,
suggesting a low price buys a good deal of tolerance.

Free games are the exception to the overall pattern. They plateau around
75%, well below the ~91% that paid games reach at high playtime, and are the
only category where the rate stops rising (20-100h slightly outperforms
100h+). Hundreds of hours in a free-to-play title evidently does not imply
the same satisfaction as the same hours in a purchased one.

The 0-1h row for cheap games rests on only 211 reviews, so its 55% is the
least reliable figure in the table.

#### Playtime by outcome

Average and median hours, split by whether the review recommends the game:

| Price | Outcome | Reviews | Avg hours | Median hours |
|---|---|---:|---:|---:|
| free | not recommended | 26,515 | 227.9 | 120.8 |
| free | recommended | 72,376 | 253.1 | 147.7 |
| low | not recommended | 1,803 | 155.1 | 57.2 |
| low | recommended | 21,989 | 132.1 | 51.1 |
| mid | not recommended | 19,011 | 185.6 | 68.9 |
| mid | recommended | 148,547 | 219.5 | 121.5 |
| high | not recommended | 30,048 | 117.8 | 39.4 |
| high | recommended | 179,711 | 185.1 | 93.0 |

The gap is widest on expensive games (median 39h against 93h) and reverses
on cheap ones, the only bucket where players who did not recommend had
logged more hours than those who did.

#### Trend over time

| Year | Reviews | Rate | Avg hours |
|---|---:|---:|---:|
| 2013 | 2,475 | 95.6% | 273.1 |
| 2014 | 9,752 | 92.8% | 264.0 |
| 2015 | 13,335 | 80.2% | 274.9 |
| 2016 | 22,689 | 76.6% | 252.4 |
| 2017 | 26,314 | 72.5% | 270.6 |
| 2018 | 24,618 | 76.9% | 265.0 |
| 2019 | 39,449 | 87.5% | 269.1 |
| 2020 | 90,728 | 88.3% | 216.9 |
| 2021 | 105,117 | 87.9% | 194.3 |
| 2022 | 165,148 | 83.4% | 141.6 |

The rate falls through the mid-2010s, bottoms out in 2017 and recovers from
2019 onwards. Average playtime per review declines steadily after 2019,
which is expected: recent reviews have had less time to accumulate hours.
That correlation between year and playtime matters for the modelling step —
the two variables carry overlapping information.

#### Persisted output

The four aggregates are written back to HDFS as Parquet under
`/user/<user>/steam/output/analysis/`, ready to be loaded into MongoDB in
phase 4. Parquet is columnar and compressed, so it scans far more cheaply
than CSV once the data grows.
