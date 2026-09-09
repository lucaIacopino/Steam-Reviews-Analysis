# Steam Reviews — Big Data Analysis

Project for the [course name] exam. The dataset is treated as if it came from
a big-data source, and the pipeline is built with tools that would still work
if it really did: Hadoop MapReduce, Spark and MongoDB.

## Contents

- [Research question](#research-question)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Setup](#setup)
- [Phase 0 — Preprocessing](#phase-0--preprocessing)
- [Phase 1 — Ingestion into HDFS](#phase-1--ingestion-into-hdfs)
- [Phase 2 — MapReduce job](#phase-2--mapreduce-job)
- [Phase 3a — Spark analysis](#phase-3a--spark-analysis)
- [Phase 3b — Machine learning](#phase-3b--machine-learning)
- [Phase 4 — MongoDB](#phase-4--mongodb)
- [Summary of findings](#summary-of-findings)

## Research question

Does the time a player spent in a game affect whether they recommend it? And
does that effect change with the price of the game?

The dataset has no genre column, so price is used as the game-level variable
(grouped into free / low / mid / high).

## Dataset

[Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam)
(Kaggle): about 41M user reviews and metadata for about 50k games.

## Architecture

```
Kaggle CSVs -> pandas -> HDFS -> MapReduce + Spark -> Parquet -> MongoDB
```

Details and the scalability discussion are in
[`docs/architecture.md`](docs/architecture.md).

## Setup

How to install Hadoop, Spark and MongoDB is in
[`docs/setup.md`](docs/setup.md).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Hadoop does not start by itself after a reboot:

```bash
start-dfs.sh
start-yarn.sh
jps
```

`jps` must show `NameNode`, `DataNode`, `SecondaryNameNode`,
`ResourceManager` and `NodeManager`.

## Phase 0 — Preprocessing

Cleans `games.csv` and `recommendations.csv` and joins them.

1. Create a `data/` folder:
   ```bash
   mkdir data
   ```
2. Download the two CSV files from the Kaggle link above into `data/`.
3. Run:
   ```bash
   python src/preprocessing/preprocessing.py
   ```

### Results

- `games.csv`: 50,872 rows after cleaning
- `recommendations.csv`: 500,000 rows sampled from the first 5,000,000
- Merged output: 500,000 rows, 21 columns

Columns:

```
app_id, helpful, funny, date, is_recommended, hours, user_id, review_id,
title, date_release, win, mac, linux, rating, positive_ratio, user_reviews,
price_final, price_original, discount, steam_deck, price_bucket
```

The result is saved as `data/steam_reviews_clean.csv`.

## Phase 1 — Ingestion into HDFS

The clean CSV is split into small chunks and uploaded to HDFS one file at a
time, with a short pause between uploads. This imitates data arriving bit by
bit instead of one big load.

```bash
python src/ingestion/hdfs_streaming_ingestion.py
```

The script splits the CSV into `data/parts/`, clears and recreates the HDFS
folder (so it can be run again safely), uploads the chunks, and checks the
result.

### Parameters

| Parameter | Value | Note |
|---|---|---|
| `CHUNK_SIZE` | 1,000 rows | 500 chunks from 500,000 rows |
| `DELAY_SECONDS` | 0.5 | pause between uploads |
| `HDFS_PATH` | `/user/<user>/steam/streaming_input` | destination |

The delay is only there to make the stream visible. In a real system the
speed would depend on the source.

### Results

```
[split] created 500 chunks of 1000 rows in 'data/parts'
[stream] uploading 500 chunks with 0.5s delay
[stream] ingestion complete
[verify] files on HDFS: 500 (expected 500)
[verify] total size: 67.9 M
```

## Phase 2 — MapReduce job

Computes the recommendation rate for each playtime bucket.

- `mapper.py` puts each review's `hours` into a bucket and emits
  `bucket \t 1` if the review recommends the game, `bucket \t 0` if not
- `reducer.py` sums each bucket and outputs
  `bucket \t total \t recommended \t rate`

```bash
bash src/mapreduce/run_job.sh
```

### Results

| Playtime | Reviews | Recommended | Rate |
|---|---:|---:|---:|
| 0-1h | 6,626 | 2,326 | 35.1% |
| 1-5h | 18,484 | 10,625 | 57.5% |
| 5-20h | 65,580 | 53,913 | 82.2% |
| 20-100h | 158,660 | 137,418 | 86.6% |
| 100h+ | 250,650 | 218,341 | 87.1% |

The rate goes up with playtime, which supports the hypothesis. But the
relation is not linear: the big jump is between 1-5h and 5-20h (+25 points),
and after 20 hours the curve is almost flat.

Playtime is capped at 999.9 hours in the source data, so the last bucket is
really 100-999.9h.

### Note on parallelism

Hadoop creates one map task per file, so the 500 chunks give 500 splits. On
one machine they run almost one after the other. On a real cluster they would
run in parallel on different nodes.

## Phase 3a — Spark analysis

`notebooks/spark_analysis.ipynb` reads the chunks back from HDFS and adds two
things MapReduce did not cover: price and time.

### Cross-check against MapReduce

The notebook computes the phase 2 result again in Spark and joins it with the
MapReduce output. All five buckets match exactly (`rate_diff = 0.0`), so both
pipelines agree.

### Playtime and price

| Playtime | free | low (<€10) | mid (€10-30) | high (>€30) |
|---|---:|---:|---:|---:|
| 0-1h | 37.8% | 55.0% | 36.2% | 30.4% |
| 1-5h | 53.3% | 85.9% | 61.4% | 52.5% |
| 5-20h | 72.2% | 93.6% | 86.2% | 81.2% |
| 20-100h | 75.8% | 94.1% | 91.0% | 86.4% |
| 100h+ | 74.9% | 91.5% | 90.8% | 90.6% |

Playtime matters, but how much depends on the price. On expensive games the
rate grows by 60 points (30% to 91%): a player who paid a lot and quit within
an hour is the harshest reviewer. Cheap games start much higher (55%) and
grow by 37 points, so a low price seems to buy some patience.

Free games are the exception. They stop around 75%, well below the ~91% of
paid games, and they are the only group where the rate stops growing.

The 0-1h cell for cheap games has only 211 reviews, so that 55% is the least
reliable number in the table.

### Playtime by outcome

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

The gap is largest on expensive games (median 39h vs 93h). On cheap games it
is the other way round: players who did not recommend had played more.

### Trend over time

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

The rate drops until 2017 and then recovers. Average playtime goes down after
2019, which makes sense: newer reviews had less time to add hours. So year
and playtime carry similar information, which matters for the model.

### Saved output

The four aggregates are written back to HDFS as Parquet under
`/user/<user>/steam/output/analysis/`.

## Phase 3b — Machine learning

`notebooks/spark_ml.ipynb` turns the question into a prediction problem:
predict `is_recommended` from `hours`, `price_final`, `positive_ratio`,
`year` and the encoded `price_bucket`.

Split: 400,336 training rows / 99,664 test rows, seed 42.

### Results

| Model | AUC | Accuracy | F1 |
|---|---:|---:|---:|
| Logistic regression | 0.720 | 0.842 | 0.790 |
| Random forest | 0.761 | 0.853 | 0.801 |
| *Always predict "recommended"* | — | *0.845* | — |

Accuracy is the least useful column here. 84.5% of reviews are positive, so
always answering "recommended" already gives 0.845. The logistic regression
does not even reach that, and the random forest beats it by less than a
point.

The confusion matrix for the random forest shows why:

| | predicted 0 | predicted 1 |
|---|---:|---:|
| **actual 0** | 1,410 | 14,076 |
| **actual 1** | 622 | 83,556 |

Out of 15,486 negative reviews in the test set, the model finds 1,410, about
9%. At the default 0.5 threshold it almost always answers "recommended".

The AUC of 0.761 still shows the model ranks reviews in a useful way. The
signal is there, but the threshold hides it. A lower threshold would find
more negative reviews and make more mistakes on the positive ones.

### Feature importances

| Feature | Importance |
|---|---:|
| hours | 0.441 |
| positive_ratio | 0.385 |
| price_final | 0.077 |
| price = free | 0.061 |
| year | 0.033 |
| price = mid | 0.002 |
| price = high | 0.001 |

Playtime is the strongest single feature, as expected. The interesting part
is second place: the game's positive ratio on the store is almost as strong.
What other players think of a game predicts one player's verdict almost as
well as how long that player played.

Price adds little once free vs paid is taken into account. This matches
phase 3a, where free games behaved differently and the paid tiers looked
similar to each other.

## Phase 4 — MongoDB

`notebooks/mongodb_queries.ipynb` loads the results into MongoDB and queries
them.

Spark writes through the official connector
(`org.mongodb.spark:mongo-spark-connector_2.12:10.3.0`), so the data goes
from HDFS to MongoDB without passing through the driver. The connector is
downloaded on the first run, so an internet connection is needed.

MongoDB must be running:

```bash
systemctl is-active mongod
```

### Collections

| Collection | Documents |
|---|---:|
| `recommendation_by_playtime` | 5 |
| `recommendation_by_playtime_price` | 20 |
| `hours_by_outcome` | 8 |
| `recommendation_by_year` | 10 |
| `model_metrics` | 2 |
| `feature_importances` | 7 |
| `reviews` | 50,097 |

The six aggregates come from the Parquet files. `reviews` holds a 10% sample
of the raw data, so the queries have real records to work on.

### Query 1 — many hours, still negative

Players with more than 100 hours who did not recommend the game, sorted by
how many people found the review helpful:

| Title | Hours | Helpful | Store positive ratio |
|---|---:|---:|---:|
| PUBG: BATTLEGROUNDS | 348.8 | 5,126 | 57 |
| Grand Theft Auto V | 229.2 | 3,521 | 86 |
| Mount & Blade II: Bannerlord | 150.6 | 2,859 | 87 |
| DayZ | 447.4 | 1,737 | 74 |
| PUBG: BATTLEGROUNDS | 219.5 | 1,729 | 57 |
| Team Fortress 2 | 284.0 | 1,151 | 93 |
| Counter-Strike: Global Offensive | 844.7 | 916 | 88 |
| War Thunder | 439.9 | 741 | 75 |
| The Elder Scrolls V: Skyrim SE | 853.7 | 693 | 94 |
| The Sims™ 3 | 303.7 | 663 | 86 |

Almost all of them are multiplayer or live-service games. These are the cases
the average rate hides: players with hundreds of hours who still turn
negative, and whose reviews other users find useful.

### Query 2 — rate by price bucket

| Price bucket | Reviews | Rate | Avg hours |
|---|---:|---:|---:|
| low | 2,400 | 92.4% | 132.9 |
| mid | 16,819 | 88.8% | 212.6 |
| high | 20,932 | 85.7% | 176.9 |
| free | 9,946 | 73.2% | 248.6 |

Same order as phase 3a: cheap games do best, free games worst even though
they have the highest average playtime.

### Query 3 — rate by year

| Year | Reviews | Rate | Avg hours |
|---|---:|---:|---:|
| 2013 | 248 | 94.4% | 284.1 |
| 2014 | 981 | 93.0% | 260.9 |
| 2015 | 1,314 | 80.9% | 288.4 |
| 2016 | 2,274 | 77.3% | 255.5 |
| 2017 | 2,683 | 72.4% | 277.6 |
| 2018 | 2,431 | 77.3% | 270.1 |
| 2019 | 3,981 | 87.9% | 261.1 |
| 2020 | 9,139 | 87.9% | 213.7 |
| 2021 | 10,392 | 88.3% | 193.1 |
| 2022 | 16,617 | 83.2% | 142.8 |

These numbers come from the 10% sample, and they are within about one point
of what Spark computed on all 500,000 rows: same drop until 2017, same
recovery after. Two different engines on two different slices of the data
agree.

## Summary of findings

1. Playtime and recommendation go together, but not in a straight line. Most
   of the effect happens in the first 20 hours.
2. The effect is much stronger on expensive games than on cheap ones.
3. Free games behave differently from every paid group: high playtime, low
   recommendation rate.
4. A game's reputation on the store predicts a single review almost as well
   as that player's own playtime.
5. Predicting single reviews is hard because the classes are unbalanced: the
   model ranks well (AUC 0.76) but at the default threshold it mostly repeats
   the majority answer.
