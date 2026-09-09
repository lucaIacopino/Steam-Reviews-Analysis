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

### Phase 3b — Predicting recommendations with MLlib

`notebooks/spark_ml.ipynb` turns the hypothesis into a supervised problem:
predict `is_recommended` from five features — `hours`, `price_final`,
`positive_ratio`, `year`, and the one-hot encoded `price_bucket`.

Split: 400,336 training rows / 99,664 test rows, seed 42.

#### Results

| Model | AUC | Accuracy | F1 |
|---|---:|---:|---:|
| Logistic regression | 0.720 | 0.842 | 0.790 |
| Random forest | 0.761 | 0.853 | 0.801 |
| *Always predict "recommended"* | — | *0.845* | — |

The accuracy column is the least informative one here. 84.5% of reviews are
positive, so a model that blindly predicts "recommended" already scores
0.845 — which the logistic regression fails to beat, and the random forest
beats by less than a point.

The confusion matrix for the random forest shows what is happening:

| | predicted 0 | predicted 1 |
|---|---:|---:|
| **actual 0** | 1,410 | 14,076 |
| **actual 1** | 622 | 83,556 |

Of the 15,486 genuinely negative reviews in the test set, the model
identifies 1,410 — about 9%. At the default 0.5 threshold it plays the
majority class almost all the time.

An AUC of 0.761 nonetheless says the model ranks reviews meaningfully: the
signal exists, but the decision threshold is where it gets lost. Moving the
threshold below 0.5 would recover negative-class recall at the cost of
precision, which is the usual trade-off under class imbalance.

#### Feature importances

| Feature | Importance |
|---|---:|
| hours | 0.441 |
| positive_ratio | 0.385 |
| price_final | 0.077 |
| price = free | 0.061 |
| year | 0.033 |
| price = mid | 0.002 |
| price = high | 0.001 |

Playtime is the strongest single predictor, which is the expected answer to
the original hypothesis. The more interesting result is second place: the
game's overall positive ratio on the store carries almost as much weight.
What the crowd thinks of a game predicts an individual verdict nearly as
well as how long that individual played it.

Price contributes little once the free/paid distinction is accounted for —
consistent with phase 3a, where free games behaved differently from every
paid tier while the paid tiers largely resembled one another.

### Phase 4 — MongoDB

`notebooks/mongodb_queries.ipynb` loads the phase 3 results into MongoDB and
queries them.

Spark writes through the official connector
(`org.mongodb.spark:mongo-spark-connector_2.12:10.3.0`), so the data goes
from HDFS to MongoDB without passing through the driver — the same code path
would hold at any volume. The connector JAR is fetched automatically on the
first run, which needs an internet connection.

Requires HDFS and `mongod` to be running:

```bash
start-dfs.sh
start-yarn.sh
systemctl is-active mongod
```

#### Collections

| Collection | Documents |
|---|---:|
| `recommendation_by_playtime` | 5 |
| `recommendation_by_playtime_price` | 20 |
| `hours_by_outcome` | 8 |
| `recommendation_by_year` | 10 |
| `model_metrics` | 2 |
| `feature_importances` | 7 |
| `reviews` | 50,097 |

The six aggregates come straight from the Parquet files written in phases 3a
and 3b. `reviews` holds a 10% sample of the raw review data, so the queries
have something to work on beyond pre-computed summaries.

#### Query 1 — invested but unconvinced

Players with over 100 hours who still did not recommend the game, ranked by
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

Almost every entry is a long-running multiplayer or live-service title. These
are the cases the aggregate rates smooth over: players with hundreds of hours
invested who turn negative anyway, and whose reviews other users find
unusually useful.

#### Query 2 — recommendation rate by price bucket

| Price bucket | Reviews | Rate | Avg hours |
|---|---:|---:|---:|
| low | 2,400 | 92.4% | 132.9 |
| mid | 16,819 | 88.8% | 212.6 |
| high | 20,932 | 85.7% | 176.9 |
| free | 9,946 | 73.2% | 248.6 |

The ordering matches phase 3a: cheap games do best, free games worst despite
having by far the highest average playtime.

#### Query 3 — recommendation rate by year

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

Computed on the 10% sample, these figures land within roughly a point of
what Spark computed over all 500,000 rows — the same dip through 2017 and
the same recovery afterwards. Two different engines over two different slices
of the data agree, which is a reasonable check that neither pipeline is
distorting the result.
