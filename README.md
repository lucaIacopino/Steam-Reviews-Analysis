# Steam Reviews — Big Data Analysis

Project for the [course name] exam — analysis of a dataset simulating a
big-data source, using a scalable pipeline based on Hadoop MapReduce, Spark
and MongoDB.

## Research hypothesis

Does the playtime at the moment of the review affect the probability that a
player recommends the game, and does this effect change depending on the
game's genre/price?

## Dataset

[Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam)
(Kaggle) — ~41M user reviews + metadata for ~50k games.

## Architecture

_(diagram and details in `docs/architecture.md`, coming soon)_

1. **Preprocessing** (pandas) — cleaning and joining `games.csv` and `recommendations.csv`
2. **Ingestion** (HDFS) — simulating data arriving as a stream
3. **MapReduce** (Hadoop Streaming) — text analysis of the reviews
4. **Spark** — statistical aggregations + Machine Learning (MLlib)
5. **MongoDB** — aggregate analysis queries

## Setup

```bash
python -m venv venv
source venv/bin/activate
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
   python src/preprocessing/fase0_preprocessing.py
   ```

#### Results

Running the script on the full Kaggle dataset produced:

- `games.csv`: 50,872 rows after cleaning
- `recommendations.csv`: 5,000,000 rows read (chunked), 500,000 rows kept
  after cleaning and sampling
- Final merged dataset: 500,000 rows, 21 columns

Final columns:

```
app_id, helpful, funny, date, is_recommended, hours, user_id, review_id,
title, date_release, win, mac, linux, rating, positive_ratio, user_reviews,
price_final, price_original, discount, steam_deck, price_bucket
```

The output is saved as `data/steam_reviews_clean.csv`, ready to be used as
input for Phase 1 (HDFS ingestion).
