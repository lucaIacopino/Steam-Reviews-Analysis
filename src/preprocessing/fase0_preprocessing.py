"""
FASE 0 - Preprocessing del dataset "Game Recommendations on Steam"
Dataset: https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam

Obiettivo:
1. Caricare games.csv e recommendations.csv
2. Pulire i dati (null, tipi)
3. Fare il join tra le due tabelle (arricchire ogni recensione con i metadati del gioco)
4. Campionare un sottoinsieme gestibile in locale
5. Esportare un CSV pulito, pronto per l'ingestion su HDFS (Fase 1)
"""

import pandas as pd

# --------------------------------------------------------------------------
# CONFIGURAZIONE - modifica questi path in base a dove hai salvato i file
# --------------------------------------------------------------------------
GAMES_PATH = "data/games.csv"
RECOMMENDATIONS_PATH = "data/recommendations.csv"
OUTPUT_PATH = "data/steam_reviews_clean.csv"

SAMPLE_SIZE = 500_000
RANDOM_SEED = 42


def load_games(path: str) -> pd.DataFrame:
    """Carica e pulisce i metadati dei giochi."""
    df = pd.read_csv(path)

    print(f"[games.csv] righe iniziali: {len(df)}")

    df = df.dropna(subset=["app_id", "title", "price_final"])

    df["price_final"] = df["price_final"].astype(float)
    df["date_release"] = pd.to_datetime(df["date_release"], errors="coerce")

    def price_bucket(p):
        if p == 0:
            return "free"
        elif p < 10:
            return "low"
        elif p < 30:
            return "mid"
        else:
            return "high"

    df["price_bucket"] = df["price_final"].apply(price_bucket)

    print(f"[games.csv] righe dopo pulizia: {len(df)}")
    return df


def load_recommendations(path: str, sample_size: int, seed: int) -> pd.DataFrame:
    """Carica un campione delle recensioni e le pulisce."""
    chunks = []
    chunk_size = 200_000
    total_read = 0

    for chunk in pd.read_csv(path, chunksize=chunk_size):
        chunks.append(chunk)
        total_read += len(chunk)
        if total_read >= sample_size * 10:
            break

    df = pd.concat(chunks, ignore_index=True)
    print(f"[recommendations.csv] righe lette: {len(df)}")

    df = df.dropna(subset=["app_id", "is_recommended", "hours"])

    df["hours"] = df["hours"].astype(float)
    df["is_recommended"] = df["is_recommended"].astype(bool)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df[(df["hours"] >= 0) & (df["hours"] < 10000)]

    if len(df) > sample_size:
        df = df.sample(n=sample_size, random_state=seed)

    print(f"[recommendations.csv] righe dopo pulizia/sampling: {len(df)}")
    return df


def join_and_export(games: pd.DataFrame, reviews: pd.DataFrame, output_path: str):
    """Unisce le due tabelle e salva il risultato."""
    merged = reviews.merge(games, on="app_id", how="inner")

    print(f"[merge] righe finali: {len(merged)}")
    print(f"[merge] colonne: {list(merged.columns)}")

    # Ordina per data per rendere più naturale la simulazione di streaming
    # nella Fase 1 (dati che "arrivano" in ordine cronologico)
    merged = merged.sort_values("date")

    merged.to_csv(output_path, index=False)
    print(f"Salvato dataset pulito in: {output_path}")


def main():
    games = load_games(GAMES_PATH)
    reviews = load_recommendations(RECOMMENDATIONS_PATH, SAMPLE_SIZE, RANDOM_SEED)
    join_and_export(games, reviews, OUTPUT_PATH)


if __name__ == "__main__":
    main()
