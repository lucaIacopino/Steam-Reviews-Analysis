"""
FASE 0 - Preprocessing del dataset "Game Recommendations on Steam"
Dataset: https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam
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

    # Colonne attese: app_id, title, date_release, win, mac, linux,
    # rating, positive_ratio, user_reviews, price_final, price_original,
    # discount, steam_deck
    print(f"[games.csv] righe iniziali: {len(df)}")

    # Rimuovi righe senza titolo o senza prezzo (dati incompleti)
    df = df.dropna(subset=["app_id", "title", "price_final"])

    # Cast espliciti
    df["price_final"] = df["price_final"].astype(float)
    df["date_release"] = pd.to_datetime(df["date_release"], errors="coerce")

    # Crea una fascia di prezzo, utile per le analisi successive (Spark/Mongo)
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


if __name__ == "__main__":
    games = load_games(GAMES_PATH)
    print(games.head())
