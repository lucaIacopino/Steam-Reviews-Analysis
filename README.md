# Steam Reviews — Big Data Analysis

Progetto per l'esame di [nome corso] — analisi di un dataset simulando una
sorgente big-data, con pipeline scalabile basata su Hadoop MapReduce, Spark e MongoDB.

## Ipotesi di ricerca

Le ore di gioco al momento della recensione influenzano la probabilità che il
giocatore raccomandi il gioco, e questo effetto varia in base al genere/prezzo
del gioco?

## Dataset

[Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam)
(Kaggle) — ~41M recensioni utente + metadati di ~50k giochi.

## Architettura

_(diagramma e dettagli in `docs/architecture.md`, in arrivo)_

1. **Preprocessing** (pandas) — pulizia e join di `games.csv` e `recommendations.csv`
2. **Ingestion** (HDFS) — simulazione di arrivo dati in streaming
3. **MapReduce** (Hadoop Streaming) — analisi testuale delle recensioni
4. **Spark** — aggregazioni statistiche + Machine Learning (MLlib)
5. **MongoDB** — query di analisi aggregate

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Come eseguire

_(istruzioni aggiornate man mano che si aggiungono le fasi)_

### Fase 0 — Preprocessing

```bash
python src/preprocessing/fase0_preprocessing.py
```

Scarica manualmente `games.csv` e `recommendations.csv` dal link Kaggle sopra
e posizionali nella cartella `data/` prima di eseguire lo script.
