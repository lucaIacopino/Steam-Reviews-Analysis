#!/usr/bin/env python3
"""
PHASE 2 - MapReduce mapper

Reads the ingested CSV chunks from stdin and emits, for every review,
the playtime bucket as key and the recommendation flag as value:

    playtime_bucket \t 1     (the player recommended the game)
    playtime_bucket \t 0     (the player did not)

The reducer then aggregates these into a recommendation rate per bucket.
"""

import csv
import sys

# Column positions in the ingested CSV (see phase 0 output schema).
# Reading by index rather than by name keeps the mapper independent from
# the header, which is repeated in every chunk.
COL_IS_RECOMMENDED = 4
COL_HOURS = 5

HEADER_FIRST_FIELD = "app_id"


def playtime_bucket(hours: float) -> str:
    """Map a raw playtime value to a discrete bucket.

    Thresholds reflect typical gaming behaviour: under an hour is barely
    a trial, over a hundred hours is a heavily invested player.
    """
    if hours < 1:
        return "0-1h"
    elif hours < 5:
        return "1-5h"
    elif hours < 20:
        return "5-20h"
    elif hours < 100:
        return "20-100h"
    else:
        return "100h+"


def main():
    reader = csv.reader(sys.stdin)

    for row in reader:
        # Every chunk carries its own header line: skip it wherever it appears
        if not row or row[0] == HEADER_FIRST_FIELD:
            continue

        try:
            hours = float(row[COL_HOURS])
            # pandas writes booleans as the strings "True"/"False"
            is_recommended = row[COL_IS_RECOMMENDED].strip().lower() == "true"
        except (ValueError, IndexError) as exc:
            # Malformed rows go to stderr so they don't pollute the job output
            print(f"skipping row: {exc}", file=sys.stderr)
            continue

        bucket = playtime_bucket(hours)
        print(f"{bucket}\t{1 if is_recommended else 0}")


if __name__ == "__main__":
    main()
