#!/usr/bin/env python3
"""
PHASE 2 - MapReduce reducer

Receives the mapper output sorted by key:

    playtime_bucket \t 0|1

Hadoop guarantees that all the values for the same key arrive consecutively,
so the reducer can aggregate a group in a single pass and emit it as soon as
the key changes.

Output format:

    playtime_bucket \t total_reviews \t recommended \t recommendation_rate
"""

import sys


def emit(bucket: str, total: int, recommended: int):
    """Print the aggregated result for one bucket."""
    if total == 0:
        return
    rate = recommended / total
    print(f"{bucket}\t{total}\t{recommended}\t{rate:.4f}")


def main():
    current_bucket = None
    total = 0
    recommended = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            bucket, value = line.split("\t", 1)
            value = int(value)
        except ValueError as exc:
            print(f"skipping line: {exc}", file=sys.stderr)
            continue

        if bucket != current_bucket:
            # Key changed: the previous group is complete
            if current_bucket is not None:
                emit(current_bucket, total, recommended)
            current_bucket = bucket
            total = 0
            recommended = 0

        total += 1
        recommended += value

    # The last group is not followed by a key change, so emit it here
    if current_bucket is not None:
        emit(current_bucket, total, recommended)


if __name__ == "__main__":
    main()
