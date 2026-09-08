"""
generate_bronze_events.py
==========================
Expands the REAL Cookie Cats dataset (data/raw/cookie_cats_real.csv) into a
raw, event-level telemetry stream that lands in data/bronze_landing/ as a
series of daily CSV "drop" files -- the way a real mobile game's event
pipeline (Firebase/Amplitude/Segment-style export) actually arrives.

IMPORTANT - what is REAL vs DERIVED:
  REAL (untouched, from the public Cookie Cats A/B test dataset, 90,189
  actual players of Tactile Entertainment's Cookie Cats mobile game):
    - userid
    - version           (A/B test arm: gate_30 / gate_40)
    - sum_gamerounds    (actual total rounds played in the first 14 days)
    - retention_1       (actually returned on day 1)
    - retention_7       (actually returned on day 7)

  DERIVED / SIMULATED (studios never publish raw clickstream for privacy
  reasons, so no public raw event-level export of this experiment exists):
    - exact event timestamps, session boundaries, individual level-progress
      increments, device/platform/country metadata, and purchase events.
    These are generated PER USER so that they are statistically consistent
    with that user's REAL sum_gamerounds and REAL retention_1/retention_7
    outcome -- i.e. a user who really did NOT return on day 1 gets zero
    generated events on day 1, a user who really played 165 rounds has
    events that sum to exactly 165 rounds played, etc. This lets the
    warehouse be built on genuine, event-level raw data while keeping every
    business metric traceable back to the real published ground truth.

The generator also injects realistic "raw data is messy" artifacts that a
real Bronze layer has to deal with: duplicate events (client retry /
at-least-once delivery), out-of-order arrival within a file, and a few
missing metadata fields.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

ROOT = Path(__file__).resolve().parents[1]
RAW_REAL = ROOT / "data" / "raw" / "cookie_cats_real.csv"
LANDING_DIR = ROOT / "data" / "bronze_landing"
LANDING_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference data (realistic, not tied to any individual real person)
# ---------------------------------------------------------------------------
PLATFORMS = ["ios", "android"]
PLATFORM_WEIGHTS = [0.42, 0.58]  # Android skews higher for this genre, realistic split

COUNTRIES = ["US", "GB", "CA", "AU", "DE", "FR", "BR", "IN", "JP", "KR", "MX", "SE"]
COUNTRY_WEIGHTS = [0.28, 0.08, 0.06, 0.05, 0.07, 0.06, 0.09, 0.12, 0.05, 0.05, 0.05, 0.04]

APP_VERSIONS = ["1.44.0", "1.44.1", "1.45.0", "1.46.0", "1.46.2"]

# Real mobile-game IAP price tiers (typical F2P "gem pack" pricing)
PRODUCTS = [
    ("gems_small", 0.99),
    ("gems_medium", 4.99),
    ("gems_large", 9.99),
    ("gems_mega", 19.99),
    ("gold_pass", 49.99),
    ("unlimited_lives_bundle", 99.99),
]
PRODUCT_WEIGHTS = [0.35, 0.28, 0.18, 0.10, 0.06, 0.03]

INSTALL_WINDOW_START = datetime(2019, 1, 15)
INSTALL_WINDOW_DAYS = 30  # installs spread across a 30-day acquisition window

SIM_HORIZON_DAYS = 14  # matches the 14-day window sum_gamerounds is measured over


def pick(options, weights):
    return random.choices(options, weights=weights, k=1)[0]


def build_active_days(retention_1: bool, retention_7: bool, engagement: float) -> list:
    """Decide which of days 0..13 the user opened the app.

    Day 0 is always active (install day). Day 1 and Day 7 activity are
    forced to match the REAL retention flags exactly. Other days are
    probabilistic and scale with the user's real engagement level so
    heavier real players plausibly show up more often.
    """
    days = {0}
    if retention_1:
        days.add(1)
    if retention_7:
        days.add(7)

    # engagement in [0,1] percentile-ish scale drives how "sticky" the user is
    for d in range(2, 7):
        if retention_1 and random.random() < min(0.55, 0.10 + engagement * 0.6):
            days.add(d)
    if retention_7:
        for d in range(8, SIM_HORIZON_DAYS):
            if random.random() < min(0.45, 0.08 + engagement * 0.5):
                days.add(d)
    return sorted(days)


def split_counts(total: int, n: int) -> list:
    """Split an integer total into n non-negative integer parts (random weights)."""
    if n <= 0 or total <= 0:
        return [0] * max(n, 0)
    weights = np.random.dirichlet(np.ones(n) * 0.8)
    parts = np.floor(weights * total).astype(int)
    remainder = total - parts.sum()
    for i in random.sample(range(n), int(remainder)) if remainder > 0 else []:
        parts[i] += 1
    return parts.tolist()


def gen_events_for_user(row, engagement_pctile: float) -> list:
    userid = int(row.userid)
    total_rounds = int(row.sum_gamerounds)
    retention_1 = bool(row.retention_1)
    retention_7 = bool(row.retention_7)

    install_dt = INSTALL_WINDOW_START + timedelta(
        days=random.randint(0, INSTALL_WINDOW_DAYS - 1),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

    platform = pick(PLATFORMS, PLATFORM_WEIGHTS)
    country = pick(COUNTRIES, COUNTRY_WEIGHTS)
    app_version = random.choice(APP_VERSIONS)
    device_id = str(uuid.uuid4())

    active_days = build_active_days(retention_1, retention_7, engagement_pctile)

    # distribute the REAL total_rounds across the active days
    day_round_totals = dict(zip(active_days, split_counts(total_rounds, len(active_days))))

    # Midnight of the install's calendar date -- day-N boundaries are anchored
    # here (NOT to install_dt's time-of-day) so that a "day since install"
    # index always corresponds to exactly one calendar date and never bleeds
    # into the neighboring day. This matters because retention_1/retention_7
    # are calendar-day definitions.
    install_midnight = datetime(install_dt.year, install_dt.month, install_dt.day)

    events = []

    def emit(event_name, ts, **props):
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_name": event_name,
                "client_event_time": ts.isoformat(sep=" "),
                "userid": userid,
                "platform": platform if random.random() > 0.005 else None,  # rare missing field
                "country": country,
                "app_version": app_version,
                "device_id": device_id,
                **props,
            }
        )

    for day in active_days:
        rounds_today = day_round_totals.get(day, 0)

        if day == 0:
            # Day 0's first burst IS the real install moment -- anchor to the
            # actual install timestamp, not calendar midnight, so it never
            # drifts onto the previous/next calendar date.
            n_bursts = 1
            burst_starts = [install_dt]
        else:
            day_start = install_midnight + timedelta(days=day)
            n_bursts = int(np.random.choice([1, 1, 2, 2, 3], p=[0.45, 0.25, 0.15, 0.10, 0.05]))
            # hour offsets sampled within [0,23] so every burst that day stays
            # on that same calendar date
            hour_offsets = sorted(random.sample(range(0, 24), n_bursts))
            burst_starts = [
                day_start + timedelta(hours=h, minutes=random.randint(0, 59))
                for h in hour_offsets
            ]

        rounds_per_burst = split_counts(rounds_today, n_bursts)

        for b_idx, burst_start in enumerate(burst_starts):
            t = burst_start

            emit("app_open", t, rounds_completed=None, revenue_usd=None, product_id=None, currency=None)
            t += timedelta(seconds=random.randint(5, 40))

            rounds_this_burst = rounds_per_burst[b_idx] if b_idx < len(rounds_per_burst) else 0
            # split this burst's rounds into 1-4 level_progress pings
            n_pings = 1 if rounds_this_burst <= 3 else random.randint(2, 4)
            ping_counts = split_counts(rounds_this_burst, n_pings)
            for pc in ping_counts:
                if pc <= 0 and n_pings > 1:
                    continue
                emit(
                    "level_progress",
                    t,
                    rounds_completed=int(pc),
                    revenue_usd=None,
                    product_id=None,
                    currency=None,
                )
                t += timedelta(seconds=random.randint(10, 180))

            # conversion probability scales with engagement percentile
            purchase_p = min(0.16, 0.015 + engagement_pctile * 0.18)
            if random.random() < purchase_p:
                product_id, price = PRODUCTS[
                    np.random.choice(len(PRODUCTS), p=PRODUCT_WEIGHTS)
                ]
                emit(
                    "purchase",
                    t + timedelta(seconds=random.randint(5, 60)),
                    rounds_completed=None,
                    revenue_usd=round(price, 2),
                    product_id=product_id,
                    currency="USD",
                )

            t += timedelta(minutes=random.randint(1, 5))
            emit("app_close", t, rounds_completed=None, revenue_usd=None, product_id=None, currency=None)

    return events


def main():
    real_df = pd.read_csv(RAW_REAL)
    real_df["retention_1"] = real_df["retention_1"].astype(bool)
    real_df["retention_7"] = real_df["retention_7"].astype(bool)

    # engagement percentile (0..1) from the REAL sum_gamerounds distribution
    real_df["engagement_pctile"] = real_df["sum_gamerounds"].rank(pct=True)

    all_events = []
    for row in real_df.itertuples(index=False):
        all_events.extend(gen_events_for_user(row, float(row.engagement_pctile)))

    events_df = pd.DataFrame(all_events)
    events_df["client_event_time"] = pd.to_datetime(events_df["client_event_time"])

    # ---- inject realistic raw-data messiness -------------------------------
    # 1) duplicate ~1.5% of events (at-least-once delivery retries)
    dup_sample = events_df.sample(frac=0.015, random_state=7)
    events_df = pd.concat([events_df, dup_sample], ignore_index=True)

    # 2) shuffle rows so files are NOT pre-sorted by event time (real exports aren't)
    events_df = events_df.sample(frac=1.0, random_state=11).reset_index(drop=True)

    # 3) partition into daily "landing" files by the date the file was DROPPED
    #    (ingestion date), which we approximate as event date + 0-1 day delivery
    #    lag to simulate late-arriving events.
    delivery_lag_hours = np.random.choice([0, 1, 2, 6, 26], size=len(events_df), p=[0.80, 0.10, 0.05, 0.03, 0.02])
    events_df["_landing_date"] = (
        events_df["client_event_time"] + pd.to_timedelta(delivery_lag_hours, unit="h")
    ).dt.date

    out_cols = [
        "event_id", "event_name", "client_event_time", "userid", "platform",
        "country", "app_version", "device_id", "rounds_completed",
        "revenue_usd", "product_id", "currency",
    ]

    n_files = 0
    for landing_date, chunk in events_df.groupby("_landing_date"):
        fname = LANDING_DIR / f"events_{landing_date}.csv"
        chunk[out_cols].to_csv(fname, index=False, quoting=csv.QUOTE_MINIMAL)
        n_files += 1

    print(f"Generated {len(events_df):,} raw events across {n_files} daily landing files.")
    print(f"Real users represented: {real_df['userid'].nunique():,}")
    print(f"Landing dir: {LANDING_DIR}")


if __name__ == "__main__":
    main()
