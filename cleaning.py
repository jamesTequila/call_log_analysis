# cleaning.py
from __future__ import annotations
import re
from collections import Counter
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd


def parse_hms_to_seconds(s: str) -> int:
    """Convert 'HH:MM:SS' to seconds. Non-parsable values -> 0."""
    try:
        parts = str(s).split(":")
        if len(parts) != 3:
            return 0
        h, m, sec = map(int, parts)
        return h * 3600 + m * 60 + sec
    except Exception:
        return 0


def classify_customer_from_activity(activity: str) -> str | None:
    """
    Look for 'Inbound: ...' in Call Activity Details.
    If first non-space char after 'Inbound:' is a digit -> retail,
    else -> trade. Returns 'trade', 'retail' or None.
    """
    if not isinstance(activity, str):
        return None

    m = re.search(r"Inbound:\s*(.+?)(?:\s*→|\s*\(|$)", activity)
    if not m:
        return None

    token = m.group(1).strip()
    if not token:
        return None

    first = token[0]
    if first.isdigit():
        return "retail"
    else:
        return "trade"


@dataclass
class CleanedData:
    raw_call_df: pd.DataFrame
    call_level_df: pd.DataFrame


def clean_call_log(call_log_path: str) -> pd.DataFrame:
    """Load and clean the raw call log."""
    df = pd.read_csv(call_log_path)

    # Drop the 'Totals' row (or any non-date value in Call Time)
    df["Call Time dt"] = pd.to_datetime(df["Call Time"], errors="coerce")
    df = df[~df["Call Time dt"].isna()].copy()

    # Convert durations to seconds
    df["Ringing_sec"] = df["Ringing"].apply(parse_hms_to_seconds)
    df["Talking_sec"] = df["Talking"].apply(parse_hms_to_seconds)

    # Classify legs as trade / retail / None
    df["customer_type_leg"] = df["Call Activity Details"].apply(
        classify_customer_from_activity
    )

    # Restrict to inbound directions
    inbound_mask = df["Direction"].isin(["Inbound", "Inbound Queue"])
    df = df[inbound_mask].copy()

    return df


def aggregate_to_call_level(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate leg-level rows to one row per Call ID."""
    def resolve_customer_type(series: pd.Series) -> str:
        """Resolve customer type from multiple legs, defaulting to retail if unclear."""
        vals = set(v for v in series if isinstance(v, str))
        if "trade" in vals:
            return "trade"
        if "retail" in vals:
            return "retail"
        # Default to retail for any unknown cases (better than leaving unclassified)
        return "retail"

    grouped = (
        df
        .groupby("Call ID")
        .agg(
            call_start=("Call Time dt", "min"),
            from_number=("From", "first"),
            to_number=("To", "first"),
            directions=("Direction", lambda x: ",".join(sorted(set(x)))),
            statuses=("Status", lambda x: ",".join(sorted(set(x)))),
            ringing_total_sec=("Ringing_sec", "sum"),
            talking_total_sec=("Talking_sec", "sum"),
            customer_type=("customer_type_leg", resolve_customer_type),
            call_activity_details=("Call Activity Details", lambda x: " | ".join(sorted(set(x.dropna().astype(str))))),
        )
        .reset_index()
    )

    grouped["is_answered"] = grouped["talking_total_sec"] > 0
    grouped["is_abandoned"] = (grouped["talking_total_sec"] == 0) & (
        grouped["ringing_total_sec"] > 0
    )

    # Date / week helpers
    grouped["date"] = grouped["call_start"].dt.date
    grouped["day_name"] = grouped["call_start"].dt.day_name()

    # Week assignment based on max date in dataset
    # Week 1 = max_date going back 7 days
    # Week 2 = 7 days before Week 1
    # Week 3+ = everything older
    max_date = grouped["call_start"].max()
    
    # Week 1: From (max_date - 7 days) up to and including max_date
    week1_start = max_date - pd.Timedelta(days=7)
    week1_end = max_date
    
    # Week 2: 7 days before Week 1
    week2_start = week1_start - pd.Timedelta(days=7)
    week2_end = week1_start
    
    # Assign week labels
    def assign_week(dt):
        if dt > week1_start and dt <= week1_end:
            return 1
        elif dt > week2_start and dt <= week2_end:
            return 2
        else:
            return 3
    
    grouped["week"] = grouped["call_start"].apply(assign_week)
    
    # Keep week_start for compatibility
    grouped["week_start"] = grouped["call_start"].dt.normalize() - pd.to_timedelta(
        grouped["call_start"].dt.dayofweek, unit="D"
    )

    return grouped


def run_cleaning(call_log_path: str,) -> CleanedData:
    raw_call_df = clean_call_log(call_log_path)
    call_level_df = aggregate_to_call_level(raw_call_df)
    return CleanedData(
        raw_call_df=raw_call_df,
        call_level_df=call_level_df,
    )
