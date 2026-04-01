from __future__ import annotations

import pandas as pd


def build_feature_store(df: pd.DataFrame) -> pd.DataFrame:
    feature_df = df.copy()
    feature_df["income_to_debt_gap"] = feature_df["monthly_income"] - (feature_df["debt_ratio"] * 800)
    feature_df["stability_x_history"] = (
        feature_df["income_stability"] * feature_df["credit_history_length"]
    )
    feature_df["risk_pressure_index"] = (
        feature_df["credit_utilization"].abs()
        + feature_df["recent_delinquency"].abs()
        + feature_df["loan_to_income"].abs()
    )
    return feature_df

