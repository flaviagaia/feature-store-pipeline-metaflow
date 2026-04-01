from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.datasets import make_classification


def build_dataset(seed: int = 42) -> pd.DataFrame:
    features, target = make_classification(
        n_samples=1600,
        n_features=11,
        n_informative=7,
        n_redundant=2,
        weights=[0.71, 0.29],
        class_sep=0.95,
        random_state=seed,
    )
    columns = [
        "income_stability",
        "debt_ratio",
        "credit_utilization",
        "recent_delinquency",
        "installment_burden",
        "credit_history_length",
        "application_velocity",
        "hard_inquiries",
        "asset_buffer",
        "behavior_score",
        "cash_flow_volatility",
    ]
    df = pd.DataFrame(features, columns=columns)
    df["monthly_income"] = 6200 + df["income_stability"] * 950 - df["cash_flow_volatility"] * 350
    df["loan_to_income"] = (df["installment_burden"] + 2.3) / (np.abs(df["monthly_income"]) / 3000)
    df["utilization_x_behavior"] = df["credit_utilization"] * df["behavior_score"]
    df["target_default"] = target
    return df

