from __future__ import annotations

import json
from pathlib import Path

from metaflow import FlowSpec, step
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.data_factory import build_dataset
from src.feature_logic import build_feature_store


class FeatureStorePipelineFlow(FlowSpec):
    @step
    def start(self):
        self.raw_df = build_dataset()
        self.next(self.validate)

    @step
    def validate(self):
        self.row_count = len(self.raw_df)
        self.positive_rate = float(self.raw_df["target_default"].mean())
        self.has_missing = bool(self.raw_df.isna().sum().sum())
        self.next(self.create_feature_store)

    @step
    def create_feature_store(self):
        self.feature_df = build_feature_store(self.raw_df)
        self.feature_columns = [col for col in self.feature_df.columns if col != "target_default"]
        self.next(self.train_model)

    @step
    def train_model(self):
        X = self.feature_df[self.feature_columns]
        y = self.feature_df["target_default"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42, stratify=y
        )
        self.model = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("model", LogisticRegression(max_iter=2500, class_weight="balanced")),
            ]
        )
        self.model.fit(X_train, y_train)
        self.y_test = y_test.to_numpy()
        self.y_score = self.model.predict_proba(X_test)[:, 1]
        self.next(self.evaluate)

    @step
    def evaluate(self):
        self.metrics = {
            "roc_auc": round(float(roc_auc_score(self.y_test, self.y_score)), 4),
            "average_precision": round(float(average_precision_score(self.y_test, self.y_score)), 4),
        }
        self.next(self.persist)

    @step
    def persist(self):
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        feature_store_path = output_dir / "feature_store_snapshot.csv"
        report_path = output_dir / "metaflow_report.json"
        self.feature_df.to_csv(feature_store_path, index=False)
        report = {
            "flow_name": "FeatureStorePipelineFlow",
            "row_count": self.row_count,
            "positive_rate": round(self.positive_rate, 4),
            "has_missing": self.has_missing,
            "feature_count": len(self.feature_columns),
            "metrics": self.metrics,
            "feature_store_path": str(feature_store_path.resolve()),
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        self.report = report
        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    FeatureStorePipelineFlow()

