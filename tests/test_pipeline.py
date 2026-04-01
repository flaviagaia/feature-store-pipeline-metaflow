from __future__ import annotations

from pathlib import Path
import unittest

from src.data_factory import build_dataset
from src.feature_logic import build_feature_store
from main import run_local_pipeline


class FeatureStorePipelineTestCase(unittest.TestCase):
    def test_feature_store_adds_derived_columns(self) -> None:
        df = build_dataset()
        feature_df = build_feature_store(df)
        self.assertIn("income_to_debt_gap", feature_df.columns)
        self.assertIn("stability_x_history", feature_df.columns)
        self.assertIn("risk_pressure_index", feature_df.columns)

    def test_local_pipeline_produces_metrics(self) -> None:
        report = run_local_pipeline(Path(__file__).resolve().parents[1])
        self.assertGreater(report["row_count"], 1000)
        self.assertGreater(report["feature_count"], 10)
        self.assertGreater(report["roc_auc"], 0.75)
        self.assertGreater(report["average_precision"], 0.65)


if __name__ == "__main__":
    unittest.main()

