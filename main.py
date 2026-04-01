from __future__ import annotations

import json
from pathlib import Path

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.data_factory import build_dataset
from src.feature_logic import build_feature_store


def run_local_pipeline(base_dir: str | Path) -> dict:
    df = build_dataset()
    feature_df = build_feature_store(df)
    X = feature_df.drop(columns=["target_default"])
    y = feature_df["target_default"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )
    model = Pipeline(
        [("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=2500, class_weight="balanced"))]
    )
    model.fit(X_train, y_train)
    y_score = model.predict_proba(X_test)[:, 1]
    report = {
        "row_count": len(feature_df),
        "positive_rate": round(float(y.mean()), 4),
        "feature_count": X.shape[1],
        "roc_auc": round(float(roc_auc_score(y_test, y_score)), 4),
        "average_precision": round(float(average_precision_score(y_test, y_score)), 4),
    }
    output_dir = Path(base_dir) / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "local_pipeline_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report


def main() -> None:
    report = run_local_pipeline(Path(__file__).resolve().parent)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

