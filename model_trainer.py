import os
import sys
import logging
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

USE_XGBOOST = False
try:
    import xgboost as xgb
    USE_XGBOOST = True
except (ImportError, Exception):
    USE_XGBOOST = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ModelTrainer")

CATEGORY_RISK_MAP = {
    "grocery": 0.05,
    "dining": 0.10,
    "utilities": 0.02,
    "transport": 0.08,
    "health_beauty": 0.12,
    "entertainment": 0.20,
    "clothing": 0.25,
    "electronics": 0.65,
    "travel_flight": 0.70,
    "crypto_exchange": 0.90,
    "luxury_goods": 0.85,
    "online_gaming": 0.60,
}

FEATURE_COLUMNS = [
    "amount",
    "hour_of_day",
    "category_risk",
    "distance_from_home",
    "velocity_flag",
    "foreign_transaction",
    "device_risk_score",
]


def generate_synthetic_fraud_dataset(n_samples: int = 100000, random_state: int = 42) -> pd.DataFrame:
    logger.info(f"Generating synthetic dataset with {n_samples:,} records...")
    np.random.seed(random_state)

    normal_amounts = np.random.lognormal(mean=3.5, sigma=1.0, size=n_samples)
    normal_amounts = np.clip(normal_amounts, 2.0, 5000.0)

    hour_probs = np.array([
        0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05,
        0.06, 0.07, 0.08, 0.08, 0.08, 0.07, 0.07, 0.07,
        0.06, 0.06, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01
    ], dtype=np.float64)
    hour_probs /= hour_probs.sum()

    normal_hours = np.random.choice(
        range(24),
        size=n_samples,
        p=hour_probs
    )

    categories = list(CATEGORY_RISK_MAP.keys())
    cat_probs = [0.22, 0.18, 0.10, 0.12, 0.08, 0.08, 0.07, 0.05, 0.04, 0.02, 0.02, 0.02]
    cat_probs = np.array(cat_probs) / sum(cat_probs)
    chosen_categories = np.random.choice(categories, size=n_samples, p=cat_probs)
    category_risks = np.array([CATEGORY_RISK_MAP[c] for c in chosen_categories])

    distances = np.random.exponential(scale=12.0, size=n_samples)
    distances = np.clip(distances, 0.1, 800.0)

    device_risks = np.random.beta(a=1.5, b=8.0, size=n_samples)
    foreign_flags = np.random.choice([0, 1], size=n_samples, p=[0.96, 0.04])
    velocity_flags = np.random.choice([0, 1], size=n_samples, p=[0.97, 0.03])

    df = pd.DataFrame({
        "amount": normal_amounts,
        "hour_of_day": normal_hours,
        "merchant_category": chosen_categories,
        "category_risk": category_risks,
        "distance_from_home": distances,
        "velocity_flag": velocity_flags,
        "foreign_transaction": foreign_flags,
        "device_risk_score": device_risks,
    })

    logit = (
        -4.8
        + 0.0018 * df["amount"]
        + 3.2 * df["category_risk"]
        + 2.8 * df["velocity_flag"]
        + 2.2 * df["foreign_transaction"]
        + 2.9 * df["device_risk_score"]
        + 0.004 * df["distance_from_home"]
        + np.where((df["hour_of_day"] >= 1) & (df["hour_of_day"] <= 4), 1.8, -0.2)
        + np.where(df["amount"] > 1200, 2.5, 0.0)
    )

    fraud_prob = 1.0 / (1.0 + np.exp(-logit))
    fraud_prob = np.clip(fraud_prob + np.random.normal(0, 0.02, size=n_samples), 0.0, 1.0)
    
    df["is_fraud"] = (np.random.rand(n_samples) < fraud_prob).astype(int)

    fraud_indices = df[df["is_fraud"] == 1].index
    
    burst_idx = np.random.choice(fraud_indices, size=int(len(fraud_indices) * 0.45), replace=False)
    df.loc[burst_idx, "velocity_flag"] = 1
    df.loc[burst_idx, "amount"] *= np.random.uniform(1.5, 4.0, size=len(burst_idx))
    
    ato_idx = np.random.choice(fraud_indices, size=int(len(fraud_indices) * 0.40), replace=False)
    df.loc[ato_idx, "device_risk_score"] = np.clip(df.loc[ato_idx, "device_risk_score"] + 0.5, 0.0, 1.0)
    df.loc[ato_idx, "distance_from_home"] += np.random.exponential(scale=350.0, size=len(ato_idx))

    logger.info(
        f"Dataset generated: Total={len(df):,}, "
        f"Legitimate={sum(df['is_fraud'] == 0):,} ({100 * (1 - df['is_fraud'].mean()):.2f}%), "
        f"Fraudulent={sum(df['is_fraud'] == 1):,} ({100 * df['is_fraud'].mean():.2f}%)"
    )
    return df


def train_and_evaluate_model(df: pd.DataFrame, model_output_path: str = "fraud_model.pkl") -> None:
    X = df[FEATURE_COLUMNS]
    y = df["is_fraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )

    pos_count = np.sum(y_train == 1)
    neg_count = np.sum(y_train == 0)
    scale_pos_weight = neg_count / max(pos_count, 1)

    if USE_XGBOOST:
        logger.info(f"Training XGBoost Classifier (scale_pos_weight: {scale_pos_weight:.2f})...")
        model = xgb.XGBClassifier(
            n_estimators=250,
            max_depth=6,
            learning_rate=0.05,
            scale_pos_weight=scale_pos_weight,
            subsample=0.85,
            colsample_bytree=0.85,
            eval_metric="aucpr",
            random_state=42,
            tree_method="hist",
        )
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )
        model_type_name = "XGBClassifier"
    else:
        logger.info("Using Scikit-Learn RandomForestClassifier with balanced class weighting...")
        model = RandomForestClassifier(
            n_estimators=150,
            max_depth=12,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_train, y_train)
        model_type_name = "RandomForestClassifier"

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    threshold = 0.60
    y_pred = (y_pred_proba >= threshold).astype(int)

    roc_auc = roc_auc_score(y_test, y_pred_proba)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred)

    print("\n" + "=" * 65)
    print("           MODEL EVALUATION SUMMARY REPORT")
    print("=" * 65)
    print(f"ROC-AUC Score:      {roc_auc:.4f}")
    print(f"Precision Score:    {precision:.4f}")
    print(f"Recall Score:       {recall:.4f}")
    print(f"F1 Score:           {f1:.4f}")
    print(f"Decision Threshold: {threshold:.2f}")
    print("\nConfusion Matrix:")
    print(f"  [TN={cm[0, 0]:<6} FP={cm[0, 1]:<6}]")
    print(f"  [FN={cm[1, 0]:<6} TP={cm[1, 1]:<6}]")
    print("\nDetailed Classification Report:")
    print(classification_report(y_test, y_pred, target_names=["Legitimate", "Fraud"]))
    print("=" * 65 + "\n")

    importance_df = pd.DataFrame({
        "Feature": FEATURE_COLUMNS,
        "Importance": model.feature_importances_
    }).sort_values(by="Importance", ascending=False)
    
    logger.info("Top Feature Importances:")
    for _, row in importance_df.iterrows():
        logger.info(f"  - {row['Feature']:<20}: {row['Importance']:.4f}")

    artifact = {
        "model": model,
        "feature_names": FEATURE_COLUMNS,
        "category_risk_map": CATEGORY_RISK_MAP,
        "threshold": threshold,
        "metadata": {
            "trained_at": datetime.utcnow().isoformat(),
            "roc_auc": float(roc_auc),
            "f1": float(f1),
            "precision": float(precision),
            "recall": float(recall),
            "model_type": model_type_name,
        }
    }

    joblib.dump(artifact, model_output_path)
    logger.info(f"Successfully exported model artifact to '{os.path.abspath(model_output_path)}'.")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(current_dir, "fraud_model.pkl")
    
    synthetic_df = generate_synthetic_fraud_dataset(n_samples=120000, random_state=42)
    train_and_evaluate_model(synthetic_df, model_output_path=model_path)
