"""
Generate a larger synthetic Telco-style CSV for local demos.

This is synthetic data (not real customers). Schema matches the pipeline's
expected raw columns so `python -m src.pipeline.build_feature_table` works
out of the box with a non-trivial row count.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)
N_ROWS = 250

GENDERS = ["Male", "Female"]
YES_NO = ["Yes", "No"]
CONTRACTS = ["Month-to-month", "One year", "Two year"]
INTERNET = ["DSL", "Fiber optic", "No"]
PAYMENT = [
    "Electronic check",
    "Mailed check",
    "Bank transfer (automatic)",
    "Credit card (automatic)",
]


def _customer_id(i: int) -> str:
    suffix = "".join(RNG.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=5))
    return f"{7000 + (i % 2000)}-{suffix}"


def _phone_and_lines() -> tuple[str, str]:
    if RNG.random() < 0.2:
        return "No", "No phone service"
    phone = "Yes"
    ml = RNG.choice(["No", "Yes"], p=[0.55, 0.45])
    return phone, str(ml)


def _internet_for_phone(phone: str) -> str:
    if phone == "No":
        return "No"
    return str(RNG.choice(INTERNET[:2], p=[0.45, 0.55]))


def _yes_no_service(phone: str, internet: str) -> str:
    if phone == "No" and internet == "No":
        return "No internet service"
    return str(RNG.choice(YES_NO, p=[0.35, 0.65]))


def build_frame(n: int = N_ROWS) -> pd.DataFrame:
    rows: list[dict] = []
    for i in range(n):
        phone, multiple_lines = _phone_and_lines()
        internet = _internet_for_phone(phone)
        tenure = int(RNG.integers(0, 73))
        monthly = float(round(RNG.uniform(18.0, 118.0), 2))
        if tenure == 0:
            total = monthly
        else:
            jitter = RNG.uniform(0.85, 1.15)
            total = float(round(monthly * tenure * jitter, 2))

        row = {
            "customerID": _customer_id(i),
            "gender": str(RNG.choice(GENDERS)),
            "SeniorCitizen": int(RNG.random() < 0.16),
            "Partner": str(RNG.choice(YES_NO, p=[0.5, 0.5])),
            "Dependents": str(RNG.choice(YES_NO, p=[0.7, 0.3])),
            "tenure": tenure,
            "PhoneService": phone,
            "MultipleLines": multiple_lines,
            "InternetService": internet,
            "OnlineSecurity": _yes_no_service(phone, internet),
            "OnlineBackup": _yes_no_service(phone, internet),
            "DeviceProtection": _yes_no_service(phone, internet),
            "TechSupport": _yes_no_service(phone, internet),
            "StreamingTV": _yes_no_service(phone, internet),
            "StreamingMovies": _yes_no_service(phone, internet),
            "Contract": str(RNG.choice(CONTRACTS, p=[0.55, 0.25, 0.20])),
            "PaperlessBilling": str(RNG.choice(YES_NO, p=[0.6, 0.4])),
            "PaymentMethod": str(RNG.choice(PAYMENT)),
            "MonthlyCharges": monthly,
            "TotalCharges": total,
            "Churn": str(RNG.choice(YES_NO, p=[0.27, 0.73])),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out = root / "data" / "raw" / "sample_raw.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df = build_frame(N_ROWS)
    df.to_csv(out, index=False)
    print(f"Wrote {len(df)} rows -> {out}")


if __name__ == "__main__":
    main()
