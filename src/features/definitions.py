"""
Feature definitions: name, inputs, description, and lightweight metadata for APIs.

kind: count | boolean | categorical | numeric | constant
dtype: expected output type name for the feature column.
"""

FEATURE_DEFINITIONS = [
    {
        "name": "num_active_services",
        "dtype": "int64",
        "kind": "count",
        "input_columns": [
            "PhoneService", "MultipleLines", "OnlineSecurity", "OnlineBackup",
            "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies",
        ],
        "description": "Count of active services (Yes = 1, else 0).",
    },
    {
        "name": "is_long_term_contract",
        "dtype": "bool",
        "kind": "boolean",
        "input_columns": ["Contract"],
        "description": "True if Contract is One year or Two year.",
    },
    {
        "name": "monthly_charge_band",
        "dtype": "string",
        "kind": "categorical",
        "input_columns": ["MonthlyCharges"],
        "description": "Low / Medium / High band from MonthlyCharges (fixed thresholds).",
    },
    {
        "name": "charge_per_tenure",
        "dtype": "float64",
        "kind": "numeric",
        "input_columns": ["TotalCharges", "tenure"],
        "description": "TotalCharges / tenure; tenure=0 -> 0.",
    },
    {
        "name": "has_tech_support",
        "dtype": "bool",
        "kind": "boolean",
        "input_columns": ["TechSupport"],
        "description": "True if TechSupport == Yes.",
    },
    {
        "name": "is_fiber_user",
        "dtype": "bool",
        "kind": "boolean",
        "input_columns": ["InternetService"],
        "description": "True if InternetService == Fiber optic.",
    },
    {
        "name": "has_streaming_bundle",
        "dtype": "bool",
        "kind": "boolean",
        "input_columns": ["StreamingTV", "StreamingMovies"],
        "description": "True if both StreamingTV and StreamingMovies are Yes.",
    },
    {
        "name": "feature_version",
        "dtype": "string",
        "kind": "constant",
        "input_columns": [],
        "description": "Version string for the feature table build (same value per row).",
    },
]

FEATURE_VERSION = "v1.0"
