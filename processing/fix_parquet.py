import pandas as pd
from pathlib import Path

# --------------------------
# Base directory (ingestion/)
# --------------------------
BASE_DIR = Path(__file__).resolve().parent

# Paths
PROCESSED_DATA_PATH = BASE_DIR / "processed_data" / "cleaned_data.parquet"
PROCESSED_DATA_PATH_FIXED = BASE_DIR / "processed_data" / "cleaned_data_fixed.parquet"

# --------------------------
# Ensure input parquet exists
# --------------------------
if not PROCESSED_DATA_PATH.exists():
    raise FileNotFoundError(f"Original parquet not found at {PROCESSED_DATA_PATH}")

# --------------------------
# Read the original parquet
# --------------------------
df = pd.read_parquet(PROCESSED_DATA_PATH)

# --------------------------
# Convert all datetime columns to microseconds
# --------------------------
datetime_cols = df.select_dtypes(include=["datetime64[ns]"]).columns
for col in datetime_cols:
    df[col] = df[col].astype("datetime64[us]")

# --------------------------
# Ensure output folder exists
# --------------------------
PROCESSED_DATA_PATH_FIXED.parent.mkdir(parents=True, exist_ok=True)

# --------------------------
# Write fixed parquet
# --------------------------
df.to_parquet(
    PROCESSED_DATA_PATH_FIXED,
    engine="pyarrow",
    coerce_timestamps="us",
    allow_truncated_timestamps=True
)

print(f"Fixed parquet written to {PROCESSED_DATA_PATH_FIXED}")
