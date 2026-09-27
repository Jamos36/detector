from pathlib import Path
import pandas as pd

# Requires:
#   pip install pandas pyarrow

here = Path(__file__).resolve().parent

for csv_path in sorted(here.glob("mock_netflow_*.csv")):
    df = pd.read_csv(
        csv_path,
        parse_dates=["flow_start_time", "flow_end_time", "time_stamp"]
    )
    out = csv_path.with_suffix(".parquet")
    df.to_parquet(out, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote {out.name}: {len(df):,} rows")
