# import os
# import glob
# import pandas as pd
# import duckdb
# import datetime

# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed')
# # today_str = datetime.datetime.now().strftime('%Y-%m-%d')
# # today_folder = os.path.join(PROCESSED_DATA_DIR, today_str)
# REPORT_FILE = os.path.join(PROJECT_ROOT, 'data', 'data_quality_report.csv')

# def validate_with_duckdb():
#     files = glob.glob(os.path.join(PROCESSED_DATA_DIR, "*", '*_transformed.parquet'))
#     if not files:
#         print("No processed files found")
#         return

#     all_dfs = []
#     for f in files:
#         try:
#             df = pd.read_parquet(f)
#             all_dfs.append(df)
#         except Exception as e:
#             print(f"Could not read {f}: {e}")

#     if not all_dfs:
#         print("No data loaded for validation.")
#         return

#     df = pd.concat(all_dfs, ignore_index=True)

#     results = []
#     # Per reading_type
#     for rtype in df['reading_type'].unique():
#         part = df[df['reading_type'] == rtype]
#         n = len(part)
#         missing = part['value'].isnull().sum()
#         anomalous = part['anomalous_reading'].sum()
#         battery_nan = part['battery_level'].isnull().sum()
#         results.append({
#             'reading_type': rtype,
#             'total_records': n,
#             'missing_value_%': 100 * missing / n if n else 0,
#             'anomalous_reading_%': 100 * anomalous / n if n else 0,
#             'battery_level_missing_%': 100 * battery_nan / n if n else 0
#         })

#     # Per sensor coverage gaps
#     for sensor in df['sensor_id'].unique():
#         sensor_df = df[df['sensor_id'] == sensor]
#         times = pd.to_datetime(sensor_df['timestamp'], errors='coerce')
#         if times.isnull().all():
#             continue
#         min_ts, max_ts = times.min(), times.max()
#         expected = pd.date_range(min_ts, max_ts, freq='H')
#         missing_hours = len(set(expected) - set(times))
#         results.append({
#             'sensor_id': sensor,
#             'missing_hours': missing_hours
#         })

#     pd.DataFrame(results).to_csv(REPORT_FILE, index=False)
#     print(f"[OK] Validation report written to {REPORT_FILE}")

# if __name__ == "__main__":
#     validate_with_duckdb()




import os
import glob
import pandas as pd
import duckdb
import datetime
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed')
REPORT_FILE = os.path.join(PROJECT_ROOT, 'data', 'data_quality_report.csv')

# --- CONFIGURATION ---
# 'all': scan all *_transformed.parquet files under all processed subfolders (default; assignment-style)
# 'today': only validate today's folder
# 'latest': only validate the most recent date-folder (regardless of system date)
# CHANGE THIS TO 'today' or 'latest' if desired!
VALIDATE_MODE = 'latest'

def get_files_to_validate():
    if VALIDATE_MODE == 'all':
        files = glob.glob(os.path.join(PROCESSED_DATA_DIR, "*", "*_transformed.parquet"))
    elif VALIDATE_MODE == 'today':
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        today_folder = os.path.join(PROCESSED_DATA_DIR, today_str)
        files = glob.glob(os.path.join(today_folder, "*_transformed.parquet"))
    elif VALIDATE_MODE == 'latest':
        # Find subfolder with max (YYYY-MM-DD) name
        subfolders = [
            os.path.join(PROCESSED_DATA_DIR, d)
            for d in os.listdir(PROCESSED_DATA_DIR)
            if os.path.isdir(os.path.join(PROCESSED_DATA_DIR, d))
        ]
        if not subfolders:
            return []
        latest_folder = max(subfolders)
        files = glob.glob(os.path.join(latest_folder, "*_transformed.parquet"))
    else:
        raise ValueError("VALIDATE_MODE must be one of 'all', 'today', or 'latest'")
    return files

def validate_with_duckdb():
    files = get_files_to_validate()
    if not files:
        print("[WARN] No processed files found to validate. Did you run transform.py?")
        return

    all_dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            all_dfs.append(df)
        except Exception as e:
            print(f"Could not read {f}: {e}")

    if not all_dfs:
        print("[FAIL] No data loaded for validation.")
        return

    df = pd.concat(all_dfs, ignore_index=True)

    results = []
    # Per reading_type
    for rtype in df['reading_type'].unique():
        part = df[df['reading_type'] == rtype]
        n = len(part)
        missing = part['value'].isnull().sum()
        anomalous = part['anomalous_reading'].sum()
        battery_nan = part['battery_level'].isnull().sum()
        # Type & range validation - flagged in anomaly detection logic (0% means passed)
        results.append({
            'reading_type': rtype,
            'total_records': n,
            'missing_value_%': round(100 * missing / n, 2) if n else 0,
            'anomalous_reading_%': round(100 * anomalous / n, 2) if n else 0,
            'battery_level_missing_%': round(100 * battery_nan / n, 2) if n else 0
        })

    # Per sensor coverage gaps
    for sensor in df['sensor_id'].unique():
        sensor_df = df[df['sensor_id'] == sensor]
        times = pd.to_datetime(sensor_df['timestamp'], errors='coerce')
        times = times.dropna()
        if times.empty:
            continue
        min_ts, max_ts = times.min(), times.max()
        expected = pd.date_range(min_ts, max_ts, freq='h')
        observed_hours = set(times.dt.floor('h'))
        expected_hours = set(expected)
        missing_hours = len(expected_hours - observed_hours)
        results.append({
            'sensor_id': sensor,
            'missing_hours': missing_hours
        })

    pd.DataFrame(results).to_csv(REPORT_FILE, index=False)
    print(f"[OK] Validation report written to {REPORT_FILE} ({len(results)} rows)")

if __name__ == "__main__":
    validate_with_duckdb()
