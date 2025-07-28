import os
import pandas as pd
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Always refer to data/raw at the project root!
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed')

CALIBRATION = {
    'temperature': (1.02, -0.4, -30, 60),
    'humidity': (0.98, 1.5, 0, 100),
    'soil_moisture': (1, 0, 0, 100),
    'light_intensity': (1, 0, 0, 100000)
}

def calibrate(row):
    params = CALIBRATION.get(row['reading_type'], (1, 0, -np.inf, np.inf))
    val = row['value'] * params[0] + params[1]
    anomalous = int(val < params[2] or val > params[3])
    return pd.Series([val, anomalous])

def clean_and_transform(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['sensor_id', 'timestamp', 'reading_type', 'value'])
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    # Make sure battery_level stays; clean (coerce bad values to NaN)
    df['battery_level'] = pd.to_numeric(df.get('battery_level', np.nan), errors='coerce')
    # Optionally drop battery_level out of range
    # df = df[(df['battery_level'] >= 0) & (df['battery_level'] <= 100)]

    df['zscore'] = df.groupby(['sensor_id', 'reading_type'])['value'].transform(
        lambda x: (x - x.mean()) / x.std(ddof=0) if len(x) > 1 and x.std(ddof=0)!=0 else 0
    )
    df = df[df['zscore'].abs() <= 3]
    df = df.dropna(subset=['value'])

    # Adjust to UTC+5:30
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True) + pd.Timedelta(hours=5, minutes=30)
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    df[['calibrated_value', 'anomalous_reading']] = df.apply(calibrate, axis=1)
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df['daily_avg'] = df.groupby(['sensor_id', 'reading_type', 'date'])['calibrated_value'].transform('mean')
    df = df.sort_values('timestamp')
    df['7day_rolling_avg'] = df.groupby(['sensor_id', 'reading_type'])['calibrated_value'].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )

    # Output all relevant columns
    columns = [
        'sensor_id','timestamp','reading_type','value','battery_level',
        'calibrated_value','anomalous_reading',
        'date','daily_avg','7day_rolling_avg'
    ]
    return df[columns]

def process_and_save_all():
    for fname in os.listdir(RAW_DATA_DIR):
        if not fname.lower().endswith('.parquet'):
            continue
        fpath = os.path.join(RAW_DATA_DIR, fname)
        try:
            df = pd.read_parquet(fpath)
            df_clean = clean_and_transform(df)
            if not df_clean.empty:
                for d in df_clean['date'].unique():
                    odir = os.path.join(PROCESSED_DATA_DIR, str(d))
                    os.makedirs(odir, exist_ok=True)
                    orig_name = os.path.splitext(fname)[0]
                    outpath = os.path.join(odir, f"{orig_name}_transformed.parquet")
                    
                    # ------ NEW: Skip if already processed
                    if os.path.exists(outpath):
                        print(f"[SKIP] Already transformed: {outpath}")
                        continue
                    # ------

                    df_out = df_clean[df_clean['date'] == d]
                    df_out.to_parquet(outpath, index=False, compression='gzip')
                    print(f"[OK] Saved: {outpath} ({len(df_out)} rows)")
            else:
                print(f"[WARN] {fname} produced no data after cleaning.")
        except Exception as e:
            print(f"[ERR] Error processing {fname}: {e}")


if __name__ == "__main__":
    process_and_save_all()
