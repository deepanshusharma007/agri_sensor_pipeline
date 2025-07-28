import os
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Always refer to data/raw at the project root!
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
LOG_DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'ingestion_log.duckdb')
LOG_TABLE = 'ingestion_log'

def ingest_all_parquet_files():
    # Ensure both data/ and data/raw/ directories exist at the root
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    print(f"[INFO] Current working directory: {os.getcwd()}")
    print(f"[INFO] Ingesting files from: {os.path.abspath(RAW_DATA_DIR)}")

    con = duckdb.connect(LOG_DB_PATH)
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            filename VARCHAR PRIMARY KEY,
            success BOOLEAN,
            n_records INT,
            error_message VARCHAR
        )
    ''')

    files = [fname for fname in os.listdir(RAW_DATA_DIR)
             if fname.lower().endswith('.parquet')]

    if not files:
        print("[WARN] No parquet files found in data/raw/. Place files there and rerun.")
        con.close()
        return

    for fname in files:
        # Check if already ingested
        exists = con.execute(
            f"SELECT COUNT(*) FROM {LOG_TABLE} WHERE filename=? AND success=TRUE",
            (fname,)
        ).fetchone()[0]
        if exists:
            print(f"[SKIP] Already ingested: {fname}")
            continue

        fpath = os.path.join(RAW_DATA_DIR, fname)
        try:
            df = pd.read_parquet(fpath)
            schema = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{fpath}')"
            ).fetchdf()
            print(f"[INFO] Schema of {fname}:\n{schema}")
            n_records = len(df)
            con.execute(
                f"INSERT OR REPLACE INTO {LOG_TABLE} VALUES (?, ?, ?, ?)",
                (fname, True, n_records, None)
            )
            print(f"[OK] Ingested {fname}: {n_records} rows")
        except Exception as e:
            print(f"[FAIL] Failed to read {fname}: {e}")
            con.execute(
                f"INSERT OR REPLACE INTO {LOG_TABLE} VALUES (?, ?, ?, ?)",
                (fname, False, 0, str(e))
            )

    summary = con.execute(f"SELECT * FROM {LOG_TABLE}").fetchdf()
    print("[INFO] Ingestion summary:")
    print(summary)
    con.close()


if __name__ == "__main__":
    ingest_all_parquet_files()
