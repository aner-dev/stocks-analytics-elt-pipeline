import duckdb

from elt.config.logging_config import get_log.
log.= get_log.__name__)
 
log.= get_log.__name__)



def check_parquet_files():
    """verify and delete corrupt .parquet files"""
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_DIR = PROJECT_ROOT / "data" / "nyc_taxi" / "raw"

    conn = duckdb.connect()
    corrupt_files = []

    for parquet_file in DATA_DIR.glob("*.parquet"):
        try:
            # trying read the file
            conn.execute(f"SELECT 1 FROM read_parquet('{parquet_file}') LIMIT 1")
            log.info(f"☑️{parquet_file.name} - OK")
        except Exception as e:
            log.error(f" {parquet_file.name} - CORRUPT: {e}")
            corrupt_files.append(parquet_file)

    # delete corrupt files
    for corrupt_file in corrupt_files:
        corrupt_file.unlink()
        log.info(f"🗑️  deleted: {corrupt_file.name}")

    conn.close()
    return len(corrupt_files)


if __name__ == "__main__":
    corrupt_count = check_parquet_files()
    print(f" 󱊒 corrupt files deleted: {corrupt_count}")
