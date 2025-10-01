import psycopg2
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "test" / "extracted_data.csv"
db = os.getenv(DATA_DIR)
data_load = db.copy_from()
print(f"loading csv into database: {data_load}")
