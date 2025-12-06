


load_dir = Path(__file__).parent.parent / "load"
sys.path.append(str(load_dir))

from load.S3_client import write_bronze
import polars as pl

raw_data = write_bronze("stocks-data")

if "Weekly Adjusted Time Series" in raw_data:
    time_series = raw_data["Weekly Adjusted Time Series"]
    records = [{"timestamp": date, **values} for date, values in time_series.items()]
    df = pl.DataFrame(records)
    print("shape", df.shape)
    print("data types", df.dtypes)
    print("flags", df.flags)
    print("columns", df.columns)
