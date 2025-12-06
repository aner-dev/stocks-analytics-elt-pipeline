# src/discovery/hn_schema.py

# DuckDB/SQL syntax for type definition.
# This structure is derived from running the discovery script.
# It represents the clean, expected raw structure.

# Schema definition for hackernews.csv.gz file
HN_RAW_SCHEMA = {
    "id": "BIGINT",  # Used BIGINT as Float64 was wrongly inferred initially
    "deleted": "TINYINT",
    "type": "VARCHAR",
    "by": "VARCHAR",
    "time": "BIGINT",  # Unix timestamp (Original format)
    "text": "VARCHAR",
    "dead": "TINYINT",
    "parent": "BIGINT",
    "poll": "BIGINT",
    "kids": "VARCHAR",  # Stored as Array, but often comes in as String
    "url": "VARCHAR",
    "score": "BIGINT",
    "title": "VARCHAR",
    "parts": "VARCHAR",
    "descendants": "BIGINT",
}

# List of columns of interest and their order for future projections
HN_COLUMNS = list(HN_RAW_SCHEMA.keys())


# Example function to get the type definition for SQL
def get_hn_schema_sql() -> str:
    """Returns the column definition string for a CREATE TABLE or a projection."""
    return ", ".join([f"{col} {typ}" for col, typ in HN_RAW_SCHEMA.items()])
