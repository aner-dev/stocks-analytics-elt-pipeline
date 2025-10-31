from download_csv import download_csv
import duckdb


def check_alpha_structure():
    # Descargar CSV fresco
    csv_path = download_csv("data", "extracted_data.csv")

    conn = duckdb.connect()

    print("=== COLUMNAS REALES ===")
    schema = conn.execute(f"DESCRIBE SELECT * FROM '{csv_path}'").fetchall()
    for col in schema:
        print(f"{col[0]}: {col[1]}")

    print("\n=== PRIMERAS 2 FILAS ===")
    sample = conn.execute(f"SELECT * FROM '{csv_path}' LIMIT 2").fetchall()
    for row in sample:
        print(row)

    conn.close()


if __name__ == "__main__":
    check_alpha_structure()
