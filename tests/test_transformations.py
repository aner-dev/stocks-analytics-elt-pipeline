import polars as pl


def test_no_negative_prices():
    # Simulate input data
    df = pl.DataFrame(
        {
            "symbol": ["AAPL", "TSLA"],
            "close": [150.0, -10.0],  # Un error común de API o ingesta
        }
    )

    # The test would be fail if find negative prices
    negative_prices = df.filter(pl.col("close") < 0)
    assert negative_prices.height == 0, (
        f"Encontrados {negative_prices.height} registros con precios negativos"
    )


def test_schema_integrity():
    df = pl.DataFrame({"price": [10.5], "volume": [100]})
    # Types Validation for DWH
    assert df.schema["price"] == pl.Float64
    assert df.schema["volume"] == pl.Int64
