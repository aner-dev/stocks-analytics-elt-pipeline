# test_delta.py
from deltalake import write_deltalake
import pandas as pd

# Datos de prueba
df = pd.DataFrame({"taxi_id": [1, 2, 3], "trip_distance": [2.5, 3.1, 1.8]})

# Probar Delta Lake
write_deltalake("./test_delta", df)
print("✅ Delta Lake funciona sin Spark!")
