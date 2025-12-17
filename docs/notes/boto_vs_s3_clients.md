both services use boto3

# boto

low-level & infrastructure management
connectivity, authentication and more low-level operations of aws API
func example: load_to_bronze(data: bytes, bucket: str, object_name: str)

Solo sabe: "Toma estos bytes y ponlos en esta ubicación". No sabe qué significan los bytes (si son JSON o Parquet) ni por qué elegiste esa ruta.

# s3

high-level management, pipeline logic
application/business
func example: write_bronze(json_data: dict, bucket: str, symbol: str)
Sabe: "Toma este json_data, codifícalo como JSON, genera el path de la Capa Bronze usando el symbol, y luego llama a la función de bajo nivel para guardarlo."
