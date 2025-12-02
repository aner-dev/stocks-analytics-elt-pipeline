import pyarrow
import pandas as pd
import psycopg2
from psycopg2 import extras
from sqlalchemy import create_engine
import glob
import os

# --- 1. CONFIGURACIÓN ---
DATABASE_URL = "dbname=parquet_db user=postgres password=postgres host=localhost"
DIRECTORIO_BASE = os.path.expanduser("~/code/projects/de/elt/data/nyc_taxi/raw/")
PATRON_BUSQUEDA = os.path.join(DIRECTORIO_BASE, "*.parquet")
NOMBRE_TABLA = "parquet_db"

# --- 2. BÚSQUEDA DE ARCHIVOS ---
try:
    lista_archivos_parquet = glob.glob(PATRON_BUSQUEDA)
    print(f"Archivos Parquet encontrados: {len(lista_archivos_parquet)}")
    if not lista_archivos_parquet:
        print("❌ No se encontraron archivos Parquet. Revisa la ruta y el patrón.")
        exit()
except Exception as e:
    print(f"Error al buscar archivos: {e}")
    exit()

# --- 3. CONEXIÓN A LA BASE DE DATOS (Preparación) ---
conn = None  # La conexión debe inicializarse a None
cur = None  # El cursor también, para un cierre seguro
engine = None

try:
    # 3.1. Establecer conexión con psycopg2
    # El resto del código de conexión aquí...
    # ...
    # Asumimos una URL simple para este ejemplo (ajusta si es necesario)
    url_parts = dict(x.split("=") for x in DATABASE_URL.split())
    db_url_sa = f"postgresql+psycopg2://{url_parts['user']}:{url_parts['password']}@{url_parts['host']}/{url_parts['dbname']}"
    engine = create_engine(db_url_sa)

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()
    print("✅ Conexión a la base de datos establecida.")

except Exception as error:
    print(f"❌ Error al conectar a la base de datos: {error}")
    # Si la conexión falla, cerramos si se llegó a abrir
    if conn:
        conn.close()
    exit()

# Bandera para saber si la tabla ya fue creada
tabla_creada = False

FILAS_POR_ARCHIVO = 10000

# --- 4. PROCESAR Y CARGAR ARCHIVOS ---
try:  # Nuevo try/except/finally global para manejo de conexión/cierre
    for i, archivo in enumerate(lista_archivos_parquet):
        print(f"\n[{i + 1}/{len(lista_archivos_parquet)}] Leyendo: {archivo}...")
        try:
            # 4.1. LECTURA DEL PARQUET CON PANDAS
            df = pd.read_parquet(archivo, engine="pyarrow")
            df = df.head(FILAS_POR_ARCHIVO)

            # 4.2. LIMPIEZA DE COLUMNAS
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]
            columnas = df.columns.tolist()

            # 4.3. CREACIÓN DE LA TABLA (Solo la primera vez)
            if not tabla_creada:
                print(
                    f"Creando o reemplazando la estructura de la tabla '{NOMBRE_TABLA}'..."
                )
                df.head(0).to_sql(
                    NOMBRE_TABLA, engine, if_exists="replace", index=False
                )
                tabla_creada = True
                print("✅ Estructura de la tabla creada.")

            # 4.4. PREPARAR Y CARGAR DATOS
            datos_a_insertar = [
                tuple(row) for row in df.itertuples(index=False, name=None)
            ]
            print(f"Cargando {len(datos_a_insertar)} filas en '{NOMBRE_TABLA}'...")

            columnas_str = ", ".join(columnas)
            insert_query = f"INSERT INTO {NOMBRE_TABLA} ({columnas_str}) VALUES %s"

            extras.execute_values(
                cur,
                insert_query,
                datos_a_insertar,
                page_size=10000,
            )

            # 4.5. Confirmar
            conn.commit()
            print(f"✅ Archivo '{os.path.basename(archivo)}' cargado exitosamente.")

        except Exception as error:
            print(f"❌ Error al procesar o cargar el archivo {archivo}: {error}")
            # Solo hacemos ROLLBACK, NO cerramos la conexión aquí.
            if conn:
                conn.rollback()

except Exception as global_error:
    # Este bloque solo captura errores que ocurren FUERA del bucle,
    # por ejemplo, si 'conn' no se inicializó correctamente o hay un error de conexión persistente.
    print(
        f"❌ ERROR GLOBAL: La carga ha terminado por una excepción no controlada: {global_error}"
    )

# --- 5. CIERRE DE CONEXIÓN (¡CRUCIAL!) ---
finally:
    if conn:
        conn.close()
        print("\nConexión a la base de datos cerrada.")
