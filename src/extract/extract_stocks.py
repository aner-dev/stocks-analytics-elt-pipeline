# src/extract/alpha_extract.py (Extraction Utility Library)
import time
from typing import Any
import requests
import structlog
import traceback
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import (
    datetime,
    timezone,
)

log = structlog.get_logger()

# Data extraction function with error handling


def extract_stocks_data(
    base_url: str, api_params: dict[str, str], symbol: str = "N/A"
) -> dict[str, Any]:
    """
    Performs the request to the Alpha Vantage API with HTTP and business error handling.
    Incluye logging explícito del traceback para debugging en el entorno Airflow/Structlog.
    """

    # Objeto de retorno por defecto
    data = {}

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept-Encoding": "gzip",
        # "Connection": "close"  # OPTIONAL - try without this first
    }
    try:
        log.info("Applying 20-second rate limit delay.", symbol=symbol)
        time.sleep(25)

        session = requests.Session()
        retries = Retry(
            total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))

        log.info("Making API request via Robust Session", symbol=symbol)

        # Usamos stream=True para no saturar el buffer de red de 64k del contenedor
        response = session.get(base_url, params=api_params, headers=headers, timeout=30)

        response.raise_for_status()
        data = response.json()

        data_key = "Weekly Adjusted Time Series"  # Definir la clave esperada

        # 1. CAPTURA DE LÍMITE DE FRECUENCIA
        if "Note" in data and "API call frequency" in data.get("Note", ""):
            # ... (Lógica Rate Limit)
            raise ValueError(
                "Alpha Vantage Rate Limit Exceeded (5 calls/min or 500/day)."
            )

        if any(k in data for k in ["Error Message", "Note", "Information"]):
            # Pero solo si no es la nota de éxito (a veces Alpha Vantage pone notas informativas)
            if "Error Message" in data:
                log.error("API returned error", symbol=symbol, api_response=data)
                raise ValueError(f"API business error for {symbol}")

        # 3. VERIFICACIÓN DE ÉXITO Y REPORTE DE REGISTROS (CORRECCIÓN CRÍTICA)
        if data_key in data:
            record_count = len(data[data_key].keys())
            log.info("Data extraction complete", symbol=symbol, records=record_count)
        else:
            # Captura si la respuesta no tiene datos pero no es un error conocido
            log.error(
                "API response missing expected data key.",
                symbol=symbol,
                api_response=data,
            )
            raise ValueError(f"API response for {symbol} is malformed or empty.")

        # Estructura de retorno necesaria para XCom/Task Mapping
        return {
            "symbol": symbol,
            "raw_json": data,
            "extract_time": str(datetime.now(timezone.utc)),
        }

    except requests.RequestException as e:
        error_type = "RequestException"
        error_message = f"Network/MTU Error: {e}"
    except requests.JSONDecodeError as e:
        error_type = "JSONDecodeError"
        error_message = f"Invalid JSON response from API: {e}"
    except ValueError as e:
        error_type = "APIValueError"
        error_message = str(e)
    except Exception as e:
        # Cualquier otro error inesperado (ej. NameError, import, etc.)
        error_type = "UnknownError"
        error_message = f"Unexpected error: {e}"

    # --- BLOQUE CRÍTICO DE DEBUGGING ---

    # 1. Capturamos la traza completa antes de que Airflow la pierda
    full_traceback = traceback.format_exc()

    # 2. Registramos el error de forma detallada
    log.error(
        "!!! TASK FAILED (Full Traceback Forced) !!!",
        symbol=symbol,
        error_type=error_type,
        error_message=error_message,
        # Incluimos la traza completa en un campo del JSON
        full_traceback=full_traceback,
    )

    # 3. Relanzamos la excepción para que Airflow marque la tarea como fallida
    raise
