# src/extract/alpha_extract.py (Extraction Utility Library)
import time
import traceback
from datetime import (
    datetime,
    timezone,
)
from typing import Any

import requests
import structlog
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

log = structlog.get_logger()

# Data extraction function with error handling


def extract_stocks_data(
    base_url: str, api_params: dict[str, str], symbol: str = "N/A"
) -> dict[str, Any]:
    """
    Performs the request to the Alpha Vantage API with HTTP and business error handling.
    Incluye logging explícito del traceback para debugging en el entorno Airflow/Structlog.
    """

    # default return object
    data = {}

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept-Encoding": "gzip",
        # "Connection": "close"  (OPTIONAL - the extraction functions correct without it)
    }
    try:
        log.info("Applying 20-second rate limit delay.", symbol=symbol)
        time.sleep(25)

        session = requests.Session()
        retries = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
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
            raise ValueError("Alpha Vantage Rate Limit Exceeded (5 calls/min or 500/day).")

        if any(k in data for k in ["Error Message", "Note", "Information"]):
            # Pero solo si no es la nota de éxito (a veces Alpha Vantage pone notas informativas)
            if "Error Message" in data:
                log.error("API returned error", symbol=symbol, api_response=data)
                return {
                    "symbol": symbol,
                    "raw_json": None,
                    "success": False,
                    "error_msg": f"API business error: {data.get('Error Message')}",
                    "error_type": "API_INVALID_CALL",
                }

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
            "success": True,
        }
    except (
        requests.RequestException,
        requests.JSONDecodeError,
        ValueError,
        Exception,
    ) as e:
        # 1. Determinamos el tipo de error manteniendo tu granularidad
        if isinstance(e, requests.RequestException):
            error_type = "RequestException"
            error_message = f"Network/MTU Error: {e}"
        elif isinstance(e, requests.JSONDecodeError):
            error_type = "JSONDecodeError"
            error_message = f"Invalid JSON response from API: {e}"
        elif isinstance(e, ValueError):
            error_type = "APIValueError"
            error_message = str(e)
        else:
            error_type = type(e).__name__
            error_message = str(e)

        # 2. Capturamos el traceback para TODOS los errores
        full_traceback = traceback.format_exc()

        # 3. Logeamos el error (Soft Fail)
        log.error(
            "!!! SOFT FAIL CAPTURED !!!",
            symbol=symbol,
            error_type=error_type,
            error_message=error_message,
            full_traceback=full_traceback,
        )

        # 4. Retornamos el diccionario de error (Esto evita que el DAG muera)
        return {
            "symbol": symbol,
            "raw_json": None,
            "success": False,
            "error_msg": error_message,
            "error_type": error_type,
        }
