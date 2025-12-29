# src/extract/test_extract.py

import json
import os
import time

import requests

# --- CONFIGURACIÓN ---
# ¡IMPORTANTE! Usando la clave API que proporcionaste
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "YELVS772CHCMKLEK")
SYMBOL_TO_TEST = "IBM"
BASE_URL = "https://www.alphavantage.co/query"
# El User-Agent que usaste en tu código
USER_AGENT_HEADER = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
# --- FIN CONFIGURACIÓN ---


def test_alpha_vantage_api_call():
    """Simula la llamada API usando el endpoint GRATUITO para diagnosticar la conexión."""

    if ALPHA_VANTAGE_API_KEY == "TU_API_KEY_AQUI" or not ALPHA_VANTAGE_API_KEY:
        print("🔴 ERROR: Por favor, configura tu clave API de Alpha Vantage.")
        return

    browser_headers = {
        "User-Agent": USER_AGENT_HEADER,
        "Accept": "application/json",
    }

    api_params = {
        # **FUNCIÓN GRATUITA (Weekly Adjusted)**
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "symbol": SYMBOL_TO_TEST,
        # 'interval' se ELIMINA, ya que no aplica para datos semanales/diarios
        "apikey": ALPHA_VANTAGE_API_KEY,
        "outputsize": "full",  # Para obtener todo el historial disponible
    }

    # 1. Aplicamos el delay
    print(f"\n--- INICIANDO PRUEBA LOCAL PARA {SYMBOL_TO_TEST} ---")
    print("Simulando delay de 5 segundos antes de la llamada (reducido para prueba)...")
    time.sleep(5)

    try:
        print(f"1. Conectando a {BASE_URL}...")

        # Realizar la solicitud
        response = requests.get(
            BASE_URL,
            params=api_params,
            timeout=45,
            verify=False,  # Manteniendo el verify=False de tu código
            headers=browser_headers,
        )
        response.raise_for_status()  # Maneja códigos HTTP 4xx/5xx

        # 2. Decodificar la respuesta JSON
        data = response.json()

        # 3. VERIFICACIÓN DE ÉXITO O FALLO DE API (Chequeando la clave de respuesta semanal)
        if "Weekly Adjusted Time Series" in data:
            print(f"✅ ÉXITO: Extracción semanal exitosa para {SYMBOL_TO_TEST} desde IP local.")
            num_records = len(data["Weekly Adjusted Time Series"])
            print(f"   Registros semanales recibidos: {num_records}")

            print("\n   => ¡El problema NO era el bloqueo de IP, sino el endpoint Premium!")

        elif "Error Message" in data:
            print(f"❌ FALLO DE API (Error de Clave o Parámetros): {data['Error Message']}")

        elif "Note" in data and "API call frequency" in data.get("Note", ""):
            print(f"❌ FALLO DE LÍMITE DE TASA (Note): {data.get('Note')}")

        else:
            # Si recibimos un JSON válido pero inesperado (ej. bloqueo suave)
            print("❌ FALLO: Respuesta inesperada del API.")
            print("Respuesta completa (JSON):")
            print(json.dumps(data, indent=2))

    except requests.exceptions.RequestException as e:
        print("🔴 FALLO CRÍTICO: RemoteDisconnected.")
        print("   Alpha Vantage cerró la conexión (Bloqueo de IP).")
        print(f"   Error: {e}")

    except requests.exceptions.RequestException as e:
        print(f"🔴 FALLO DE CONEXIÓN/HTTP: {e}")

    except Exception as e:
        print(f"🔴 FALLO INESPERADO: {e}")


# Ejecutar la prueba
if __name__ == "__main__":
    test_alpha_vantage_api_call()
