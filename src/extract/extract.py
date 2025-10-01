from pathlib import Path
import os
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent
ENV_FILE = PROJECT_ROOT / "config" / "api.env"

print(f"searching api.env in: {ENV_FILE}")

if not ENV_FILE.exists():
    print(f"❌ ERROR: {ENV_FILE} no encontrado")
    print("💡 Asegúrate de que api.env esté en el mismo directorio que extract.py")
    exit(1)
# Cargar configuración
load_dotenv(ENV_FILE)


URL = os.getenv("DATA_URL")
LIMIT = os.getenv("API_LIMIT")


def extract_and_save():
    try:
        print("Extrayendo datos...")
        response = requests.get(URL, params={"$limit": LIMIT}, timeout=30)
        response.raise_for_status()  # error-handling for the HTTPS connection

        content_type = response.headers.get("content-type", "")
        if "csv" not in content_type:
            print(f"Warning: content_type is {content_type}, expect csv")

        csv_text = response.text
        lines = csv_text.split("\n")

        print(f"data extracted: {len(lines)} lines")

        # Guardar como JSON para usar en tests
        # with open("tests/extracted_data.csv", "w", newline="", encoding="utf-8") as f:
        #    f.write(csv_text)

        output_csv_dir = Path(PROJECT_ROOT / "test/extracted_data.csv")
        output_csv = output_csv_dir.write_text(csv_text)
        print(f"csv data saved in {output_csv}")

        print("💾 Datos CSV guardados en test/extracted_data.csv")

        if lines:
            print(f"first line (headers): {lines[0]}")
            print(
                f"show data (line 2): {lines[1] if len(lines) > 1 else 'not data available'}"
            )

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = extract_and_save()
    exit(0 if success else 1)
