from tqdm import tqdm
import requests

import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time

from elt_pipeline_hn.config.logging_config import get_logger

log = structlog.get_logger()


PROJECT_ROOT = Path(__file__).parent.parent.parent
DOTENV_PATH = PROJECT_ROOT / "config" / "api.env"

if not DOTENV_PATH.exists():
    log.error(f"❌ .env file not found at: {DOTENV_PATH}")
    sys.exit(1)

load_dotenv(dotenv_path=DOTENV_PATH)

# --- Configuration from .env ---
NYC_TAXI_BASE_URL = os.getenv("NYC_TAXI_BASE_URL")
NYC_TAXI_DATA_TYPE = os.getenv("NYC_TAXI_DATA_TYPE", "yellow_tripdata")
NYC_TAXI_DOWNLOAD_DIR = Path(os.getenv("NYC_TAXI_DOWNLOAD_DIR", "data/nyc_taxi/raw"))
NYC_TAXI_DOWNLOAD_DELAY = int(os.getenv("NYC_TAXI_DOWNLOAD_DELAY", "1"))
NYC_TAXI_USER_AGENT = os.getenv(
    "NYC_TAXI_USER_AGENT", "SQL-Practice-Dataset-Collector/1.0"
)

# Parse dates from environment
NYC_TAXI_START_DATE = datetime.strptime(
    os.getenv("NYC_TAXI_START_DATE", "2020-01-01"), "%Y-%m-%d"
).date()
NYC_TAXI_END_DATE = datetime.strptime(
    os.getenv("NYC_TAXI_END_DATE", "2024-12-01"), "%Y-%m-%d"
).date()

# Construct full URL template
FULL_URL_TEMPLATE = (
    f"{NYC_TAXI_BASE_URL}/{NYC_TAXI_DATA_TYPE}_{{year}}-{{month:02d}}.parquet"
)

log.remove(0)


def validate_configuration() -> bool:
    """
    Validate that all required environment variables are properly configured

    Returns:
        bool: True if configuration is valid, False otherwise
    """
    required_vars = {
        "NYC_TAXI_BASE_URL": NYC_TAXI_BASE_URL,
        "NYC_TAXI_START_DATE": NYC_TAXI_START_DATE,
        "NYC_TAXI_END_DATE": NYC_TAXI_END_DATE,
    }

    missing_vars = [var for var, value in required_vars.items() if not value]

    if missing_vars:
        log.error(f"Missing required environment variables: {missing_vars}")
        log.info("Please ensure your .env file contains:")
        log.info("   NYC_TAXI_BASE_URL=https://d37ci6vzurychx.cloudfront.net/trip-data")
        log.info("   NYC_TAXI_START_DATE=2020-01-01")
        log.info("   NYC_TAXI_END_DATE=2024-12-01")
        return False

    log.info("✅ Configuration validated successfully")
    return True


def generate_monthly_urls(start_date: date, end_date: date) -> list:
    """
    Generate monthly URLs for the specified date range

    Args:
        start_date: Start date for the range
        end_date: End date for the range

    Returns:
        list: List of tuples containing (url, target_date)
    """
    urls = []
    current_date = start_date

    while current_date <= end_date:
        url = FULL_URL_TEMPLATE.format(year=current_date.year, month=current_date.month)
        urls.append((url, current_date))
        current_date += relativedelta(months=1)

    log.info(f"📅 Date range: {start_date} to {end_date}")
    log.info(f"🔗 Generated URLs: {len(urls)} months")

    return urls


def download_parquet_file(url: str, target_date: date) -> bool:
    """
    Download a single Parquet file with tqdm progress bar

    Args:
        url: The URL to download from
        target_date: Target date for filename generation

    Returns:
        bool: True if download was successful, False otherwise
    """
    filename = (
        f"{NYC_TAXI_DATA_TYPE}_{target_date.year}-{target_date.month:02d}.parquet"
    )
    filepath = NYC_TAXI_DOWNLOAD_DIR / filename

    # Check if file already exists and is valid
    if filepath.exists():
        file_size = filepath.stat().st_size
        if file_size > 1024:  # Consider files >1KB as valid
            size_mb = file_size / (1024 * 1024)
            log.info(f"✅ File exists: {filename} ({size_mb:.1f} MB)")
            return True
        else:
            # Remove corrupt/empty file
            filepath.unlink()
            log.warning(f"🗑️ Removed corrupt file: {filename}")

    log.info(f"⬇️ Downloading: {filename}")

    try:
        headers = {"User-Agent": NYC_TAXI_USER_AGENT}
        response = requests.get(url, stream=True, timeout=30, headers=headers)
        response.raise_for_status()

        # Get total size for progress bar
        total_size = int(response.headers.get("content-length", 0))

        # Create progress bar for individual file
        progress_bar = tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=filename[:20],  # Show truncated filename
            leave=False,  # Remove bar after completion
        )

        with open(filepath, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    progress_bar.update(len(chunk))  # Update progress bar

        progress_bar.close()  # Close the progress bar

        # Verify successful download
        if filepath.exists() and filepath.stat().st_size > 1024:
            size_mb = filepath.stat().st_size / (1024 * 1024)
            log.info(f"🎉 Download complete: {filename} ({size_mb:.1f} MB)")
            return True
        else:
            log.error(f"❌ File is empty or corrupt: {filename}")
            if filepath.exists():
                filepath.unlink()
            return False

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            log.warning(f"❌ File not available: {filename} (404)")
        else:
            log.error(f"❌ HTTP Error {e.response.status_code}: {filename}")
        return False
    except requests.exceptions.Timeout:
        log.error(f"⏰ Timeout downloading: {filename}")
        return False
    except Exception as e:
        log.error(f"❌ Error downloading {filename}: {str(e)}")
        # Clean up corrupted file
        if filepath.exists():
            filepath.unlink()
        return False


def calculate_download_statistics() -> dict:
    """
    Calculate statistics for downloaded files

    Returns:
        dict: Statistics including file count and total size
    """
    parquet_files = list(NYC_TAXI_DOWNLOAD_DIR.glob("*.parquet"))

    if not parquet_files:
        return {"file_count": 0, "total_gb": 0, "total_mb": 0, "avg_file_mb": 0}

    total_size = sum(f.stat().st_size for f in parquet_files)
    avg_file_size = total_size / len(parquet_files)

    return {
        "file_count": len(parquet_files),
        "total_gb": total_size / (1024**3),
        "total_mb": total_size / (1024**2),
        "avg_file_mb": avg_file_size / (1024**2),
    }


def main():
    """Main execution function"""
    print("🚕 NYC TAXI DATA BULK DOWNLOADER")
    print("=" * 50)

    # Validate configuration
    if not validate_configuration():
        return

    # Create download directory
    NYC_TAXI_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Generate URLs for the specified date range
    urls_with_dates = generate_monthly_urls(NYC_TAXI_START_DATE, NYC_TAXI_END_DATE)

    # Create overall progress bar
    overall_progress = tqdm(
        total=len(urls_with_dates), desc="📦 Overall Progress", unit="files"
    )

    # Download files with progress tracking
    successful_downloads = 0
    start_time = time.time()

    for index, (url, target_date) in enumerate(urls_with_dates, 1):
        # Update overall progress description with current file
        current_file = f"{target_date.year}-{target_date.month:02d}"
        overall_progress.set_description(f"📦 {current_file}")

        if download_parquet_file(url, target_date):
            successful_downloads += 1

        # Update overall progress
        overall_progress.update(1)

        # Rate limiting between downloads
        if index < len(urls_with_dates) and NYC_TAXI_DOWNLOAD_DELAY > 0:
            time.sleep(NYC_TAXI_DOWNLOAD_DELAY)

    # Close overall progress bar
    overall_progress.close()

    # Calculate total time
    total_time = time.time() - start_time

    # Generate final report
    stats = calculate_download_statistics()

    print(f"\n{'=' * 50}")
    print("📊 DOWNLOAD REPORT:")
    print(f"   📁 Files downloaded: {successful_downloads}/{len(urls_with_dates)}")
    print(f"   💾 Total size: {stats['total_gb']:.2f} GB ({stats['total_mb']:.1f} MB)")
    print(f"   ⏱️  Total time: {total_time:.1f}s")
    print(f"   📈 Average file size: {stats['avg_file_mb']:.1f} MB")
    print(f"   📍 Download directory: {NYC_TAXI_DOWNLOAD_DIR.absolute()}")

    # Provide recommendations based on total volume
    if stats["total_gb"] < 5:
        print("\n💡 RECOMMENDATION: For larger datasets, update your .env file with:")
        print("   NYC_TAXI_START_DATE=2018-01-01  # Earlier start date")
        print("   NYC_TAXI_DATA_TYPE=green_tripdata  # Additional dataset")


if __name__ == "__main__":
    main()
