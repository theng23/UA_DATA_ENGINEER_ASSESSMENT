import json
import logging
import os
import random
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union


# =========================================================
# CONFIG
# =========================================================
def resolve_base_dir() -> Path:
    """
    Resolve project root robustly across:
    - .py script
    - Jupyter/Notebook
    - VS Code interactive
    - terminal run

    Priority:
      1) PROJECT_ROOT env var
      2) folder containing this script
      3) current working dir if it looks like project root
      4) walk up parents of cwd to find data/raw
      5) fallback to cwd
    """
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        p = Path(env_root).expanduser().resolve()
        if p.exists():
            return p

    if "__file__" in globals():
        return Path(__file__).resolve().parent

    cwd = Path.cwd().resolve()

    if (cwd / "data" / "raw").exists():
        return cwd

    for candidate in [cwd, *cwd.parents]:
        if (candidate / "data" / "raw").exists():
            return candidate

    return cwd


BASE_DIR = resolve_base_dir()
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
LOG_DIR = BASE_DIR / "logs"

HEADERS_SOURCE = RAW_DIR / "mock_orders_headers.json"
DETAILS_SOURCE = RAW_DIR / "mock_orders_details.json"

CONFIG = {
    "api_mode": "file",   # "file" or "http"
    "timeout_seconds": 10,
    "auth": {
        "enabled": False,
        "bearer_token": None
        # Production change point:
        # set enabled=True and provide bearer_token.
        # Authentication logic is centralized in get_auth_headers().
    },
    "retry": {
        "max_attempts": 3,
        "base_delay_seconds": 1,
        "jitter": True
    },
    "http": {
        # Example placeholders for future real API mode
        "headers_url": None,
        "details_url": None
    }
}


# =========================================================
# LOGGING
# =========================================================
def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "ingestion.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def log_ingestion_error(
    endpoint: str,
    error_type: str,
    error_message: str,
    raw_response: Optional[str] = None,
    request_page: Optional[int] = None,
    ingestion_ts: Optional[str] = None
) -> None:
    """
    Structured error log for recoverable ingestion issues,
    especially malformed JSON.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    error_record = {
        "endpoint": endpoint,
        "request_page": request_page,
        "raw_response": raw_response[:1000] if raw_response else None,
        "error_type": error_type,
        "error_message": error_message,
        "ingestion_ts": ingestion_ts or datetime.now(timezone.utc).isoformat()
    }

    error_log_path = LOG_DIR / "ingestion_errors.log"
    with open(error_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(error_record, ensure_ascii=False) + "\n")

    logging.error(f"INGESTION ERROR | {json.dumps(error_record, ensure_ascii=False)}")


# =========================================================
# EXCEPTIONS
# =========================================================
class APIIngestionError(Exception):
    """Base exception for all ingestion errors."""


class APIConnectionError(APIIngestionError):
    """Transient connection or request failure."""


class APISourceNotFoundError(APIIngestionError):
    """Local mock source file is missing."""


class APIEmptyResponseError(APIIngestionError):
    """Response is empty."""


class APIResponseFormatError(APIIngestionError):
    """Unexpected response structure."""


class APIHTTPError(APIIngestionError):
    """HTTP error with attached status code."""
    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


# =========================================================
# RETRY LOGIC
# =========================================================
def should_retry(exception: Exception) -> bool:
    """
    Retry only transient failures:
    - HTTP 5xx -> retry
    - connection timeout / request failure -> retry
    Do NOT retry:
    - HTTP 4xx
    - source file missing
    - malformed JSON
    - empty response
    """
    if isinstance(exception, APIHTTPError):
        return exception.status_code >= 500

    if isinstance(exception, APIConnectionError):
        return True

    if isinstance(exception, APISourceNotFoundError):
        return False

    return False


def with_retry(func, *args, config: Dict[str, Any], endpoint_name: str, **kwargs):
    retry_cfg = config.get("retry", {})
    max_attempts = retry_cfg.get("max_attempts", 3)
    base_delay = retry_cfg.get("base_delay_seconds", 1)
    jitter = retry_cfg.get("jitter", True)

    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)

        except Exception as e:
            last_exception = e

            if not should_retry(e):
                logging.warning(
                    f"{endpoint_name}: non-retryable error on attempt {attempt}: {e}"
                )
                raise

            if attempt == max_attempts:
                logging.error(f"{endpoint_name}: all {max_attempts} attempts failed")
                raise

            delay = base_delay * (2 ** (attempt - 1))
            if jitter:
                delay += random.uniform(0, 0.5)

            logging.warning(
                f"{endpoint_name}: attempt {attempt}/{max_attempts} failed ({e}). "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)

    raise last_exception


# =========================================================
# API CLIENT
# =========================================================
class APIClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def get_auth_headers(self) -> Dict[str, str]:
        """
        Single place to control authentication.
        This is the only place that needs changing
        when auth changes.
        """
        headers = {"Content-Type": "application/json"}
        auth_cfg = self.config.get("auth", {})

        if auth_cfg.get("enabled") and auth_cfg.get("bearer_token"):
            headers["Authorization"] = f"Bearer {auth_cfg['bearer_token']}"

        return headers

    def get(
        self,
        source: Union[Path, str],
        endpoint_name: str,
        page_num: Optional[int] = None
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        mode = self.config.get("api_mode", "file")
        logging.info(f"Calling endpoint: {endpoint_name} | mode={mode}")

        if mode == "file":
            return self._get_from_file(Path(source), endpoint_name, page_num=page_num)

        if mode == "http":
            return with_retry(
                self._get_from_http_once,
                source,
                endpoint_name,
                page_num=page_num,
                config=self.config,
                endpoint_name=endpoint_name
            )

        raise ValueError(f"Unsupported api_mode: {mode}")

    def _get_from_file(
        self,
        file_path: Path,
        endpoint_name: str,
        page_num: Optional[int] = None
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Read local JSON file as mock API response.

        Supports:
        - current format: JSON list
        - future-ready format: {"data": [...], "next_page": "..."}
        """
        try:
            if not file_path.exists():
                raise APISourceNotFoundError(
                    f"{endpoint_name}: source file not found: {file_path.resolve()}"
                )

            with open(file_path, "r", encoding="utf-8") as f:
                raw_text = f.read()

            if not raw_text.strip():
                raise APIEmptyResponseError(f"{endpoint_name}: empty response")

            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError as e:
                log_ingestion_error(
                    endpoint=endpoint_name,
                    error_type="MalformedJSON",
                    error_message=str(e),
                    raw_response=raw_text,
                    request_page=page_num
                )
                logging.warning(f"{endpoint_name}: malformed JSON — skipping and returning empty list")
                return []

            if isinstance(data, list):
                logging.info(f"{endpoint_name}: retrieved {len(data)} records")
                return data

            if isinstance(data, dict):
                # Future pagination-compatible format
                page_data = data.get("data", [])
                if page_data is None:
                    page_data = []
                if not isinstance(page_data, list):
                    raise APIResponseFormatError(
                        f"{endpoint_name}: dict response must contain list under 'data'"
                    )
                logging.info(
                    f"{endpoint_name}: retrieved {len(page_data)} records from dict response"
                )
                return data

            raise APIResponseFormatError(
                f"{endpoint_name}: expected JSON list or dict response"
            )

        except OSError as e:
            raise APIConnectionError(f"{endpoint_name}: failed to read file: {e}") from e

    def _get_from_http_once(
        self,
        url: str,
        endpoint_name: str,
        page_num: Optional[int] = None
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        import requests

        try:
            response = requests.get(
                url,
                headers=self.get_auth_headers(),
                timeout=self.config.get("timeout_seconds", 10)
            )

            if 400 <= response.status_code < 500:
                raise APIHTTPError(
                    f"{endpoint_name}: HTTP {response.status_code} client error — will NOT retry",
                    status_code=response.status_code
                )

            if 500 <= response.status_code < 600:
                raise APIHTTPError(
                    f"{endpoint_name}: HTTP {response.status_code} server error — will retry",
                    status_code=response.status_code
                )

            raw_text = response.text
            if not raw_text.strip():
                raise APIEmptyResponseError(f"{endpoint_name}: empty response")

            try:
                data = response.json()
            except ValueError as e:
                log_ingestion_error(
                    endpoint=endpoint_name,
                    error_type="MalformedJSON",
                    error_message=str(e),
                    raw_response=raw_text,
                    request_page=page_num
                )
                logging.warning(f"{endpoint_name}: malformed JSON from HTTP — skipping")
                return []

            if isinstance(data, list):
                logging.info(f"{endpoint_name}: retrieved {len(data)} records")
                return data

            if isinstance(data, dict):
                page_data = data.get("data", [])
                if page_data is None:
                    page_data = []
                if not isinstance(page_data, list):
                    raise APIResponseFormatError(
                        f"{endpoint_name}: dict response must contain list under 'data'"
                    )
                logging.info(
                    f"{endpoint_name}: retrieved {len(page_data)} records from dict response"
                )
                return data

            raise APIResponseFormatError(
                f"{endpoint_name}: expected JSON list or dict response"
            )

        except requests.Timeout as e:
            raise APIConnectionError(f"{endpoint_name}: connection timeout") from e
        except requests.RequestException as e:
            raise APIConnectionError(f"{endpoint_name}: request failed: {e}") from e


# =========================================================
# VALIDATION / DEBUG
# =========================================================
def log_runtime_paths() -> None:
    logging.info(f"BASE_DIR: {BASE_DIR}")
    logging.info(f"RAW_DIR: {RAW_DIR}")
    logging.info(f"BRONZE_DIR: {BRONZE_DIR}")
    logging.info(f"LOG_DIR: {LOG_DIR}")
    logging.info(f"HEADERS_SOURCE: {HEADERS_SOURCE}")
    logging.info(f"DETAILS_SOURCE: {DETAILS_SOURCE}")
    logging.info(f"HEADERS_SOURCE exists: {HEADERS_SOURCE.exists()}")
    logging.info(f"DETAILS_SOURCE exists: {DETAILS_SOURCE.exists()}")


def validate_required_sources() -> None:
    """
    Validate local mock files early when running in file mode.
    """
    if CONFIG.get("api_mode") != "file":
        return

    missing = []

    if not HEADERS_SOURCE.exists():
        missing.append(str(HEADERS_SOURCE.resolve()))

    if not DETAILS_SOURCE.exists():
        missing.append(str(DETAILS_SOURCE.resolve()))

    if missing:
        raise APISourceNotFoundError(
            f"Required mock API files not found. BASE_DIR={BASE_DIR} | missing={missing}"
        )


# =========================================================
# PAGINATION
# =========================================================
def fetch_with_pagination(
    client: APIClient,
    source: Union[Path, str],
    endpoint_name: str
) -> List[Dict[str, Any]]:
    """
    Supports both:
    Format A:
      [ {...}, {...} ]

    Format B:
      {
        "data": [ {...}, {...} ],
        "next_page": "cursor_123"
      }
    """
    all_records: List[Dict[str, Any]] = []
    page_num = 0
    current_source = source

    while True:
        page_num += 1
        logging.info(f"{endpoint_name}: fetching page {page_num}")

        response_data = client.get(current_source, endpoint_name, page_num=page_num)

        if isinstance(response_data, list):
            all_records.extend(response_data)
            logging.info(
                f"{endpoint_name}: plain list format, no pagination. Total={len(all_records)}"
            )
            break

        if isinstance(response_data, dict):
            page_data = response_data.get("data", [])
            next_page = response_data.get("next_page")

            all_records.extend(page_data)

            if not next_page:
                logging.info(f"{endpoint_name}: pagination complete. Total={len(all_records)}")
                break

            logging.info(
                f"{endpoint_name}: page {page_num} done, next_page={next_page}"
            )

            # Only meaningful for real HTTP mode
            if CONFIG.get("api_mode") == "http":
                separator = "&" if "?" in str(source) else "?"
                current_source = f"{source}{separator}cursor={next_page}"
            else:
                # File mode cannot actually paginate to another file;
                # break safely after first page dict response
                logging.info(
                    f"{endpoint_name}: file mode detected dict response with next_page; "
                    f"stopping after current page."
                )
                break
            continue

        logging.warning(f"{endpoint_name}: unexpected response type {type(response_data)}")
        break

    return all_records


# =========================================================
# FIELD MAPPING
# =========================================================
def map_order_headers(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "order_id": r.get("id"),
            "customer_id": r.get("userId"),
            "order_code": r.get("title"),
            "order_notes": r.get("body")
        }
        for r in records
    ]


def map_order_details(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "line_id": r.get("id"),
            "order_id": r.get("postId"),
            "line_description": r.get("name"),
            "buyer_contact": r.get("email"),
            "line_notes": r.get("body")
        }
        for r in records
    ]


# =========================================================
# BRONZE SAVE
# =========================================================
def add_ingestion_timestamp(
    records: List[Dict[str, Any]],
    ingestion_ts: str
) -> List[Dict[str, Any]]:
    return [{**r, "ingestion_timestamp": ingestion_ts} for r in records]


def save_bronze(records: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logging.info(f"Saved Bronze: {output_path.resolve()} | records={len(records)}")


# =========================================================
# JOIN
# =========================================================
def join_orders_and_lines(
    order_headers: List[Dict[str, Any]],
    order_lines: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    lines_by_order: Dict[Any, List[Dict[str, Any]]] = {}

    for line in order_lines:
        oid = line.get("order_id")
        lines_by_order.setdefault(oid, []).append(line)

    joined_orders = []
    for header in order_headers:
        oid = header.get("order_id")
        joined_orders.append(
            {
                **header,
                "line_items": lines_by_order.get(oid, [])
            }
        )

    logging.info(f"Joined: {len(joined_orders)} orders")
    return joined_orders


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    setup_logging()
    logging.info("=== ERP API Ingestion START ===")

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    client = APIClient(CONFIG)

    try:
        log_runtime_paths()
        validate_required_sources()

        # Resolve sources by mode
        if CONFIG.get("api_mode") == "file":
            headers_source = HEADERS_SOURCE
            details_source = DETAILS_SOURCE
        else:
            headers_source = CONFIG["http"].get("headers_url")
            details_source = CONFIG["http"].get("details_url")

            if not headers_source or not details_source:
                raise APIIngestionError(
                    "HTTP mode requires CONFIG['http']['headers_url'] and CONFIG['http']['details_url']"
                )

        # Step 1: Fetch
        raw_headers = fetch_with_pagination(client, headers_source, "GET /orders/headers")
        raw_details = fetch_with_pagination(client, details_source, "GET /orders/lines")

        # Step 2: Save Bronze
        save_bronze(
            add_ingestion_timestamp(raw_headers, ingestion_ts),
            BRONZE_DIR / "orders_headers_bronze.json"
        )
        save_bronze(
            add_ingestion_timestamp(raw_details, ingestion_ts),
            BRONZE_DIR / "orders_details_bronze.json"
        )

        # Step 3: Map
        mapped_headers = map_order_headers(raw_headers)
        mapped_details = map_order_details(raw_details)
        logging.info(f"Mapped: {len(mapped_headers)} headers, {len(mapped_details)} details")

        # Step 4: Join
        joined_orders = join_orders_and_lines(mapped_headers, mapped_details)

        if joined_orders:
            logging.info("Sample joined order:")
            logging.info(json.dumps(joined_orders[0], ensure_ascii=False, indent=2))

        logging.info("=== ERP API Ingestion COMPLETE ===")

    except APIIngestionError as e:
        logging.error(f"Ingestion failed: {e}")
        raise
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()