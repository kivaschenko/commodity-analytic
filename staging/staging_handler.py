"""
Staging Handler - Orchestrates data staging operations.
Manages bronze layer (raw data) storage and validation.
"""

import json
import tempfile
from typing import List, Dict, Any, Union, Optional
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from storage_services import (
    BaseStorageService,
    HetznerStorageService,
    MinioStorageService,
)
from config.settings import settings
from logger import logger


BASE_DIR = Path(__file__).resolve().parent.parent


class StagingHandler:
    """
    Manages staging layer operations:
    - Store raw data to remote object storage
    - Load staged records
    - Keep bronze layer as JSON
    - Keep silver layer as Parquet
    """

    def __init__(
        self,
        staging_path: Union[str, Path, None] = None,
        storage_type: str = "minio",
        layer: str = "bronze",
    ):
        """
        Args:
            staging_path: Local staging path used when no remote storage is configured.
            storage_type: "minio", "hetzner", or "local".  # 'local' is not implemented yet, but reserved for future use.
            layer: "bronze" or "silver".
            storage_service: Optional explicit storage service instance.
        """
        self.layer = layer.lower()
        self.storage_type = storage_type.lower()
        self.staging_path = (
            Path(staging_path) if staging_path else BASE_DIR / "staging_data"
        )
        self.storage_service: Optional[BaseStorageService] = (
            MinioStorageService(self.layer)
            if self.storage_type == "minio"
            else HetznerStorageService(self.layer)
        )
        self.bronze_bucket = settings.bronze_bucket
        self.silver_bucket = settings.silver_bucket

    def _validate_file_format(self, file_format: Optional[str]) -> str:
        if file_format:
            file_format = file_format.lower()
        else:
            file_format = "json" if self.layer == "bronze" else "parquet"

        if self.layer == "bronze" and file_format != "json":
            raise ValueError("Bronze layer only supports JSON format")

        if self.layer == "silver" and file_format != "parquet":
            raise ValueError("Silver layer only supports Parquet format")

        return file_format

    def _write_data(
        self, destination: Path, data: List[Dict], file_format: str
    ) -> None:
        if file_format == "json":
            with destination.open("w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, default=str)
            return

        if file_format == "parquet":
            df = pd.DataFrame(data)
            df.to_parquet(destination, engine="pyarrow", index=False)
            return

        raise ValueError(f"Unsupported file format: {file_format}")

    def _read_data(self, source_path: Path) -> List[Dict]:
        suffix = source_path.suffix.lower()
        if suffix == ".json":
            return json.loads(source_path.read_text(encoding="utf-8"))

        if suffix == ".parquet":
            return pd.read_parquet(source_path).to_dict("records")

        raise ValueError(f"Unsupported staged file format: {suffix}")

    def stage_raw_data(
        self,
        data: List[Dict],
        source_name: str,
        file_format: Optional[str] = None,
    ) -> str:
        """
        Store staged data in the configured layer.

        Bronze layer remains JSON. Silver layer is written as Parquet.
        """
        file_format = self._validate_file_format(file_format)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{source_name}_{timestamp}.{file_format}"
        object_key = f"{source_name}/{filename}"

        if self.storage_service is not None:
            with tempfile.NamedTemporaryFile(
                suffix=f".{file_format}", delete=False
            ) as tmp:
                tmp_path = Path(tmp.name)
            try:
                self._write_data(tmp_path, data, file_format)
                self.storage_service.upload_file(str(tmp_path), object_key)
                staged_path = f"s3://{self.storage_service.bucket_name}/{object_key}"
                logger.info("Staged %s records to %s", len(data), staged_path)
                return staged_path
            finally:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except OSError:
                        pass

        local_path = self.staging_path / source_name / filename
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_data(local_path, data, file_format)
        logger.info("Staged %s records to %s", len(data), local_path)
        return str(local_path)

    def add_staging_metadata(self, data: List[Dict], 
                            source_name: str,
                            extraction_time: datetime = None) -> List[Dict]:
        """
        Add metadata to raw data records.
        
        Args:
            data: Raw records
            source_name: Data source identifier
            extraction_time: When data was extracted
        
        Returns:
            Data with added metadata columns
        """
        if extraction_time is None:
            extraction_time = datetime.now(timezone.utc)

        enriched_data = []
        for record in data:
            enriched = {
                "_staging_timestamp": extraction_time.isoformat(),
                "_source": source_name,
                "_staging_id": f"{source_name}_{extraction_time.timestamp()}",
                **record
            }
            enriched_data.append(enriched)

        return enriched_data

    def get_staging_status(self, source_name: str) -> Dict[str, Any]:
        """Return the current staging status for the requested source."""
        suffix = ".json" if self.layer == "bronze" else ".parquet"
        prefix = f"{source_name}/"

        if self.storage_service is not None:
            keys = self.storage_service.list_files(prefix)
            objects = [key for key in keys if key.endswith(suffix)]
            if not objects:
                return {"status": "no_data", "source": source_name}

            latest_key = sorted(objects, reverse=True)[0]
            return {
                "status": "active",
                "source": source_name,
                "latest_file": f"s3://{self.storage_service.bucket_name}/{latest_key}",
                "file_count": len(objects),
                "latest_row_count": None,
                "latest_timestamp": None,
            }

        source_path = self.staging_path / source_name
        if not source_path.exists():
            return {"status": "no_data", "source": source_name}

        files = sorted(source_path.glob(f"*{suffix}"), reverse=True)
        if not files:
            return {"status": "no_data", "source": source_name}

        latest_file = files[0]
        row_count = None
        if suffix == ".json":
            row_count = len(json.loads(latest_file.read_text(encoding="utf-8")))

        return {
            "status": "active",
            "source": source_name,
            "latest_file": str(latest_file),
            "file_count": len(files),
            "latest_row_count": row_count,
            "latest_timestamp": latest_file.stat().st_mtime,
        }

    def load_latest_records(self, source_name: str) -> List[Dict]:
        """Load the latest staged records for a source."""
        status = self.get_staging_status(source_name)
        if status.get("status") != "active":
            return []

        latest_file = status["latest_file"]
        if latest_file.startswith("s3://"):
            bucket_key = latest_file.replace("s3://", "", 1)
            bucket, key = bucket_key.split("/", 1)
            suffix = Path(key).suffix.lower()
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                self.storage_service.download_file(key, str(tmp_path))  # type: ignore
                return self._read_data(tmp_path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()

        return self._read_data(Path(latest_file))
