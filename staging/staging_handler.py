"""
Staging Handler - Orchestrates data staging operations.
Manages bronze layer (raw data) storage and validation.
"""

import json
import logging
from typing import List, Dict, Any, Union
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class StagingHandler:
    """
    Manages staging layer operations:
    - Store raw data to S3/MinIO (bronze layer)
    - Schema validation
    - Duplicate detection
    - Data timestamping
    """

    def __init__(self, staging_path: Union[str, Path] = None, 
                 storage_type: str = "local"):
        """
        Args:
            staging_path: Path for staging data (local or S3)
            storage_type: "local", "s3", or "minio"
        """
        self.staging_path = Path(staging_path) if staging_path else Path("./staging_data")
        self.storage_type = storage_type
        self.staging_path.mkdir(parents=True, exist_ok=True)

    def stage_raw_data(self, data: List[Dict], 
                      source_name: str, 
                      file_format: str = "json") -> str:
        """
        Store raw extracted data to staging layer.
        
        Args:
            data: Raw data from parser
            source_name: Source identifier (e.g., 'yfinance', 'investing_com')
            file_format: Output format ('json', 'csv', 'parquet')
        
        Returns:
            Path to staged data file
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_name}_{timestamp}.{file_format}"
        filepath = self.staging_path / source_name / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if file_format == "json":
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        elif file_format == "csv":
            # TODO: Implement CSV writing
            pass
        elif file_format == "parquet":
            # TODO: Implement Parquet writing
            pass

        logger.info(f"Staged {len(data)} records to {filepath}")
        return str(filepath)

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
            extraction_time = datetime.utcnow()

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
        """
        Get staging status for a source.
        
        Returns:
            Status including latest file, row count, etc.
        """
        source_path = self.staging_path / source_name
        
        if not source_path.exists():
            return {"status": "no_data", "source": source_name}

        files = sorted(source_path.glob("*.json"), reverse=True)
        if not files:
            return {"status": "no_files", "source": source_name}

        latest_file = files[0]
        with open(latest_file) as f:
            data = json.load(f)

        return {
            "status": "active",
            "source": source_name,
            "latest_file": str(latest_file),
            "file_count": len(files),
            "latest_row_count": len(data),
            "latest_timestamp": latest_file.stat().st_mtime
        }
