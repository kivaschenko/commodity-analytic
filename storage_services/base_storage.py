from abc import ABC, abstractmethod
from typing import List, Any


class BaseStorageService(ABC):
    def __init__(self, layer: str = "bronze"):
        self.layer = layer
        self.bucket_name = None  # To be set by subclasses based on layer

    @abstractmethod
    def upload_file(self, file_path: str | Any, destination_path: str) -> None:
        pass

    @abstractmethod
    def download_file(self, source_path: str, destination_path: str) -> str:
        pass

    @abstractmethod
    def list_files(self, directory_path: str) -> List[str]:
        pass

    @abstractmethod
    def delete_file(self, file_path: str) -> None:
        pass
