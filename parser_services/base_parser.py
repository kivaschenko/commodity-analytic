from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union

from pandas import DataFrame


class BaseParser(ABC):
    @abstractmethod
    def parse(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_results(
        self, results: 'Union[List[Dict[str, Any]], Any, DataFrame]', 
        filepath: str, 
        file_ext: str = "json", 
        storage_type: str = "local"
    ) -> None:
        pass
