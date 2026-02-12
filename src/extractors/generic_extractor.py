from typing import List, Dict, Any
from src.extractors.base_extractor import BaseExtractor
from src.config import logger
import yaml

class GenericExtractor(BaseExtractor):
    """
    A concrete extractor that relies entirely on YAML configuration.
    """
    
    def __init__(self, amc_code: str, config_path: str = "src/config/amc_config.yaml"):
        self.amc_code = amc_code
        self.config = self._load_config(config_path, amc_code)
        super().__init__(self.config.get('amc_name', amc_code), "1.0")

    def _load_config(self, path: str, code: str) -> Dict[str, Any]:
        try:
            with open(path, 'r') as f:
                full_config = yaml.safe_load(f)
            
            amc_cfg = full_config.get('amcs', {}).get(code)
            if not amc_cfg:
                raise ValueError(f"AMC Code '{code}' not found in {path}")
            
            return amc_cfg
        except Exception as e:
            logger.error(f"Failed to load config for {code}: {e}")
            raise

    def extract(self, file_path: str) -> List[Dict[str, Any]]:
        if self.config.get('file_type') == 'excel':
            return self.extract_from_excel_config(file_path, self.config)
        else:
            logger.error(f"Unsupported file type '{self.config.get('file_type')}' for {self.amc_code}")
            return []
