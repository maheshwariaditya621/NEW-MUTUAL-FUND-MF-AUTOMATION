import importlib
from typing import Optional, Type
from src.downloaders.base_downloader import BaseDownloader
from src.config import logger

class DownloaderFactory:
    """
    Factory to return the appropriate AMC downloader.
    """
    
    # Mapping of AMC slug to (module_path, class_name)
    DOWNLOADER_MAP = {
        "abakkus": ("src.downloaders.abakkus_downloader", "AbakkusDownloader"),
        "absl": ("src.downloaders.absl_downloader", "ABSLDownloader"),
        "angelone": ("src.downloaders.angelone_downloader", "AngelOneDownloader"),
        "axis": ("src.downloaders.axis_downloader", "AxisDownloader"),
        "bajaj": ("src.downloaders.bajaj_downloader", "BajajDownloader"),
        "bandhan": ("src.downloaders.bandhan_downloader", "BandhanDownloader"),
        "baroda": ("src.downloaders.baroda_downloader", "BarodaDownloader"),
        "boi": ("src.downloaders.boi_downloader", "BOIDownloader"),
        "canara": ("src.downloaders.canara_downloader", "CanaraDownloader"),
        "capitalmind": ("src.downloaders.capitalmind_downloader", "CapitalMindDownloader"),
        "choice": ("src.downloaders.choice_downloader", "ChoiceDownloader"),
        "dsp": ("src.downloaders.dsp_downloader", "DSPDownloader"),
        "edelweiss": ("src.downloaders.edelweiss_downloader", "EdelweissDownloader"),
        "franklin": ("src.downloaders.franklin_downloader", "FranklinDownloader"),
        "groww": ("src.downloaders.groww_downloader", "GrowwDownloader"),
        "hdfc": ("src.downloaders.hdfc_downloader", "HDFCDownloader"),
        "helios": ("src.downloaders.helios_downloader", "HeliosDownloader"),
        "hsbc": ("src.downloaders.hsbc_downloader", "HSBCDownloader"),
        "icici": ("src.downloaders.icici_downloader", "ICICIDownloader"),
        "invesco": ("src.downloaders.invesco_downloader", "InvescoDownloader"),
        "iti": ("src.downloaders.iti_downloader", "ITIDownloader"),
        "jio_br": ("src.downloaders.jio_br_downloader", "JioBRDownloader"),
        "jmfinancial": ("src.downloaders.jmfinancial_downloader", "JMFinancialDownloader"),
        "kotak": ("src.downloaders.kotak_downloader", "KotakDownloader"),
        "lic": ("src.downloaders.lic_downloader", "LICDownloader"),
        "mahindra": ("src.downloaders.mahindra_downloader", "MahindraDownloader"),
        "mirae_asset": ("src.downloaders.mirae_asset_downloader", "MiraeAssetDownloader"),
        "motilal": ("src.downloaders.motilal_downloader", "MotilalDownloader"),
        "navi": ("src.downloaders.navi_downloader", "NaviDownloader"),
        "nippon": ("src.downloaders.nippon_downloader", "NipponDownloader"),
        "nj": ("src.downloaders.nj_downloader", "NJDownloader"),
        "old_bridge": ("src.downloaders.old_bridge_downloader", "OldBridgeDownloader"),
        "pgim_india": ("src.downloaders.pgim_india_downloader", "PGIMIndiaDownloader"),
        "ppfas": ("src.downloaders.ppfas_downloader", "PPFASDownloader"),
        "quant": ("src.downloaders.quant_downloader", "QuantDownloader"),
        "quantum": ("src.downloaders.quantum_downloader", "QuantumDownloader"),
        "samco": ("src.downloaders.samco_downloader", "SamcoDownloader"),
        "sbi": ("src.downloaders.sbi_downloader", "SBIDownloader"),
        "shriram": ("src.downloaders.shriram_downloader", "ShriramDownloader"),
        "sundaram": ("src.downloaders.sundaram_downloader", "SundaramDownloader"),
        "tata": ("src.downloaders.tata_downloader", "TataDownloader"),
        "taurus": ("src.downloaders.taurus_downloader", "TaurusDownloader"),
        "threesixtyone": ("src.downloaders.threesixtyone_downloader", "ThreeSixtyOneDownloader"),
        "trust": ("src.downloaders.trust_downloader", "TrustDownloader"),
        "unifi": ("src.downloaders.unifi_downloader", "UnifiDownloader"),
        "union": ("src.downloaders.union_downloader", "UnionDownloader"),
        "uti": ("src.downloaders.uti_downloader", "UTIDownloader"),
        "wealth_company": ("src.downloaders.wealth_company_downloader", "WealthCompanyDownloader"),
        "whiteoak": ("src.downloaders.whiteoak_downloader", "WhiteOakDownloader"),
        "zerodha": ("src.downloaders.zerodha_downloader", "ZerodhaDownloader"),
    }

    @staticmethod
    def get_downloader(amc_slug: str) -> Optional[BaseDownloader]:
        """
        Returns an instance of the downloader for the given AMC.
        """
        amc_slug = amc_slug.lower()
        if amc_slug not in DownloaderFactory.DOWNLOADER_MAP:
            logger.error(f"No downloader mapping found for AMC: {amc_slug}")
            return None
            
        module_path, class_name = DownloaderFactory.DOWNLOADER_MAP[amc_slug]
        
        try:
            module = importlib.import_module(module_path)
            downloader_class: Type[BaseDownloader] = getattr(module, class_name)
            return downloader_class()
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load downloader {class_name} from {module_path}: {e}")
            return None
