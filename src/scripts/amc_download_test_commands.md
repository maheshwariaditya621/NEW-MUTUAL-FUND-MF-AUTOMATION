# AMC Download Test Commands

Test each AMC downloader manually for **February 2026** and **December 2025**.

> **Setup:** Run all commands from `D:\CODING\NEW MUTUAL FUND MF AUTOMATION\`  
> **Note:** Expected responses — `"status": "skipped"` (already downloaded), `"status": "not_published"` (data not out yet), `"status": "success"` (freshly downloaded), `"status": "failed"` (error)

---

## February 2026 (Year=2026, Month=2)

### 1. ABAKKUS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.abakkus_downloader import AbakkusDownloader; import json; d=AbakkusDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 2. ABSL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.absl_downloader import ABSLDownloader; import json; d=ABSLDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 3. ANGEL ONE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.angelone_downloader import AngelOneDownloader; import json; d=AngelOneDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 4. AXIS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.axis_downloader import AxisDownloader; import json; d=AxisDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 5. BAJAJ
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.bajaj_downloader import BajajDownloader; import json; d=BajajDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 6. BANDHAN
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.bandhan_downloader import BandhanDownloader; import json; d=BandhanDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 7. BARODA BNP PARIBAS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.baroda_downloader import BarodaDownloader; import json; d=BarodaDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 8. BOI (Bank of India)
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.boi_downloader import BOIDownloader; import json; d=BOIDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 9. CANARA ROBECO
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.canara_downloader import CanaraDownloader; import json; d=CanaraDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 10. CAPITALMIND
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.capitalmind_downloader import CapitalMindDownloader; import json; d=CapitalMindDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 11. CHOICE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.choice_downloader import ChoiceDownloader; import json; d=ChoiceDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 12. DSP
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.dsp_downloader import DSPDownloader; import json; d=DSPDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 13. EDELWEISS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.edelweiss_downloader import EdelweissDownloader; import json; d=EdelweissDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 14. FRANKLIN TEMPLETON
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.franklin_downloader import FranklinDownloader; import json; d=FranklinDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 15. GROWW
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.groww_downloader import GrowwDownloader; import json; d=GrowwDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 16. HDFC
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.hdfc_downloader import HDFCDownloader; import json; d=HDFCDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 17. HELIOS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.helios_downloader import HeliosDownloader; import json; d=HeliosDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 18. HSBC
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.hsbc_downloader import HSBCDownloader; import json; d=HSBCDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 19. ICICI PRUDENTIAL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.icici_downloader import ICICIDownloader; import json; d=ICICIDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 20. INVESCO
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.invesco_downloader import InvescoDownloader; import json; d=InvescoDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 21. ITI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.iti_downloader import ITIDownloader; import json; d=ITIDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 22. JIO BLACKROCK
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.jio_br_downloader import JioBRDownloader; import json; d=JioBRDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 23. JM FINANCIAL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.jmfinancial_downloader import JMFinancialDownloader; import json; d=JMFinancialDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 24. KOTAK
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.kotak_downloader import KotakDownloader; import json; d=KotakDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 25. LIC
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.lic_downloader import LICDownloader; import json; d=LICDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 26. MAHINDRA MANULIFE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.mahindra_downloader import MahindraDownloader; import json; d=MahindraDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 27. MIRAE ASSET
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.mirae_asset_downloader import MiraeAssetDownloader; import json; d=MiraeAssetDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 28. MOTILAL OSWAL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.motilal_downloader import MotilalDownloader; import json; d=MotilalDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 29. NAVI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.navi_downloader import NaviDownloader; import json; d=NaviDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 30. NIPPON
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.nippon_downloader import NipponDownloader; import json; d=NipponDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 31. NJ
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.nj_downloader import NJDownloader; import json; d=NJDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 32. OLD BRIDGE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.old_bridge_downloader import OldBridgeDownloader; import json; d=OldBridgeDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 33. PGIM INDIA
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.pgim_india_downloader import PGIMIndiaDownloader; import json; d=PGIMIndiaDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 34. PPFAS (Parag Parikh)
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.ppfas_downloader import PPFASDownloader; import json; d=PPFASDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 35. QUANT
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.quant_downloader import QuantDownloader; import json; d=QuantDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 36. QUANTUM
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.quantum_downloader import QuantumDownloader; import json; d=QuantumDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 37. SAMCO
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.samco_downloader import SamcoDownloader; import json; d=SamcoDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 38. SBI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.sbi_downloader import SBIDownloader; import json; d=SBIDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 39. SHRIRAM
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.shriram_downloader import ShriramDownloader; import json; d=ShriramDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 40. SUNDARAM
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.sundaram_downloader import SundaramDownloader; import json; d=SundaramDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 41. TATA
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.tata_downloader import TataDownloader; import json; d=TataDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 42. TAURUS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.taurus_downloader import TaurusDownloader; import json; d=TaurusDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 43. 360 ONE (ThreeSixtyOne)
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.threesixtyone_downloader import ThreeSixtyOneDownloader; import json; d=ThreeSixtyOneDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 44. TRUST
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.trust_downloader import TrustDownloader; import json; d=TrustDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 45. UNIFI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.unifi_downloader import UnifiDownloader; import json; d=UnifiDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 46. UNION
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.union_downloader import UnionDownloader; import json; d=UnionDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 47. UTI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.uti_downloader import UTIDownloader; import json; d=UTIDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 48. WEALTH COMPANY
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.wealth_company_downloader import WealthCompanyDownloader; import json; d=WealthCompanyDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 49. WHITEOAK
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.whiteoak_downloader import WhiteOakDownloader; import json; d=WhiteOakDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

### 50. ZERODHA
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.zerodha_downloader import ZerodhaDownloader; import json; d=ZerodhaDownloader(); r=d.download(2026,2); print(json.dumps(r,indent=2))"
```

---

## December 2025 (Year=2025, Month=12)

### 1. ABAKKUS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.abakkus_downloader import AbakkusDownloader; import json; d=AbakkusDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 2. ABSL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.absl_downloader import ABSLDownloader; import json; d=ABSLDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 3. ANGEL ONE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.angelone_downloader import AngelOneDownloader; import json; d=AngelOneDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 4. AXIS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.axis_downloader import AxisDownloader; import json; d=AxisDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 5. BAJAJ
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.bajaj_downloader import BajajDownloader; import json; d=BajajDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 6. BANDHAN
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.bandhan_downloader import BandhanDownloader; import json; d=BandhanDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 7. BARODA BNP PARIBAS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.baroda_downloader import BarodaDownloader; import json; d=BarodaDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 8. BOI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.boi_downloader import BOIDownloader; import json; d=BOIDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 9. CANARA ROBECO
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.canara_downloader import CanaraDownloader; import json; d=CanaraDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 10. CAPITALMIND
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.capitalmind_downloader import CapitalMindDownloader; import json; d=CapitalMindDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 11. CHOICE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.choice_downloader import ChoiceDownloader; import json; d=ChoiceDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 12. DSP
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.dsp_downloader import DSPDownloader; import json; d=DSPDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 13. EDELWEISS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.edelweiss_downloader import EdelweissDownloader; import json; d=EdelweissDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 14. FRANKLIN TEMPLETON
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.franklin_downloader import FranklinDownloader; import json; d=FranklinDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 15. GROWW
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.groww_downloader import GrowwDownloader; import json; d=GrowwDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 16. HDFC
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.hdfc_downloader import HDFCDownloader; import json; d=HDFCDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 17. HELIOS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.helios_downloader import HeliosDownloader; import json; d=HeliosDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 18. HSBC
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.hsbc_downloader import HSBCDownloader; import json; d=HSBCDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 19. ICICI PRUDENTIAL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.icici_downloader import ICICIDownloader; import json; d=ICICIDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 20. INVESCO
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.invesco_downloader import InvescoDownloader; import json; d=InvescoDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 21. ITI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.iti_downloader import ITIDownloader; import json; d=ITIDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 22. JIO BLACKROCK
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.jio_br_downloader import JioBRDownloader; import json; d=JioBRDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 23. JM FINANCIAL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.jmfinancial_downloader import JMFinancialDownloader; import json; d=JMFinancialDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 24. KOTAK
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.kotak_downloader import KotakDownloader; import json; d=KotakDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 25. LIC
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.lic_downloader import LICDownloader; import json; d=LICDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 26. MAHINDRA MANULIFE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.mahindra_downloader import MahindraDownloader; import json; d=MahindraDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 27. MIRAE ASSET
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.mirae_asset_downloader import MiraeAssetDownloader; import json; d=MiraeAssetDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 28. MOTILAL OSWAL
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.motilal_downloader import MotilalDownloader; import json; d=MotilalDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 29. NAVI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.navi_downloader import NaviDownloader; import json; d=NaviDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 30. NIPPON
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.nippon_downloader import NipponDownloader; import json; d=NipponDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 31. NJ
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.nj_downloader import NJDownloader; import json; d=NJDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 32. OLD BRIDGE
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.old_bridge_downloader import OldBridgeDownloader; import json; d=OldBridgeDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 33. PGIM INDIA
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.pgim_india_downloader import PGIMIndiaDownloader; import json; d=PGIMIndiaDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 34. PPFAS (Parag Parikh)
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.ppfas_downloader import PPFASDownloader; import json; d=PPFASDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 35. QUANT
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.quant_downloader import QuantDownloader; import json; d=QuantDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 36. QUANTUM
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.quantum_downloader import QuantumDownloader; import json; d=QuantumDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 37. SAMCO
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.samco_downloader import SamcoDownloader; import json; d=SamcoDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 38. SBI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.sbi_downloader import SBIDownloader; import json; d=SBIDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 39. SHRIRAM
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.shriram_downloader import ShriramDownloader; import json; d=ShriramDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 40. SUNDARAM
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.sundaram_downloader import SundaramDownloader; import json; d=SundaramDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 41. TATA
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.tata_downloader import TataDownloader; import json; d=TataDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 42. TAURUS
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.taurus_downloader import TaurusDownloader; import json; d=TaurusDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 43. 360 ONE (ThreeSixtyOne)
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.threesixtyone_downloader import ThreeSixtyOneDownloader; import json; d=ThreeSixtyOneDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 44. TRUST
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.trust_downloader import TrustDownloader; import json; d=TrustDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 45. UNIFI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.unifi_downloader import UnifiDownloader; import json; d=UnifiDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 46. UNION
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.union_downloader import UnionDownloader; import json; d=UnionDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 47. UTI
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.uti_downloader import UTIDownloader; import json; d=UTIDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 48. WEALTH COMPANY
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.wealth_company_downloader import WealthCompanyDownloader; import json; d=WealthCompanyDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 49. WHITEOAK
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.whiteoak_downloader import WhiteOakDownloader; import json; d=WhiteOakDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

### 50. ZERODHA
```powershell
$env:PYTHONPATH="."; .venv\Scripts\python.exe -c "from src.downloaders.zerodha_downloader import ZerodhaDownloader; import json; d=ZerodhaDownloader(); r=d.download(2025,12); print(json.dumps(r,indent=2))"
```

---

## Status Reference

| Status | Meaning |
|---|---|
| `success` | Files downloaded successfully |
| `skipped` | Already downloaded previously (idempotent) |
| `not_published` | AMC hasn't released data for that month yet |
| `before_inception` | AMC didn't exist for that period |
| `failed` | Download error — check the `reason` field |

> **Tip:** If you get `skipped` and want to force a fresh re-download, delete the folder `data\raw\<amc>\<year>_<month>` first, then re-run.
