
$AMCS = @(
    "absl", "angelone", "axis", "bajaj", "bandhan", "baroda", "boi", "canara", 
    "capitalmind", "choice", "dsp", "edelweiss", "franklin", "groww", "hdfc", 
    "helios", "hsbc", "icici", "invesco", "iti", "jio_br", "jmfinancial", 
    "kotak", "lic", "mahindra", "mirae_asset", "motilal", "navi", "nippon", 
    "nj", "old_bridge", "pgim_india", "ppfas", "quant", "quantum", "samco", 
    "sbi", "sundaram", "tata", "taurus", "threesixtyone", "trust", "unifi", 
    "union", "uti", "wealth_company", "whiteoak"
)

$StartYear = 2025
$StartMonth = 11
$EndYear = 2025
$EndMonth = 12

echo "Starting Global Backfill for $StartYear-$StartMonth to $EndYear-$EndMonth"

foreach ($amc in $AMCS) {
    echo "----------------------------------------------------------------"
    echo "Running Backfill for: $amc"
    echo "----------------------------------------------------------------"
    
    $cmd = "python src/scheduler/${amc}_backfill.py --start-year $StartYear --start-month $StartMonth --end-year $EndYear --end-month $EndMonth"
    
    Invoke-Expression $cmd
    
    if ($LASTEXITCODE -ne 0) {
        echo "❌ Failed: $amc"
    } else {
        echo "✅ Completed: $amc"
    }
    
    Start-Sleep -Seconds 2
}

echo "----------------------------------------------------------------"
echo "Global Backfill Sequence Completed"
echo "----------------------------------------------------------------"
