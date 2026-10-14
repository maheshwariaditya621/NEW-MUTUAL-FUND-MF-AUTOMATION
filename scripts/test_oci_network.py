try:
    from curl_cffi import requests as cffi_requests
    print("curl_cffi is available")
    for u in ["https://www.angelonemf.com/downloads", "https://www.bajajamc.com", "https://cms.whiteoakamc.com"]:
        try:
            r = cffi_requests.get(u, impersonate="chrome120", timeout=10)
            print(u, "->", r.status_code)
        except Exception as ex:
            print(u, "-> error:", ex)
except ImportError:
    print("curl_cffi NOT installed")
    import requests
    for u in ["https://www.angelonemf.com/downloads", "https://www.bajajamc.com", "https://cms.whiteoakamc.com"]:
        try:
            r = requests.get(u, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"}, timeout=10)
            print(u, "->", r.status_code)
        except Exception as ex:
            print(u, "-> error:", ex)
