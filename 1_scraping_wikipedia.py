
#################################################################################################################################################################
###############################   1.  IMPORTING MODULES AND INITIALIZING VARIABLES   ############################################################################
#################################################################################################################################################################

from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd
import glob
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple

pd.options.mode.chained_assignment = None

load_dotenv()

#################################################################################################################################################################
###############################   CONFIG (EDIT IF NEEDED)   ######################################################################################################
#################################################################################################################################################################

# Wikipedia language edition
WIKI_LANG = os.getenv("WIKI_LANG", "en")

# Polite request settings
REQUEST_TIMEOUT = 20         # seconds
DELAY_BETWEEN_REQUESTS = 1.0 # seconds
MAX_RETRIES = 3

# REQUIRED by Wikimedia policy: use a descriptive UA with contact info
# See: https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
USER_AGENT = os.getenv(
    "WIKIPEDIA_USER_AGENT",
    "JaydeepScraper/1.0 (jaydeep.patel@example.com; Productsquads Technolabs LLP) requests/2.x"
)

# Keep your existing .env paths so storage remains the same
SNAPSHOT_STORAGE_FILE = os.getenv("SNAPSHOT_STORAGE_FILE")            # e.g., C:\...\snapshot_id.txt
DATASET_STORAGE_FOLDER = os.getenv("DATASET_STORAGE_FOLDER")          # e.g., C:\...\datasets\
KEYWORDS_XLSX = os.getenv("KEYWORDS_XLSX", "keywords.xlsx")

# OPTIONAL: Set True to fetch data on first run (when snapshot file doesn't exist).
# If False, first run only creates the snapshot ID; re-run will fetch.
AUTO_FETCH_ON_FIRST_RUN = True

#################################################################################################################################################################
###############################   SESSION & HELPERS   ###########################################################################################################
#################################################################################################################################################################

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,   # REQUIRED by Wikimedia policy
    "Accept-Encoding": "gzip",  # polite & efficient
})

def ensure_dir(path: str):
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)

def safe_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    return name.strip()[:180]

def canonical_url(lang: str, title: str) -> str:
    from urllib.parse import quote
    return f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"

#################################################################################################################################################################
###############################   TITLE RESOLUTION (SEARCH)   ###################################################################################################
#################################################################################################################################################################

def resolve_title_action_search(query: str, lang: str) -> Optional[str]:
    """
    Use MediaWiki Action API search to get the best matching title.
    """
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": query,
        "srlimit": 1,
        "format": "json",
        "formatversion": 2,
    }
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            hits = data.get("query", {}).get("search", [])
            return hits[0].get("title") if hits else None
        except Exception as e:
            last_exc = e
            time.sleep(0.7 * attempt)
    raise RuntimeError(f"Action search failed for '{query}': {last_exc}")

def resolve_title_rest_search(query: str, lang: str) -> Optional[str]:
    """
    Use Core REST API search as secondary fallback.
    """
    url = f"https://{lang}.wikipedia.org/w/rest.php/v1/search/page"
    params = {"q": query, "limit": 1}
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            pages = data.get("pages", [])
            return pages[0].get("title") if pages else None
        except Exception as e:
            last_exc = e
            time.sleep(0.7 * attempt)
    raise RuntimeError(f"REST search failed for '{query}': {last_exc}")

def resolve_title(query: str, lang: str) -> Optional[str]:
    title = resolve_title_action_search(query, lang)
    if title:
        return title
    return resolve_title_rest_search(query, lang)

#################################################################################################################################################################
###############################   CONTENT + TABLE OF CONTENTS (TOC)   ############################################################################################
#################################################################################################################################################################

def fetch_plaintext_extract(title: str, lang: str) -> Tuple[str, str, Optional[int]]:
    """
    Fetch plain-text extract via Action API prop=extracts.
    Returns (text, normalized_title, pageid).
    """
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": 1,
        "redirects": 1,
        "format": "json",
        "formatversion": 2,
        "titles": title,
    }
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            pages = data.get("query", {}).get("pages", [])
            if not pages:
                return "", title, None
            page = pages[0]
            text = page.get("extract") or ""
            norm_title = page.get("title", title)
            pageid = page.get("pageid")
            return text, norm_title, pageid
        except Exception as e:
            last_exc = e
            time.sleep(0.7 * attempt)
    raise RuntimeError(f"Extract failed for '{title}': {last_exc}")

def fetch_sections(title: str, lang: str) -> List[str]:
    """
    Fetch TOC (section headings) via Action API action=parse&prop=sections.
    Returns list of section titles.
    """
    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": title,
        "prop": "sections",
        "format": "json",
    }
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            sections = data.get("parse", {}).get("sections", [])
            toc = []
            for s in sections:
                name = s.get("line") or s.get("anchor") or ""
                name = str(name).strip()
                if name:
                    toc.append(name)
            return toc
        except Exception as e:
            last_exc = e
            time.sleep(0.7 * attempt)
    return []

#################################################################################################################################################################
###############################   FETCH & WRITE (NDJSON TO data.txt)   ##########################################################################################
#################################################################################################################################################################

def fetch_and_write_ndjson(df: pd.DataFrame, dataset_folder: str, lang: str):
    ensure_dir(dataset_folder)
    out_path = Path(dataset_folder) / "data.txt"   # << keep the same filename
    with out_path.open("w", encoding="utf-8") as f:
        for idx in df.index:
            query = str(df.loc[idx, "Keyword"]).strip()
            if not query:
                continue

            print(f"[{idx}] Resolving '{query}' on {lang}.wikipedia.org ...")
            try:
                resolved = resolve_title(query, lang)
            except Exception as e:
                print(f"  -> Title resolution error: {e}")
                resolved = None

            title = resolved or query

            # Fetch plain text
            text, norm_title, pageid = "", title, None
            try:
                text, norm_title, pageid = fetch_plaintext_extract(title, lang)
            except Exception as e:
                print(f"  -> Extract error: {e}")

            # Fetch TOC
            toc = []
            try:
                toc = fetch_sections(norm_title, lang)
            except Exception as e:
                print(f"  -> Sections error: {e}")

            obj = {
                "url": canonical_url(lang, norm_title),
                "title": norm_title,
                "table_of_contents": toc,
                "raw_text": text,
                # optional extras:
                # "input_keyword": query,
                # "pageid": pageid,
                # "language": lang,
            }

            # NDJSON (one JSON object per line), to data.txt
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            print(f"  -> Wrote line for '{norm_title}' (chars={len(text)}, sections={len(toc)})")

            time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\n==> Saved NDJSON to: {out_path}")

#################################################################################################################################################################
###############################   2.  IF SnapshotID IS NOT SET, CREATE LOCAL SNAPSHOT-ID (NO BRIGHT DATA)   #####################################################
#################################################################################################################################################################

# NOTE: We are NOT calling Bright Data at all. We just mimic your snapshot file behavior.
# On first run, we create a local snapshot-id. Optionally, we also fetch immediately.

file_exists = SNAPSHOT_STORAGE_FILE and os.path.isfile(SNAPSHOT_STORAGE_FILE)

# Load keywords.xlsx
keywords = pd.read_excel(KEYWORDS_XLSX, engine="openpyxl")  # expects columns: Keyword, Pages

if not file_exists:
    # Create a local snapshot id (timestamp-based)
    import datetime, uuid
    run_id = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8]

    print(f"Local snapshot created: {run_id}")

    # Write snapshot-id to file
    ensure_dir(Path(SNAPSHOT_STORAGE_FILE).parent.as_posix())
    with open(SNAPSHOT_STORAGE_FILE, "w", encoding="utf-8") as f:
        f.write(run_id)

    # Either stop here (exact Bright Data flow) or fetch now
    if AUTO_FETCH_ON_FIRST_RUN:
        print("Fetching articles now (AUTO_FETCH_ON_FIRST_RUN=True)")
        ensure_dir(DATASET_STORAGE_FOLDER)
        fetch_and_write_ndjson(keywords, DATASET_STORAGE_FOLDER, WIKI_LANG)
    else:
        print("Snapshot ID written. Re-run the script to fetch and store data.")

else:

#################################################################################################################################################################
###############################   3.  IF SnapshotID IS SET, WRITE DATA TO FILES (NO BRIGHT DATA PROGRESS)   #####################################################
#################################################################################################################################################################

    # Clean dataset folder before writing (same behavior as your original script)
    if DATASET_STORAGE_FOLDER:
        files = glob.glob(os.path.join(DATASET_STORAGE_FOLDER, "*"))
        for f in files:
            try:
                os.remove(f)
            except Exception as e:
                print(f"Warning: could not remove {f}: {e}")

    # Read snapshot id (for logging only)
    with open(SNAPSHOT_STORAGE_FILE, "r", encoding="utf-8") as f:
        snapshot_id = f.read().strip()

    print("status")
    print("ready")  # local snapshot is always ready (no external queue)

    print("Snapshot is ready")
    print("")

    print("== > All articles are ready - start writing data to datasets directory")

    # Ensure the folder exists
    ensure_dir(DATASET_STORAGE_FOLDER)

    # Build and write NDJSON to data.txt (same file name as before)
    fetch_and_write_ndjson(keywords, DATASET_STORAGE_FOLDER, WIKI_LANG)
