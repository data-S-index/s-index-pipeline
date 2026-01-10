BASE_API_URL = "https://www.ebi.ac.uk/emdb/api"

# CSV search to get all released EMDB IDs
EMDB_IDS_URL = (
    f'{BASE_API_URL}/search/current_status:"REL"'
    "?rows=1000000&wt=csv&download=true&fl=emdb_id"
)

ENTRY_BASE_URL = f"{BASE_API_URL}/entry"

DEFAULT_MAX_WORKERS = 12
DEFAULT_TIMEOUT_IDS = 300
DEFAULT_TIMEOUT_ENTRY = 120
