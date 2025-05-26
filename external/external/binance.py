import hashlib
import hmac
import json

# TODO: Get secrets path from args
with open(
    "/Users/praneshbalekai/Desktop/IB_PRD/data/vault_secrets/bnb_keys.json", "r"
) as f:
    secrets = json.load(f)

API_KEY = secrets["API_KEY"]
API_SECRET = secrets["API_SECRET"]

# BASE_URL = "https://api.binance.com"
BASE_URL = "https://api-gcp.binance.com"

DEFAULT_HEADERS = {"X-MBX-APIKEY": API_KEY}


def get_query_signature(query_string: str) -> str:
    """Return authenticated signature for trading endpoints that require authentication.

    Args:
        query_string (str): URL encoded query params as str.

    Returns:
        str: auth signature
    """
    return hmac.new(
        API_SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()
