import hashlib
import hmac

from data.secrets.bnb_keys import API_KEY, API_SECRET

BASE_URL = "https://api.binance.com"

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
