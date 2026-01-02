import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

class RPCClient:
    def __init__(self, retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
        self.session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def call(self, url: str, payload: dict, timeout: int = 10) -> dict:
        """
        Executes a POST request to the given URL with the specified payload.
        Returns the JSON response or raises an exception.
        """
        try:
            response = self.session.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            # Re-raise or handle as needed. For now, we let the caller handle it 
            # or wrap it in a custom exception if we had one.
            raise e
