"""Abstract base class shared by all API extractors."""

import logging
from abc import ABC, abstractmethod
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base for HTTP-based extractors.

    Provides a ``_get`` helper with automatic exponential-backoff retries
    (up to 3 attempts) on any ``requests.RequestException``.  Concrete
    subclasses must implement :meth:`extract`.
    """

    BASE_URL: str = ""

    def __init__(self, timeout: int = 30) -> None:
        """Initialise the extractor with a shared requests session.

        Args:
            timeout: HTTP read/connect timeout in seconds.
        """
        self.timeout = timeout
        self.session = requests.Session()

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Perform a GET request and return the parsed JSON body.

        Args:
            url: Fully-qualified URL to request.
            params: Optional query-string parameters.
            headers: Optional request headers.

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            requests.HTTPError: On a 4xx/5xx response after all retries.
            requests.RequestException: On network-level failures after all retries.
        """
        logger.debug("GET %s  params=%s", url, params)
        response = self.session.get(
            url, params=params, headers=headers, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    @abstractmethod
    def extract(self) -> dict[str, Any]:
        """Extract data from the upstream API.

        Returns:
            A dictionary containing the extracted payload.
        """
