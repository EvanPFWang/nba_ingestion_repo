"""Network capture utilities for bball_index_scraper.

Provides Playwright response interception for:
- Capturing JSON API responses
- Detecting GraphQL payloads
- Building network response cache for extraction
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CapturedResponse:
    #Captured network response
    url: str
    status: int
    content_type: str
    body: Optional[str] = None
    json_data: Optional[Any] = None
    is_json: bool = False

    def __post_init__(self):
        if self.body and not self.json_data:
            self._try_parse_json()

    def _try_parse_json(self):
        #Try to parse body as JSON
        if self.body:
            try:
                self.json_data = json.loads(self.body)
                self.is_json = True
            except (json.JSONDecodeError, TypeError):
                pass


@dataclass
class NetworkCapture:
    """Capture and filter network responses

    Usage with Playwright:
    ```
        capture = NetworkCapture()
        page.on("response", capture.on_response)
        json_responses = capture.get_json_responses()
    ```
    """

    responses: List[CapturedResponse] = field(default_factory=list)
    url_patterns: List[str] = field(default_factory=list)
    max_responses: int = 100

    def __post_init__(self):
        if not self.url_patterns:
            #default patterns for API endpoints
            self.url_patterns = ["/api/", "/graphql",
                                 "/_next/data/", "/player", "/stats", ]

    async def on_response(self, response):
        #Playwright response object handler
        url = response.url

        #check URL patterns
        if not self._should_capture(url):
            return

        #check content type
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type.lower() and "application/javascript" not in content_type.lower():
            return

        try:
            body = await response.text()

            captured = CapturedResponse(
                url=url,
                status=response.status,
                content_type=content_type,
                body=body,
            )

            if captured.is_json:
                self.responses.append(captured)
                logger.debug("Captured JSON response: %s", url)

                #limit stored responses
                if len(self.responses) > self.max_responses:
                    self.responses = self.responses[-self.max_responses:]

        except Exception as e:
            logger.debug("Failed to capture response %s: %s", url, e)

    def _should_capture(self, url: str) -> bool:
        """Check if URL should be captured."""
        for pattern in self.url_patterns:
            if pattern in url:
                return True
        return False

    def get_json_responses(self) -> List[Dict]:
        #Get all captured JSON response bodies + returns list of JSON data dicts
        return [r.json_data for r in self.responses if r.is_json and r.json_data]

    def get_stats_responses(self) -> List[Dict]:
        #Get responses containing player stats +   returns list of responses with stats-like data

        stats_responses = []

        for response in self.responses:
            if not response.is_json or not response.json_data:
                continue

            data = response.json_data

            #check for stats-like structure
            if self._has_stats_data(data):
                stats_responses.append(data)

        return stats_responses

    def _has_stats_data(self, data: Any, depth: int = 0) -> bool:
        #Check if data contains stats-like entries
        if depth > 5:
            return False

        if isinstance(data, dict):
            #look for stat-like keys
            stat_keys = ["stats", "statistics", "playerStats", "metrics", "grades"]
            if any(k in data for k in stat_keys):
                return True

            #look for stat-like structure
            value_keys = ["value", "percentile", "grade", "rank"]
            if sum(1 for k in value_keys if k in data) >= 2:
                return True

            #recurse into values
            for v in data.values():
                if self._has_stats_data(v, depth + 1):
                    return True

        elif isinstance(data, list):
            for item in data[:10]:
                if self._has_stats_data(item, depth + 1):
                    return True

        return False

    def get_graphql_responses(self) -> List[Dict]:
        #"""Get GraphQL query responses  +   returns list of GraphQL response data.
        graphql_responses = []

        for response in self.responses:
            if "/graphql" in response.url and response.is_json:
                graphql_responses.append(response.json_data)

        return graphql_responses

    def clear(self):
        #"""Clear captured responses."""
        self.responses.clear()

    def summary(self) -> Dict:
        #Get capture summary+    returns dict with capture statistics

        return {
            "total_responses": len(self.responses),
            "json_responses": sum(1 for r in self.responses if r.is_json),
            "stats_responses": len(self.get_stats_responses()),
            "graphql_responses": len(self.get_graphql_responses()),
            "urls": [r.url for r in self.responses],
        }


def create_response_handler(capture: NetworkCapture) -> Callable:
    #Create sync response handler wrapper + returns sync callback function for page.on("response", ...)
    import asyncio

    def handler(response): asyncio.create_task(capture.on_response(response))

    return handler
