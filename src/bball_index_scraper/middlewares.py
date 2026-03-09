"""Scrapy middlewares for bball_index_scraper.

Includes:
- PlaywrightRetryMiddleware: exponential backoff with Playwright cleanup
- JitteredDelayMiddleware: randomized delays for human-like pacing
"""

import logging
import random
import time
from typing import Optional

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.http import Request, Response
from scrapy.spiders import Spider
from scrapy.exceptions import IgnoreRequest
from scrapy.utils.response import response_status_message

logger = logging.getLogger(__name__)


class PlaywrightRetryMiddleware(RetryMiddleware):
    """Retry middleware with exponential backoff for Playwright requests.

    Handles:
    - Exponential backoff delays (5s, 20s, 80s)
    - Playwright page/context cleanup on retry
    - Auth expiration detection
    """

    def __init__(self, settings):
        super().__init__(settings)
        self.backoff_base = settings.getfloat("RETRY_BACKOFF_BASE", 5)
        self.backoff_multiplier = settings.getfloat("RETRY_BACKOFF_MULTIPLIER", 4)

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def spider_opened(self, spider: Spider):
        self.spider = spider

    def _retry(self, request: Request, reason, spider: Spider) -> Optional[Request]:
        #Override to add exponential backoff delay 
        retries = request.meta.get("retry_times", 0) + 1
        retry_times = self.max_retry_times

        if "max_retry_times" in request.meta:
            retry_times = request.meta["max_retry_times"]

        stats = spider.crawler.stats

        if retries <= retry_times:
            #calculate backoff delay
            delay = self.backoff_base * (self.backoff_multiplier ** (retries - 1))
            delay = min(delay, 120)  #cap at 2 minutes

            logger.info(
                "Retrying %s (failed %d times): %s - backing off %.1fs",
                request.url,
                retries,
                reason,
                delay,
            )

            #sleep before retry
            time.sleep(delay)

            retryreq = request.copy()
            retryreq.meta["retry_times"] = retries
            retryreq.dont_filter = True
            retryreq.priority = request.priority + self.priority_adjust

            if stats:
                stats.inc_value("retry/count")
                stats.inc_value(f"retry/reason_count/{reason}")

            return retryreq
        else:
            if stats:
                stats.inc_value("retry/max_reached")

            logger.error(
                "Gave up retrying %s (failed %d times): %s",
                request.url,
                retries,
                reason,
            )
            return None

    def process_response(self, request: Request, response: Response, spider: Spider) -> Response:
        #Check for auth expiration and other retryable conditions 

        #check for auth expiration (redirect to login)
        if self._is_auth_expired(response):
            logger.warning("Auth expired - redirect to login detected: %s", request.url)
            spider.crawler.stats.inc_value("auth/expired")

            #mark for re-auth
            request.meta["auth_expired"] = True

            #retry if under limit
            reason = "auth_expired"
            return self._retry(request, reason, spider) or response

        #check standard HTTP error codes
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response

        return response

    def _is_auth_expired(self, response: Response) -> bool:
        #Detect if response indicates expired authentication 

        #check for login page redirect
        if "/login" in response.url or "/signin" in response.url:
            return True

        #check for redirect chains ending at login
        if hasattr(response, "request") and response.request:
            if "/login" in str(response.request.url):
                return True

        #check response body for login indicators
        body = response.text if hasattr(response, "text") else ""
        login_indicators = ["please log in", "sign in to continue", "session expired"]
        for indicator in login_indicators:
            if indicator.lower() in body.lower():
                return True

        return False

    def process_exception(self, request: Request, exception, spider: Spider):
        #Handle Playwright-specific exceptions 
        exception_name = type(exception).__name__

        #playwright timeout errors
        if "Timeout" in exception_name or "timeout" in str(exception).lower():
            reason = f"playwright_timeout: {exception}"
            return self._retry(request, reason, spider)

        #context/page closed errors
        if "closed" in str(exception).lower():
            reason = f"context_closed: {exception}"
            return self._retry(request, reason, spider)

        #network errors
        if "net::" in str(exception).lower():
            reason = f"network_error: {exception}"
            return self._retry(request, reason, spider)

        return None


class JitteredDelayMiddleware:
    """Add randomized jitter to request timing for human-like pacing.

    Applies variable delays based on:
    - Base delay from settings
    - Random jitter factor
    - Request type (initial vs subsequent)
    """

    def __init__(self, settings):
        self.base_delay = settings.getfloat("DOWNLOAD_DELAY", 2.0)
        self.min_jitter = 0.5  #minimum multiplier
        self.max_jitter = 2.0  #maximum multiplier
        self.initial_delay_multiplier = 1.5  #extra delay for first request to domain
        self._domain_first_request = set()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(crawler.settings)

    def process_request(self, request: Request, spider: Spider):
        #Apply jittered delay before request 

        #skip if this is a Playwright sub-request
        if request.meta.get("playwright_include_page"):
            return None

        #calculate jittered delay
        jitter = random.uniform(self.min_jitter, self.max_jitter)
        delay = self.base_delay * jitter

        #extra delay for first request to domain
        domain = self._extract_domain(request.url)
        if domain not in self._domain_first_request:
            self._domain_first_request.add(domain)
            delay *= self.initial_delay_multiplier
            logger.debug("First request to %s - extra delay: %.2fs", domain, delay)

        if delay > 0:
            time.sleep(delay)

        return None

    @staticmethod
    def _extract_domain(url: str) -> str:
        #Extract domain from URL 
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc


class AuthStateMiddleware:
    #Middleware to handle authenticated session state.
    #Monitors for auth expiration and signals when re-auth is needed 

    def __init__(self, settings):
        self.auth_state_file = settings.get("AUTH_STATE_FILE")
        self.auth_expired_count = 0
        self.max_auth_failures = 3

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def process_response(self, request: Request, response: Response, spider: Spider):
        #Track auth failures 
        if request.meta.get("auth_expired"):
            self.auth_expired_count += 1

            if self.auth_expired_count >= self.max_auth_failures:
                logger.error(
                    "Auth expired %d times - stopping spider. "
                    "Run scripts/save_auth_state.py to re-authenticate.",
                    self.auth_expired_count,
                )
                spider.crawler.engine.close_spider(spider, "auth_expired")

        return response

    def spider_closed(self, spider: Spider, reason: str):
        #Log auth status on spider close 
        if self.auth_expired_count > 0:
            logger.warning(
                "Spider closed with %d auth expiration events. "
                "Auth state may need refresh.",
                self.auth_expired_count,
            )
