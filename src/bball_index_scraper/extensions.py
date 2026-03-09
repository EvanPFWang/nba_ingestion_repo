import logging
from datetime import datetime
from typing import Optional

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.spiders import Spider

logger = logging.getLogger(__name__)


class ProgressExtension:
    #Track and log crawl progress
    """
    Periodically logs:
    - Items scraped
    - Pages processed
    - Errors encountered
    - Estimated completion
    """

    def __init__(self, stats, log_interval: int = 60):
        self.stats = stats
        self.log_interval = log_interval
        self.start_time: Optional[datetime] = None
        self.last_log_time: Optional[datetime] = None
        self.total_expected: int = 0

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        ext = cls(
            stats=crawler.stats,
            log_interval=crawler.settings.getint("PROGRESS_LOG_INTERVAL", 60),
        )

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.request_scheduled, signal=signals.request_scheduled)

        return ext

    def spider_opened(self, spider: Spider):
        #Initialize progress tracking
        self.start_time = datetime.utcnow()
        self.last_log_time = self.start_time

        logger.info("=" * 60)
        logger.info("Spider '%s' started at %s", spider.name, self.start_time.isoformat())
        logger.info("=" * 60)

    def spider_closed(self, spider: Spider, reason: str):
        #Log final stats on spider close
        end_time = datetime.utcnow()
        duration = end_time - self.start_time if self.start_time else None

        items_scraped = self.stats.get_value("item_scraped_count", 0)
        pages_crawled = self.stats.get_value("response_received_count", 0)
        retry_count = self.stats.get_value("retry/count", 0)
        error_count = self.stats.get_value("spider_exceptions/count", 0)
        auth_expired = self.stats.get_value("auth/expired", 0)

        logger.info("=" * 60)
        logger.info("Spider '%s' closed - reason: %s", spider.name, reason)
        logger.info("Duration: %s", duration)
        logger.info("Items scraped: %d", items_scraped)
        logger.info("Pages crawled: %d", pages_crawled)
        logger.info("Retries: %d", retry_count)
        logger.info("Errors: %d", error_count)
        logger.info("Auth expirations: %d", auth_expired)

        if items_scraped > 0 and duration:
            rate = items_scraped / duration.total_seconds() * 60
            logger.info("Scrape rate: %.1f items/min", rate)

        logger.info("=" * 60)

    def item_scraped(self, item, spider: Spider):
        #Track item scraped event
        now = datetime.utcnow()

        #periodic progress log
        if self.last_log_time:
            elapsed = (now - self.last_log_time).total_seconds()
            if elapsed >= self.log_interval:
                self._log_progress(spider)
                self.last_log_time = now

    def request_scheduled(self, request, spider: Spider):
        #Track scheduled requests
        pass

    def _log_progress(self, spider: Spider):
        #Log current progress
        items_scraped = self.stats.get_value("item_scraped_count", 0)
        pages_crawled = self.stats.get_value("response_received_count", 0)
        pending = self.stats.get_value("scheduler/enqueued", 0) - self.stats.get_value("scheduler/dequeued", 0)
        retry_count = self.stats.get_value("retry/count", 0)

        elapsed = datetime.utcnow() - self.start_time if self.start_time else None

        logger.info(
            "Progress: %d items | %d pages | %d pending | %d retries | elapsed: %s",
            items_scraped,
            pages_crawled,
            max(0, pending),
            retry_count,
            elapsed,
        )
