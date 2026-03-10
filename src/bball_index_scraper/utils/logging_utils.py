#logging utilities for bball_index_scraper    +   Provides consistent logging setup and formatting.


import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
        level: str = "INFO",
        log_file: Optional[Path] = None,
        format_string: Optional[str] = None,
) -> logging.Logger:
    """Setup logging configuration.

    level: Log level (DEBUG, INFO, WARNING, ERROR)
    log_file: Optional file to write logs to
    format_string: Custom format string

    returns root logger"""
    if format_string is None:
        format_string = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

    #configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    #clear existing handlers
    root_logger.handlers.clear()

    #console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(format_string))
    root_logger.addHandler(console_handler)

    #file handler (optional)
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(file_handler)

    #reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("scrapy").setLevel(logging.INFO)
    logging.getLogger("playwright").setLevel(logging.WARNING)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    #Get logger instance + returns logger instance

    return logging.getLogger(name)


class SpiderLogger:
    #Context-aware logger for spider operations

    def __init__(self, spider_name: str):
        self.logger = logging.getLogger(spider_name)
        self.spider_name = spider_name

    def started(self, url: str):
        #log URL start
        self.logger.info("Started: %s", url)

    def success(self, url: str, source: str, stat_count: int):
        #log successful extraction
        self.logger.info("Success [%s]: %s - %d stats", source, url, stat_count)

    def retry(self, url: str, attempt: int, error: str):
        #log retry attempt
        self.logger.warning("Retry %d: %s - %s", attempt, url, error)

    def skipped(self, url: str, reason: str):
        #log skipped URL
        self.logger.info("Skipped: %s - %s", url, reason)

    def failed(self, url: str, error: str):
        #log failed extraction
        self.logger.error("Failed: %s - %s", url, error)

    def auth_expired(self, url: str):
        #log auth expiration
        self.logger.warning("Auth expired: %s", url)

    def extraction_source(self, url: str, source: str):
        #log extraction source detection
        self.logger.debug("Extraction source for %s: %s", url, source)
