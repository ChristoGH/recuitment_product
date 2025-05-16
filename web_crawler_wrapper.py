"""
Web Crawler Wrapper

This module provides a clean async interface for the web crawler.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime

from crawl4ai import AsyncWebCrawler, BrowserConfig

# Disable colorama output globally
os.environ["ANSI_COLORS_DISABLED"] = "1"


@asynccontextmanager
async def get_crawler():
    """
    Async context manager for the web crawler.
    Handles proper initialization and cleanup.
    """
    async with AsyncWebCrawler(config=BrowserConfig()) as crawler:
        yield crawler


async def crawl_website(url: str):
    """
    Crawl a website and return the result.

    Args:
        url: The URL to crawl

    Returns:
        The crawler result object
    """
    async with get_crawler() as crawler:
        u = url.strip()

        # Crawl4AI ≥ 0.6: new coroutine name
        if hasattr(crawler, "arun"):
            return await crawler.arun(u)  # ts added automatically

        # Crawl4AI 0.5-series fallback
        return await crawler.crawl(u, ts=datetime.utcnow())
