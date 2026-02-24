"""
jedeschule_lite — Scrapy-free school data scraper for German schools.

Drop-in replacement for jedeschule spiders that works in any Python
environment (including Microsoft Fabric Spark notebooks).

Usage:
    from jedeschule_lite import scrape_all, scrape_state

    # Scrape all 16 Bundeslaender
    schools = scrape_all()

    # Scrape a single state
    schools = scrape_state("berlin")
"""

from jedeschule_lite.runner import scrape_all, scrape_state

__all__ = ["scrape_all", "scrape_state"]
