"""
Universal Ecommerce Crawler (Playwright-based)
Handles: pagination, product discovery, extraction, and infinite scroll.
"""

import json
import re
import asyncio
from typing import List, Dict, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Page, Playwright


class UniversalCrawler:
    def __init__(self, max_pages: int = 10, max_products: int = 50, headless: bool = True):
        self.max_pages = max_pages
        self.max_products = max_products
        self.headless = headless
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.browser: Browser | None = None
        self.playwright: Playwright | None = None

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)

    async def stop(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def crawl(self, start_url: str) -> List[Dict]:
        products: List[Dict] = []
        await self.start()

        try:
            await self._discover_products(start_url)

            for product_url in list(self.product_urls)[: self.max_products]:
                data = await self._extract_product(product_url)
                if data:
                    products.append(data)
        finally:
            await self.stop()   # 🔥 CRITICAL FIX: browser ALWAYS closes

        return products

    async def _discover_products(self, listing_url: str):
        page = await self.browser.new_page()
        current_url = listing_url
        page_count = 0

        try:
            while page_count < self.max_pages:
                if current_url in self.visited_urls:
                    break

                self.visited_urls.add(current_url)
                page_count += 1

                await page.goto(current_url, wait_until="domcontentloaded", timeout=30000)
                await self._handle_infinite_scroll(page)

                soup = BeautifulSoup(await page.content(), "html.parser")
                self._extract_links_from_page_soup(soup, current_url)

                next_url = self._find_next_page_url(soup, current_url, page_count)
                if not next_url:
                    break
                current_url = next_url
        finally:
            await page.close()

    async def _handle_infinite_scroll(self, page: Page, scroll_limit: int = 3):
        for _ in range(scroll_limit):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.2)

    def _find_next_page_url(self, soup: BeautifulSoup, current_url: str, current_page_num: int):
        link = soup.select_one('a[rel="next"]')
        if link and link.get("href"):
            return urljoin(current_url, link["href"])

        parsed = urlparse(current_url)
        if "page=" in parsed.query:
            query = re.sub(r"page=\d+", f"page={current_page_num + 1}", parsed.query)
            return parsed._replace(query=query).geturl()

        return None

    def _extract_links_from_page_soup(self, soup: BeautifulSoup, base_url: str):
        for a in soup.find_all("a", href=True):
            if "/products/" in a["href"]:
                self.product_urls.add(urljoin(base_url, a["href"]))

    async def _extract_product(self, product_url: str) -> Dict | None:
        page = await self.browser.new_page()
        try:
            await page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
            soup = BeautifulSoup(await page.content(), "html.parser")

            title = soup.find("h1")
            if not title:
                return None

            product = {
                "source_url": product_url,
                "title": title.get_text(strip=True),
                "description": None,
                "price": None,
                "currency": None,
                "availability": None,
                "category": None,
                "images": [],
            }

            desc = soup.select_one(".product__description, .product-description")
            if desc:
                product["description"] = desc.get_text(" ", strip=True)

            imgs = []
            for img in soup.find_all("img"):
                src = img.get("src")
                if src and "cdn.shopify" in src:
                    imgs.append(src.split("?")[0])
            product["images"] = list(set(imgs))[:5]

            m = re.search(r"/collections/([^/]+)/", product_url)
            if m:
                product["category"] = m.group(1).replace("-", " ").title()

            return product
        finally:
            await page.close()
