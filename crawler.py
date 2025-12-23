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

    async def _new_page(self) -> Page:
        assert self.browser is not None
        return await self.browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )

    async def crawl(self, start_url: str) -> List[Dict]:
        if not self.browser:
            await self.start()

        products: List[Dict] = []

        await self._discover_products(start_url)

        for i, product_url in enumerate(list(self.product_urls)[: self.max_products]):
            product_data = await self._extract_product(product_url)
            if product_data:
                products.append(product_data)
            if i + 1 >= self.max_products:
                break

        return products

    async def _discover_products(self, listing_url: str):
        page = await self._new_page()
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
            await asyncio.sleep(1.5)

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
            href = a["href"].lower()
            if "/products/" in href:
                self.product_urls.add(urljoin(base_url, a["href"]))

    async def _extract_product(self, product_url: str) -> Dict | None:
        page = await self._new_page()
        try:
            await page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
            soup = BeautifulSoup(await page.content(), "html.parser")

            product = {
                "source_url": product_url,
                "title": None,
                "description": None,
                "price": None,
                "currency": None,
                "availability": None,
                "category": None,
                "images": [],
                "sku": None,
                "variants": None,
            }

            # ---------- TITLE ----------
            h1 = soup.find("h1")
            if h1:
                product["title"] = h1.get_text(strip=True)

            # ---------- DESCRIPTION ----------
            desc = soup.select_one(".product__description, .product-description")
            if desc:
                product["description"] = desc.get_text(" ", strip=True)

            # ---------- PRICE ----------
            price_tag = soup.find(attrs={"itemprop": "price"})
            if price_tag:
                try:
                    product["price"] = float(price_tag.get("content"))
                    product["currency"] = "INR"
                except Exception:
                    pass

            # ---------- IMAGES ----------
            imgs = []
            for img in soup.find_all("img"):
                src = img.get("src")
                if src and "cdn.shopify" in src:
                    imgs.append(src.split("?")[0])
            product["images"] = list(set(imgs))[:5]

            # ---------- CATEGORY (FIX) ----------
            crumbs = soup.select("nav.breadcrumb a, .breadcrumb a")
            if crumbs:
                product["category"] = crumbs[-1].get_text(strip=True)
            else:
                m = re.search(r"/collections/([^/]+)/", product_url)
                if m:
                    product["category"] = m.group(1).replace("-", " ").title()

            return product if product["title"] else None
        finally:
            await page.close()