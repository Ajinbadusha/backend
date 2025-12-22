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
    """Crawl any ecommerce site using hybrid extraction strategy with Playwright."""

    def __init__(self, max_pages: int = 10, max_products: int = 50, headless: bool = True):
        self.max_pages = max_pages
        self.max_products = max_products
        self.headless = headless
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.browser: Browser | None = None
        self.playwright: Playwright | None = None

    async def start(self):
        """Initialize Playwright and launch the browser."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)

    async def stop(self):
        """Close the browser and stop Playwright."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def _new_page(self) -> Page:
        """
        Create a new page with a realistic desktop User-Agent so
        Amazon / Flipkart and other large sites are less likely to block.
        """
        assert self.browser is not None
        return await self.browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )

    async def crawl(self, start_url: str) -> List[Dict]:
        """Main crawl method - returns list of product data dicts."""
        if not self.browser:
            # Fallback; normally start/stop are managed by the caller
            await self.start()

        products: List[Dict] = []

        # STEP 1: Discover product URLs (with Playwright for dynamic content)
        await self._discover_products(start_url)

        # STEP 2: Extract data from each product
        for i, product_url in enumerate(list(self.product_urls)[: self.max_products]):
            product_data = await self._extract_product(product_url)
            if product_data:
                products.append(product_data)
            if i + 1 >= self.max_products:
                break

        return products

    async def _discover_products(self, listing_url: str):
        """
        SOW 2.3.A - Find product URLs using hybrid strategy with Playwright.
        Handles pagination and best-effort infinite scroll.
        """
        if not self.browser:
            return

        page = await self._new_page()
        current_url = listing_url
        page_count = 0

        try:
            while page_count < self.max_pages:
                if current_url in self.visited_urls:
                    break

                self.visited_urls.add(current_url)
                page_count += 1
                print(f"Crawling page {page_count}: {current_url}")

                await page.goto(current_url, wait_until="domcontentloaded", timeout=30000)

                # Best-effort infinite scroll
                await self._handle_infinite_scroll(page)

                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                self._extract_links_from_page_soup(soup, current_url)

                next_url = self._find_next_page_url(soup, current_url, page_count)
                if next_url and next_url not in self.visited_urls:
                    current_url = next_url
                else:
                    break
        except Exception as e:
            print(f"Error discovering products from {current_url}: {e}")
        finally:
            await page.close()

    async def _handle_infinite_scroll(self, page: Page, scroll_limit: int = 3):
        """Scroll down a few times to trigger infinite loading."""
        for i in range(scroll_limit):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.5)
            print(f" -> Scrolled down {i + 1}/{scroll_limit} times.")

    def _find_next_page_url(self, soup: BeautifulSoup, current_url: str, current_page_num: int) -> str | None:
        """Find the next page URL using various heuristics."""
        # Strategy 1: rel="next" link
        next_link = soup.select_one('a[rel="next"]')
        if next_link and next_link.get("href"):
            return urljoin(current_url, next_link["href"])

        # Strategy 2: "Next" button/link text
        next_link = soup.find("a", string=re.compile(r"next|→|›", re.I))
        if next_link and next_link.get("href"):
            return urljoin(current_url, next_link["href"])

        # Strategy 3: Common pagination URL patterns (e.g., ?page=N+1)
        parsed_url = urlparse(current_url)

        # Increment a 'page' query parameter
        if "page=" in parsed_url.query:
            next_page_num = current_page_num + 1
            query = re.sub(r"page=\d+", f"page={next_page_num}", parsed_url.query)
            return parsed_url._replace(query=query).geturl()

        # Increment a path segment like /page/N+1
        if re.search(r"/page/\d+", parsed_url.path):
            next_page_num = current_page_num + 1
            path = re.sub(r"/page/\d+", f"/page/{next_page_num}", parsed_url.path)
            return parsed_url._replace(path=path).geturl()

        return None

    def _extract_links_from_page_soup(self, soup: BeautifulSoup, base_url: str):
        """
        Extract product links using heuristics from static HTML soup.

        Extended to support Amazon (/dp/) and Flipkart (/p/) plus generic
        ecommerce patterns so more sites get covered out-of-the-box.
        """
        product_links: Set[str] = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()

            # Skip obvious non-product listing params
            if any(skip in href_lower for skip in ["page=", "sort=", "filter=", "category="]):
                continue

            full_url = urljoin(base_url, href)
            full_lower = full_url.lower()

            # Amazon product detail pages
            if "amazon." in full_lower and "/dp/" in full_lower:
                product_links.add(full_url)
                continue

            # Flipkart product detail pages
            if "flipkart." in full_lower and "/p/" in full_lower:
                product_links.add(full_url)
                continue

            # Generic ecommerce product patterns
            if re.search(r"/products?/|/item/|/product-detail/|/buy/", href_lower):
                product_links.add(full_url)
                continue

        self.product_urls.update(product_links)

    async def _extract_product(self, product_url: str) -> Dict | None:
        """SOW 2.3.B - Extract product data from product page using Playwright."""
        if not self.browser:
            return None

        page = await self._new_page()
        try:
            await page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            product: Dict = {
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

            # METHOD 1: JSON-LD
            product_data = self._extract_json_ld(soup)
            if product_data and product_data.get("title"):
                product.update(product_data)
                return product

            # METHOD 2: Embedded JSON (window state, NEXT_DATA, etc.)
            product_data = self._extract_embedded_json(soup, html)
            if product_data and product_data.get("title"):
                product.update(product_data)
                return product

            # METHOD 3: DOM heuristics
            product_data = self._extract_dom_heuristics(soup)
            if product_data and product_data.get("title"):
                product.update(product_data)
                return product

            return None
        except Exception as e:
            print(f"Error extracting {product_url}: {e}")
            return None
        finally:
            await page.close()

    def _extract_json_ld(self, soup: BeautifulSoup) -> Dict | None:
        """Extract from schema.org JSON-LD."""
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                if not script.string:
                    continue
                data = json.loads(script.string)

                # Some sites wrap JSON-LD in a list
                if isinstance(data, list):
                    for item in data:
                        result = self._extract_product_from_jsonld_item(item)
                        if result:
                            return result
                else:
                    result = self._extract_product_from_jsonld_item(data)
                    if result:
                        return result
            except Exception:
                continue
        return None

    def _extract_product_from_jsonld_item(self, data: Dict) -> Dict | None:
        if not isinstance(data, dict):
            return None

        # If it's an Offer, try to find the parent Product
        if data.get("@type") in {"Offer", "offer"} and data.get("itemOffered"):
            data = data.get("itemOffered")

        if not isinstance(data, dict) or data.get("@type") not in {"Product", "product"}:
            return None

        images: List[str] = []
        img_field = data.get("image")
        if isinstance(img_field, list):
            for img in img_field:
                if isinstance(img, dict) and img.get("url"):
                    images.append(img["url"])
                elif isinstance(img, str):
                    images.append(img)
        elif isinstance(img_field, dict) and img_field.get("url"):
            images.append(img_field["url"])
        elif isinstance(img_field, str):
            images.append(img_field)

        offers = data.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}

        price_raw = offers.get("price")
        try:
            price_val = float(price_raw) if price_raw is not None else None
        except Exception:
            price_val = None

        return {
            "title": data.get("name"),
            "description": data.get("description"),
            "price": price_val,
            "currency": offers.get("priceCurrency"),
            "images": images,
            "sku": data.get("sku"),
            "availability": offers.get("availability"),
        }

    def _extract_embedded_json(self, soup: BeautifulSoup, html_text: str) -> Dict | None:
        """Extract from common embedded JSON (window.STATE, NEXT_DATA, etc.)."""
        patterns = [
            r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
            r"__NEXT_DATA__\s*=\s*({.*?});",
            r"window\.STATE\s*=\s*({.*?});",
            r"Shopify\.theme\s*=\s*({.*?});",
        ]

        for pattern in patterns:
            match = re.search(pattern, html_text, re.DOTALL)
            if match:
                try:
                    json_str = match.group(1).strip()
                    if json_str.endswith(";"):
                        json_str = json_str[:-1]
                    data = json.loads(json_str)
                    return self._extract_from_nested_json(data)
                except Exception:
                    continue
        return None

    def _extract_from_nested_json(self, obj) -> Dict | None:
        """Recursively search for product info in nested JSON."""
        if isinstance(obj, dict):
            result = {
                "title": obj.get("title") or obj.get("name") or obj.get("product_name"),
                "price": obj.get("price") or obj.get("amount"),
                "description": obj.get("description") or obj.get("desc"),
                "images": obj.get("images") or obj.get("image_urls") or [],
            }
            if result["title"]:
                price = result["price"]
                try:
                    result["price"] = float(price) if price is not None else None
                except Exception:
                    pass
                return result

            for v in obj.values():
                if isinstance(v, (dict, list)):
                    data = self._extract_from_nested_json(v)
                    if data and data.get("title"):
                        return data

        elif isinstance(obj, list):
            for item in obj:
                data = self._extract_from_nested_json(item)
                if data and data.get("title"):
                    return data

        return None

    def _extract_dom_heuristics(self, soup: BeautifulSoup) -> Dict | None:
        """Fallback: extract using DOM heuristics from static HTML."""
        product: Dict = {}

        # Title: usually h1 or meta og:title
        title_tag = soup.find("h1") or soup.find("meta", {"property": "og:title"})
        if title_tag:
            if title_tag.name == "meta":
                product["title"] = title_tag.get("content")
            else:
                product["title"] = title_tag.get_text(strip=True)

        # Price: look for common price selectors
        price_tags = soup.find_all(class_=re.compile(r"price", re.I))
        if price_tags:
            price_text = price_tags[0].get_text()
            price_match = re.search(r"[\$£€₹]?\s*(\d+\.?\d*)", price_text)
            if price_match:
                try:
                    product["price"] = float(price_match.group(1))
                except Exception:
                    pass

        # Description
        desc_tag = soup.find(class_=re.compile(r"description|desc|product-info", re.I))
        if desc_tag:
            product["description"] = desc_tag.get_text(strip=True)[:500]

        # Images
        images: List[str] = []
        for img in soup.find_all("img")[:6]:
            src = img.get("src") or img.get("data-src")
            if src and src.startswith("http"):
                images.append(src)
        product["images"] = images

        return product if product.get("title") else None
