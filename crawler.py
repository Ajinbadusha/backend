"""
Universal Ecommerce Crawler (Playwright-based)
Handles: pagination, product discovery, extraction, and infinite scroll.
"""

import json
import re
import asyncio
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Page, Playwright


class UniversalCrawler:
    """Crawl any ecommerce site using hybrid extraction strategy with Playwright."""

    def __init__(
        self,
        max_pages: int = 10,
        max_products: int = 50,
        headless: bool = True,
        navigation_timeout_ms: int = 30000,
    ):
        self.max_pages = max_pages
        self.max_products = max_products
        self.headless = headless
        self.navigation_timeout_ms = navigation_timeout_ms

        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.browser: Optional[Browser] = None
        self.playwright: Optional[Playwright] = None

    async def start(self):
        """Initialize Playwright and launch the browser."""
        if self.playwright or self.browser:
            return
        self.playwright = await async_playwright().start()
        # Use container‑friendly Chromium flags and shorter launch timeout
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
            timeout=60000,
        )

    async def stop(self):
        """Close the browser and stop Playwright."""
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None

    async def crawl(self, start_url: str) -> List[Dict]:
        """Main crawl method - returns list of product data dicts."""
        if not self.browser:
            # This is a fallback, the main app should call start/stop
            await self.start()

        products: List[Dict] = []

        # STEP 1: Discover product URLs (with Playwright for dynamic content)
        await self._discover_products(start_url)

        # STEP 2: Extract data from each product
        # Use a new page for each product to prevent state leakage
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

        page = await self.browser.new_page()
        await page.set_viewport_size({"width": 1280, "height": 720})
        current_url = listing_url
        page_count = 0

        try:
            while page_count < self.max_pages:
                # Normalize URL to reduce duplicate checks
                normalized = self._normalize_url(current_url)
                if normalized in self.visited_urls:
                    break
                self.visited_urls.add(normalized)
                page_count += 1

                print(f"[Crawler] Crawling page {page_count}: {current_url}")
                try:
                    await page.goto(
                        current_url,
                        wait_until="domcontentloaded",
                        timeout=self.navigation_timeout_ms,
                    )
                except Exception as nav_err:
                    # Some sites never fire domcontentloaded; try a best-effort wait
                    print(
                        f"[Crawler] Navigation error on {current_url}: {nav_err}. "
                        "Trying again with no wait_until."
                    )
                    try:
                        await page.goto(
                            current_url,
                            timeout=self.navigation_timeout_ms,
                        )
                    except Exception as nav_err2:
                        print(
                            f"[Crawler] Failed to load {current_url} on retry: {nav_err2}"
                        )
                        break

                # Best-effort infinite scroll (SOW 2.3.A)
                await self._handle_infinite_scroll(page)

                # Get the final HTML content after dynamic loading
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                self._extract_links_from_page_soup(soup, current_url)

                # Check for pagination links
                next_url = self._find_next_page_url(soup, current_url, page_count)

                if next_url:
                    current_url = next_url
                else:
                    break
        except Exception as e:
            print(f"[Crawler] Error discovering products from {current_url}: {e}")
        finally:
            await page.close()

    async def _handle_infinite_scroll(self, page: Page, scroll_limit: int = 3):
        """Scroll down a few times to trigger infinite loading."""
        for i in range(scroll_limit):
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception as e:
                print(f"[Crawler] Scroll error: {e}")
                break
            await asyncio.sleep(1.5)
            print(f"  -> Scrolled down {i + 1}/{scroll_limit} times.")

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for duplicate detection."""
        parsed = urlparse(url)
        # Drop fragment and standardize scheme/host
        normalized = parsed._replace(
            fragment="",
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
        )
        return urlunparse(normalized)

    def _find_next_page_url(
        self, soup: BeautifulSoup, current_url: str, current_page_num: int
    ) -> Optional[str]:
        """Find the next page URL using various heuristics."""
        # Strategy 1: rel="next" link
        next_link = soup.select_one('a[rel="next"]')
        if next_link and next_link.get("href"):
            return urljoin(current_url, next_link.get("href"))

        # Strategy 2: "Next" button/link text
        next_link = soup.find("a", string=re.compile(r"next|→|›", re.I))
        if next_link:
            href = next_link.get("href")
            if href:
                return urljoin(current_url, href)

        # Strategy 3: Common pagination URL patterns (e.g., ?page=N+1)
        parsed_url = urlparse(current_url)

        # Try to increment a 'page' parameter
        if "page=" in parsed_url.query:
            next_page_num = current_page_num + 1
            query = re.sub(r"page=\d+", f"page={next_page_num}", parsed_url.query)
            return parsed_url._replace(query=query).geturl()

        # Try to increment a path segment like /page/N+1
        if re.search(r"/page/\d+", parsed_url.path):
            next_page_num = current_page_num + 1
            path = re.sub(r"/page/\d+", f"/page/{next_page_num}", parsed_url.path)
            return parsed_url._replace(path=path).geturl()

        return None

    def _extract_links_from_page_soup(self, soup: BeautifulSoup, base_url: str):
        """Extract product links using heuristics from static HTML soup."""
        product_links: Set[str] = set()

        # Strategy 1: Find all links that look like product URLs
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if not href:
                continue

            lower_href = href.lower()

            # Skip obvious non-product / filter URLs
            if any(skip in lower_href for skip in ["sort=", "filter=", "#", "login"]):
                continue

            # Match common product patterns including collections/.../products/...
            if re.search(
                r"/products/|/collections/.+?/products/|/online-pharmacy/p/",
                lower_href,
            ):
                if any(skip in lower_href for skip in ["page=", "category="]):
                    continue
                full_url = urljoin(base_url, href)
                product_links.add(full_url)

        # Strategy 2: Find links within common product card elements
        for card in soup.find_all(
            class_=re.compile(r"product-card|grid__item|product-item|product", re.I)
        ):
            link = card.find("a", href=True)
            if not link:
                continue
            href = link.get("href")
            if not href:
                continue
            full_url = urljoin(base_url, href)
            product_links.add(full_url)

        self.product_urls.update(product_links)

    async def _extract_product(self, product_url: str) -> Optional[Dict]:
        """SOW 2.3.B - Extract product data from product page using Playwright."""
        if not self.browser:
            return None

        page = await self.browser.new_page()
        await page.set_viewport_size({"width": 1280, "height": 720})
        try:
            try:
                await page.goto(
                    product_url,
                    wait_until="domcontentloaded",
                    timeout=self.navigation_timeout_ms,
                )
            except Exception as nav_err:
                print(
                    f"[Crawler] Navigation error on product {product_url}: {nav_err}. "
                    "Retrying with lighter waiting."
                )
                try:
                    # Relax waiting condition on retry
                    await page.goto(
                        product_url,
                        timeout=self.navigation_timeout_ms,
                    )
                except Exception as nav_err2:
                    print(
                        f"[Crawler] Failed to load product {product_url} on retry: {nav_err2}"
                    )
                    return None

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

            print(f"[Crawler] No product data extracted from {product_url}")
            return None
        except Exception as e:
            print(f"[Crawler] Error extracting {product_url}: {e}")
            return None
        finally:
            await page.close()

    def _extract_json_ld(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract from schema.org JSON-LD."""
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                text = script.string or script.get_text(strip=True)
                if not text:
                    continue
                data = json.loads(text)

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

    def _extract_product_from_jsonld_item(self, data: Dict) -> Optional[Dict]:
        if not isinstance(data, dict):
            return None

        # Check for Product or Offer type
        type_field = data.get("@type")
        if isinstance(type_field, list):
            if not any(t.lower() in {"product", "offer"} for t in map(str, type_field)):
                return None
        else:
            if str(type_field).lower() not in {"product", "offer"}:
                return None

        # If it's an Offer, try to find the parent Product
        if str(type_field).lower() == "offer" and data.get("itemOffered"):
            data = data.get("itemOffered")
            if not isinstance(data, dict):
                return None

        images: List[str] = []
        img_field = data.get("image")
        if isinstance(img_field, list):
            for img in img_field:
                if isinstance(img, dict) and img.get("url"):
                    images.append(img["url"])
                elif isinstance(img, str):
                    images.append(img)
        elif isinstance(img_field, dict):
            if img_field.get("url"):
                images.append(img_field["url"])
        elif isinstance(img_field, str):
            images.append(img_field)

        offers = data.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}

        price_raw = offers.get("price") or data.get("price")
        try:
            price_val = float(price_raw) if price_raw is not None else None
        except Exception:
            price_val = None

        return {
            "title": data.get("name") or data.get("title"),
            "description": data.get("description"),
            "price": price_val,
            "currency": offers.get("priceCurrency"),
            "images": images,
            "sku": data.get("sku"),
            "availability": offers.get("availability"),
        }

    def _extract_embedded_json(
        self, soup: BeautifulSoup, html_text: str
    ) -> Optional[Dict]:
        """Extract from common embedded JSON (window.STATE, NEXT_DATA, etc.)."""
        patterns = [
            r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
            r"__NEXT_DATA__\s*=\s*({.*?});",
            r"window\.STATE\s*=\s*({.*?});",
            r"Shopify\.theme\s*=\s*({.*?});",
        ]

        for pattern in patterns:
            match = re.search(pattern, html_text, re.DOTALL)
            if not match:
                continue
            try:
                json_str = match.group(1).strip()
                json_str = json_str.rstrip(";/ \n\t")
                data = json.loads(json_str)
                return self._extract_from_nested_json(data)
            except Exception:
                continue
        return None

    def _extract_from_nested_json(self, obj) -> Optional[Dict]:
        """Recursively search for product info in nested JSON."""
        if isinstance(obj, dict):
            result = {
                "title": obj.get("title")
                or obj.get("name")
                or obj.get("product_name"),
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

    def _extract_dom_heuristics(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Fallback: extract using DOM heuristics from static HTML."""
        product: Dict = {}

        # Title: usually h1 or meta og:title
        title_tag = soup.find("h1") or soup.find("meta", {"property": "og:title"})
        if title_tag:
            if getattr(title_tag, "name", "") == "meta":
                product["title"] = title_tag.get("content")
            else:
                product["title"] = title_tag.get_text(strip=True)

        # Price: look for common price selectors
        price_tags = soup.find_all(class_=re.compile(r"price", re.I))
        if price_tags:
            price_text = price_tags[0].get_text(" ", strip=True)
            price_match = re.search(r"[\$£€₹]?\s*(\d+\.?\d*)", price_text)
            if price_match:
                try:
                    product["price"] = float(price_match.group(1))
                except Exception:
                    pass

        # Description
        desc_tag = soup.find(
            class_=re.compile(r"description|desc|product-info", re.I)
        )
        if desc_tag:
            product["description"] = desc_tag.get_text(strip=True)[:500]

        # Images
        images: List[str] = []
        for img in soup.find_all("img")[:6]:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
            if src and src.startswith("http"):
                images.append(src)
        product["images"] = images

        return product if product.get("title") else None
