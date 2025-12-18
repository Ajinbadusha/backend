"""
Universal Ecommerce Crawler
Handles: pagination, product discovery, extraction

This version uses plain HTTP requests + BeautifulSoup only,
so it works on hosts where Playwright browsers cannot run.
"""

import json
import re
from typing import List, Dict, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class UniversalCrawler:
    """Crawl any ecommerce site using hybrid extraction strategy"""

    def __init__(self, max_pages: int = 10, max_products: int = 50, headless: bool = True):
        self.max_pages = max_pages
        self.max_products = max_products
        self.headless = headless  # kept for compatibility, but unused
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    async def start(self):
        """Kept for backwards compatibility (no-op for requests-based crawler)."""
        return

    async def stop(self):
        """Kept for backwards compatibility (no-op for requests-based crawler)."""
        return

    async def crawl(self, start_url: str) -> List[Dict]:
        """Main crawl method - returns list of product data dicts."""
        products: List[Dict] = []

        # STEP 1: Discover product URLs (basic pagination via links)
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
        SOW 2.3.A - Find product URLs using hybrid strategy with improved pagination.
        """
        current_url = listing_url
        page_num = 1

        for _ in range(min(self.max_pages, 10)):
            if current_url in self.visited_urls:
                break
            self.visited_urls.add(current_url)

            try:
                resp = self.session.get(current_url, timeout=20)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")

                self._extract_links_from_page_soup(soup, current_url)

                # Try multiple pagination strategies
                next_url = None

                # Strategy 1: rel="next" link
                next_link = soup.select_one('a[rel="next"]')
                if next_link and next_link.get("href"):
                    next_url = urljoin(current_url, next_link["href"])

                # Strategy 2: "Next" button/link text
                if not next_url:
                    next_link = soup.find("a", string=re.compile(r"next|→|›", re.I))
                    if next_link and next_link.get("href"):
                        next_url = urljoin(current_url, next_link["href"])

                # Strategy 3: Common pagination URL patterns
                if not next_url:
                    page_num += 1
                    # Try ?page=N
                    if "?" in listing_url:
                        next_url = re.sub(r"page=\d+", f"page={page_num}", listing_url)
                        if next_url == listing_url:
                            next_url = f"{listing_url}&page={page_num}"
                    else:
                        next_url = f"{listing_url}?page={page_num}"

                    # Try /page/N pattern
                    if "/page/" not in next_url:
                        base = listing_url.rstrip("/")
                        next_url = f"{base}/page/{page_num}"

                if next_url and next_url not in self.visited_urls:
                    current_url = next_url
                else:
                    break

            except Exception as e:
                print(f"Error discovering products from {current_url}: {e}")
                break

    def _extract_links_from_page_soup(self, soup: BeautifulSoup, base_url: str):
        """Extract product links using heuristics from static HTML soup."""
        patterns = [
            r"/product[s]?/",
            r"/item[s]?/",
            r"/p\d+",
            r"product[-_]id=",
            r"sku=",
        ]

        for a in soup.find_all("a", href=True):
            href = a["href"]

            # Skip obvious non-product links
            if any(skip in href.lower() for skip in ["page=", "sort=", "filter=", "category="]):
                continue

            if any(re.search(p, href, re.I) for p in patterns):
                full_url = urljoin(base_url, href)
                if full_url not in self.product_urls:
                    self.product_urls.add(full_url)

    async def _extract_product(self, product_url: str) -> Dict:
        """SOW 2.3.B - Extract product data from static product page."""
        try:
            resp = self.session.get(product_url, timeout=20)
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

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

            # METHOD 1: JSON-LD
            product_data = self._extract_json_ld(soup)
            if product_data:
                product.update(product_data)
                return product

            # METHOD 2: Embedded JSON (window state, NEXT_DATA, etc.)
            product_data = self._extract_embedded_json(soup, html)
            if product_data:
                product.update(product_data)
                return product

            # METHOD 3: DOM heuristics
            product_data = self._extract_dom_heuristics(soup)
            if product_data:
                product.update(product_data)
                return product

            return None
        except Exception as e:
            print(f"Error extracting {product_url}: {e}")
            return None

    def _extract_json_ld(self, soup: BeautifulSoup) -> Dict:
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

    def _extract_product_from_jsonld_item(self, data: Dict) -> Dict:
        if not isinstance(data, dict):
            return None
        if data.get("@type") not in {"Product", "product"}:
            return None

        images = []
        img_field = data.get("image")
        if isinstance(img_field, list):
            for img in img_field:
                if isinstance(img, dict) and img.get("url"):
                    images.append(img["url"])
                elif isinstance(img, str):
                    images.append(img)
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

    def _extract_embedded_json(self, soup: BeautifulSoup, html_text: str) -> Dict:
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
                    data = json.loads(match.group(1))
                    return self._extract_from_nested_json(data)
                except Exception:
                    continue
        return None

    def _extract_from_nested_json(self, obj) -> Dict:
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

    def _extract_dom_heuristics(self, soup: BeautifulSoup) -> Dict:
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
        images = []
        for img in soup.find_all("img")[:6]:
            src = img.get("src") or img.get("data-src")
            if src and src.startswith("http"):
                images.append(src)
        product["images"] = images

        return product if product.get("title") else None
