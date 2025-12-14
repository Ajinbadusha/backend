"""
Universal Ecommerce Crawler
Handles: pagination, infinite scroll, product discovery, extraction
"""

import asyncio
from playwright.async_api import async_playwright, Browser, Page as PlaywrightPage
from bs4 import BeautifulSoup
import json
import re
from typing import List, Dict, Set
from urllib.parse import urljoin, urlparse
import hashlib


class UniversalCrawler:
    """Crawl any ecommerce site using hybrid extraction strategy"""
    
    def __init__(self, max_pages: int = 10, max_products: int = 50, headless: bool = True):
        self.max_pages = max_pages
        self.max_products = max_products
        self.headless = headless
        self.visited_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.browser: Browser = None
        self.page: PlaywrightPage = None
        
    async def start(self):
        """Initialize browser"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.page = await self.browser.new_page()
        self.page.set_default_timeout(10000)
        
    async def stop(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
    
    async def crawl(self, start_url: str) -> List[Dict]:
        """Main crawl method - returns list of product URLs and data"""
        await self.start()
        products = []
        
        try:
            # STEP 1: Discover product URLs
            await self._discover_products(start_url)
            
            # STEP 2: Extract data from each product
            for i, product_url in enumerate(list(self.product_urls)[:self.max_products]):
                product_data = await self._extract_product(product_url)
                if product_data:
                    products.append(product_data)
                if i + 1 >= self.max_products:
                    break
        finally:
            await self.stop()
        
        return products
    
    async def _discover_products(self, listing_url: str):
        """SOW 2.3.A - Find product URLs using hybrid strategy"""
        await self.page.goto(listing_url)
        
        # Handle pagination (demo: max 3 pages)
        for page_num in range(min(3, self.max_pages)):
            await self._extract_links_from_page()
            
            # Try to find next page link
            next_button = await self.page.query_selector('a[rel="next"]') or \
                         await self.page.query_selector('button:has-text("Next")') or \
                         await self.page.query_selector('[aria-label="Next"]')
            
            if next_button:
                await next_button.click()
                await self.page.wait_for_load_state('networkidle')
            else:
                break
    
    async def _extract_links_from_page(self):
        """Extract product links using heuristics"""
        content = await self.page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        # Common product link patterns
        patterns = [
            r'/product[s]?/',
            r'/item[s]?/',
            r'/p\d+',
            r'product[-_]id=',
            r'sku=',
        ]
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Skip pagination, category, etc
            if any(skip in href.lower() for skip in ['page', 'sort', 'filter', 'category']):
                continue
            # Check if matches product pattern
            if any(re.search(p, href, re.I) for p in patterns):
                full_url = urljoin(self.page.url, href)
                if full_url not in self.visited_urls:
                    self.product_urls.add(full_url)
                    self.visited_urls.add(full_url)
    
    async def _extract_product(self, product_url: str) -> Dict:
        """SOW 2.3.B - Extract product data from page"""
        try:
            await self.page.goto(product_url)
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            product = {
                'source_url': product_url,
                'title': None,
                'description': None,
                'price': None,
                'currency': None,
                'availability': None,
                'category': None,
                'images': [],
                'sku': None,
                'variants': None,
            }
            
            # METHOD 1: Try to parse JSON-LD structured data
            product_data = self._extract_json_ld(soup)
            if product_data:
                product.update(product_data)
                return product
            
            # METHOD 2: Try common embedded JSON patterns
            product_data = self._extract_embedded_json(soup, content)
            if product_data:
                product.update(product_data)
                return product
            
            # METHOD 3: DOM fallback heuristics
            product_data = self._extract_dom_heuristics(soup)
            if product_data:
                product.update(product_data)
                return product
            
            return None
        except Exception as e:
            print(f"Error extracting {product_url}: {e}")
            return None
    
    def _extract_json_ld(self, soup: BeautifulSoup) -> Dict:
        """Extract from schema.org JSON-LD"""
        for script in soup.find_all('script', {'type': 'application/ld+json'}):
            try:
                data = json.loads(script.string)
                if data.get('@type') == 'Product':
                    return {
                        'title': data.get('name'),
                        'description': data.get('description'),
                        'price': float(data.get('offers', {}).get('price', 0)) if data.get('offers') else None,
                        'currency': data.get('offers', {}).get('priceCurrency'),
                        'images': [img.get('url') for img in data.get('image', [])] if data.get('image') else [],
                        'sku': data.get('sku'),
                        'availability': data.get('offers', {}).get('availability'),
                    }
            except:
                continue
        return None
    
    def _extract_embedded_json(self, soup: BeautifulSoup, html_text: str) -> Dict:
        """Extract from common embedded JSON (window.STATE, NEXT_DATA, etc)"""
        patterns = [
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
            r'__NEXT_DATA__\s*=\s*({.*?});',
            r'window\.STATE\s*=\s*({.*?});',
            r'Shopify\.theme\s*=\s*({.*?});',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html_text, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    # Try to find product data in parsed JSON
                    return self._extract_from_nested_json(data)
                except:
                    continue
        return None
    
    def _extract_from_nested_json(self, obj) -> Dict:
        """Recursively search for product info in nested JSON"""
        if isinstance(obj, dict):
            # Look for product fields
            result = {
                'title': obj.get('title') or obj.get('name') or obj.get('product_name'),
                'price': obj.get('price') or obj.get('amount'),
                'description': obj.get('description') or obj.get('desc'),
                'images': obj.get('images') or obj.get('image_urls') or [],
            }
            if result['title']:
                return result
            # Recurse into nested objects
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    data = self._extract_from_nested_json(v)
                    if data and data.get('title'):
                        return data
        elif isinstance(obj, list):
            for item in obj:
                data = self._extract_from_nested_json(item)
                if data and data.get('title'):
                    return data
        return None
    
    def _extract_dom_heuristics(self, soup: BeautifulSoup) -> Dict:
        """Fallback: extract using DOM heuristics"""
        product = {}
        
        # Title: usually h1 or meta og:title
        title_tag = soup.find('h1') or soup.find('meta', {'property': 'og:title'})
        product['title'] = title_tag.get_text() if title_tag and title_tag.name != 'meta' else (title_tag.get('content') if title_tag else None)
        
        # Price: look for common price selectors
        price_tags = soup.find_all(class_=re.compile(r'price', re.I))
        if price_tags:
            price_text = price_tags[0].get_text()
            price_match = re.search(r'[\$£€₹]?\s*(\d+\.?\d*)', price_text)
            if price_match:
                product['price'] = float(price_match.group(1))
        
        # Description
        desc_tag = soup.find(class_=re.compile(r'description|desc|product-info', re.I))
        product['description'] = desc_tag.get_text()[:500] if desc_tag else None
        
        # Images
        images = []
        for img in soup.find_all('img')[:6]:
            src = img.get('src') or img.get('data-src')
            if src and src.startswith('http'):
                images.append(src)
        product['images'] = images
        
        return product if product.get('title') else None

