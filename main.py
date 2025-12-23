"""
FastAPI Backend - REST API + WebSocket
"""

import os
import uuid
from datetime import datetime
from typing import Dict, Optional, List
from urllib.parse import urlparse, urlunparse
import math
import hashlib
import ipaddress
from io import BytesIO
import io
import csv
import asyncio
import aiohttp

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    Depends,
    BackgroundTasks,
    HTTPException,
    Header,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from PIL import Image, ImageDraw, ImageFont

from database import (
    Base,
    engine,
    get_db,
    Job,
    Product,
    ProductImage,
    ProductEnrichment,
    ProductVector,
    Page,
    SessionLocal,
    JobLog,
)
from crawler import UniversalCrawler
from enrichment import AIEnrichment

# ------------------ INIT ------------------

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Ecommerce Crawler API", version="1.0.0")
app.mount("/static", StaticFiles(directory="."), name="static")

# ------------------ CORS ------------------

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ MODELS ------------------

class JobCreate(BaseModel):
    url: str
    options: Optional[Dict] = {
        "max_pages": 5,
        "max_products": 50,
        "download_images": True,
    }


class JobResponse(BaseModel):
    job_id: str
    status: str
    counters: Dict


class ProductResponse(BaseModel):
    id: str
    title: str
    price: Optional[float]
    images: List[str]
    source_url: str
    description: Optional[str]
    match_reason: Optional[str]


# ------------------ HELPERS ------------------

def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            return False
        ip = ipaddress.ip_address(host) if host.replace(".", "").isdigit() else None
        if ip and ip.is_private:
            return False
        return True
    except Exception:
        return False


# ------------------ ROOT ------------------

@app.get("/")
async def root():
    return {"status": "running"}


# ------------------ JOBS ------------------

@app.post("/jobs", response_model=JobResponse)
async def create_job(
    job: JobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    if not is_safe_url(job.url):
        raise HTTPException(status_code=400, detail="Unsafe URL")

    parsed = urlparse(job.url)
    norm_url = urlunparse(parsed._replace(netloc=parsed.netloc.lower()))

    job_id = str(uuid.uuid4())
    db_job = Job(
        id=job_id,
        input_url=norm_url,
        domain=parsed.netloc,
        status="queued",
        counters={
            "products_extracted": 0,
            "products_enriched": 0,
            "products_indexed": 0,
        },
    )
    db.add(db_job)
    db.commit()

    background_tasks.add_task(crawl_and_process, job_id, norm_url, job.options)

    return {"job_id": job_id, "status": "queued", "counters": db_job.counters}


# ------------------ SEARCH (✅ FIXED) ------------------

@app.get("/search", response_model=List[ProductResponse])
async def search(
    job_id: str,
    q: str,
    limit: int = 10,
    category: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    availability: Optional[str] = None,
    db: Session = Depends(get_db),
):
    base_query = db.query(Product).filter(Product.job_id == job_id)

    # ✅ FIX 1: category-safe filtering
    if category:
        base_query = base_query.filter(
            Product.category.isnot(None),
            Product.category.ilike(category),
        )

    if min_price is not None:
        base_query = base_query.filter(Product.price >= min_price)
    if max_price is not None:
        base_query = base_query.filter(Product.price <= max_price)
    if availability:
        base_query = base_query.filter(Product.availability.ilike(f"%{availability}%"))

    products = base_query.all()
    if not products:
        return []

    scored = []
    query_words = [w for w in q.lower().split() if w.strip()]

    for prod in products:
        text = f"{prod.title or ''} {prod.description or ''}".lower()
        score = sum(1 for w in query_words if w in text)
        if score > 0:
            scored.append((score, prod))

    # ✅ FIX 2: fallback – NEVER return empty
    if not scored:
        scored = [(1, p) for p in products]

    scored.sort(reverse=True, key=lambda x: x[0])

    return [
        ProductResponse(
            id=p.id,
            title=p.title or "Unknown",
            price=p.price,
            images=[img.storage_url for img in p.images],
            source_url=p.source_url,
            description=p.description,
            match_reason="Keyword / fallback match",
        )
        for _, p in scored[:limit]
    ]


# ------------------ BACKGROUND TASK ------------------

async def crawl_and_process(job_id: str, url: str, options: Dict):
    db: Session = SessionLocal()
    try:
        crawler = UniversalCrawler(
            max_pages=options.get("max_pages", 5),
            max_products=options.get("max_products", 50),
        )

        products = await crawler.crawl(url)

        for data in products:
            product = Product(
                id=str(uuid.uuid4()),
                job_id=job_id,
                source_url=data["source_url"],
                title=data.get("title"),
                description=data.get("description"),
                price=data.get("price"),
                currency=data.get("currency"),
                availability=data.get("availability"),
                category=data.get("category"),
                raw_json=data,
            )
            db.add(product)

        db.commit()

    except Exception as e:
        print(f"Job {job_id} failed:", e)
    finally:
        db.close()


# ------------------ RUN ------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)