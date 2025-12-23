"""
FastAPI Backend - Ecommerce Crawl Demo (Demo-Safe)
"""

import os
import uuid
from datetime import datetime
from typing import Dict, Optional, List
from urllib.parse import urlparse, urlunparse
import ipaddress

from fastapi import (
    FastAPI,
    Depends,
    BackgroundTasks,
    HTTPException,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import (
    Base,
    engine,
    get_db,
    Job,
    Product,
    SessionLocal,
)
from crawler import UniversalCrawler

# ---------------- INIT ----------------

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Ecommerce Crawler API", version="1.0.0")

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- MODELS ----------------

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


# ---------------- HELPERS ----------------

def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if not parsed.hostname:
            return False
        ip = ipaddress.ip_address(parsed.hostname) if parsed.hostname.replace(".", "").isdigit() else None
        if ip and ip.is_private:
            return False
        return True
    except Exception:
        return False


# ---------------- ROOT ----------------

@app.get("/")
async def root():
    return {"status": "running"}


# ---------------- JOBS ----------------

@app.post("/jobs", response_model=JobResponse)
async def create_job(
    job: JobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    if not is_safe_url(job.url):
        raise HTTPException(status_code=400, detail="Unsafe URL")

    parsed = urlparse(job.url)
    normalized_url = urlunparse(parsed._replace(netloc=parsed.netloc.lower()))

    job_id = str(uuid.uuid4())

    db_job = Job(
        id=job_id,
        input_url=normalized_url,
        domain=parsed.netloc,
        status="queued",
        counters={
            "pages_visited": 0,
            "products_discovered": 0,
            "products_extracted": 0,
            "products_indexed": 0,
        },
        created_at=datetime.utcnow(),
    )

    db.add(db_job)
    db.commit()

    background_tasks.add_task(
        crawl_and_process,
        job_id,
        normalized_url,
        job.options or {},
    )

    return {
        "job_id": job_id,
        "status": db_job.status,
        "counters": db_job.counters,
    }


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job.id,
        "status": job.status,
        "counters": job.counters,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
        "error": job.error,
    }


# ---------------- SEARCH (SAFE FALLBACK) ----------------

@app.get("/search", response_model=List[ProductResponse])
async def search(
    job_id: str,
    q: str,
    limit: int = 10,
    category: Optional[str] = None,
    db: Session = Depends(get_db),
):
    query = db.query(Product).filter(Product.job_id == job_id)

    if category:
        query = query.filter(
            Product.category.isnot(None),
            Product.category.ilike(category),
        )

    products = query.all()
    if not products:
        return []

    scored = []
    words = [w for w in q.lower().split() if w.strip()]

    for p in products:
        text = f"{p.title or ''} {p.description or ''}".lower()
        score = sum(1 for w in words if w in text)
        if score > 0:
            scored.append((score, p))

    if not scored:
        scored = [(1, p) for p in products]

    scored.sort(reverse=True, key=lambda x: x[0])

    return [
        ProductResponse(
            id=p.id,
            title=p.title or "Unknown",
            price=p.price,
            images=[],
            source_url=p.source_url,
            description=p.description,
            match_reason="Keyword / fallback match",
        )
        for _, p in scored[:limit]
    ]


# ---------------- BACKGROUND CRAWL (🔥 FIXED) ----------------

async def crawl_and_process(job_id: str, url: str, options: Dict):
    db: Session = SessionLocal()
    try:
        # -------- Load job --------
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return

        # -------- Phase 1: Crawling --------
        job.status = "crawling"
        job.started_at = datetime.utcnow()
        db.commit()
        db.refresh(job)

        crawler = UniversalCrawler(
            max_pages=options.get("max_pages", 5),
            max_products=options.get("max_products", 50),
        )

        products = await crawler.crawl(url)

        # -------- Phase 2: Parsing --------
        job.status = "parsing"
        job.counters["pages_visited"] = len(crawler.visited_urls)
        job.counters["products_discovered"] = len(products)
        db.commit()
        db.refresh(job)

        # -------- Store products --------
        stored = 0
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
            stored += 1

        db.commit()

        # -------- Phase 3: Completed --------
        job.status = "completed"
        job.counters["products_extracted"] = stored
        job.counters["products_indexed"] = stored
        job.finished_at = datetime.utcnow()
        db.commit()
        db.refresh(job)

    except Exception as e:
        # -------- Failure path --------
        try:
            job.status = "failed"
            job.error = str(e)
            job.finished_at = datetime.utcnow()
            db.commit()
        except Exception:
            pass

        print(f"[crawl_and_process] Job {job_id} failed:", e)

    finally:
        db.close()

# ---------------- RUN ----------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)