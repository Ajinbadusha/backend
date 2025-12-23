"""
FastAPI Backend - Ecommerce Crawl Demo (Fixed & Stable)
"""

import uuid
from datetime import datetime
from typing import Dict, Optional, List
from urllib.parse import urlparse, urlunparse
import ipaddress

from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import Base, engine, get_db, Job, Product, SessionLocal
from crawler import UniversalCrawler

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Ecommerce Crawler API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class JobCreate(BaseModel):
    url: str
    options: Optional[Dict] = {"max_pages": 5, "max_products": 50}


class JobResponse(BaseModel):
    job_id: str
    status: str
    counters: Dict


class ProductResponse(BaseModel):
    id: str
    title: str
    source_url: str
    description: Optional[str]


def is_safe_url(url: str) -> bool:
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        ip = ipaddress.ip_address(host) if host.replace(".", "").isdigit() else None
        return not ip.is_private if ip else True
    except Exception:
        return False


@app.post("/jobs", response_model=JobResponse)
async def create_job(job: JobCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    if not is_safe_url(job.url):
        raise HTTPException(status_code=400, detail="Unsafe URL")

    parsed = urlparse(job.url)
    normalized = urlunparse(parsed._replace(netloc=parsed.netloc.lower()))

    job_id = str(uuid.uuid4())
    db_job = Job(
        id=job_id,
        input_url=normalized,
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

    background_tasks.add_task(crawl_and_process, job_id, normalized, job.options or {})
    return {"job_id": job_id, "status": "queued", "counters": db_job.counters}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(404)
    return job


@app.get("/search", response_model=List[ProductResponse])
async def search(job_id: str, q: str, db: Session = Depends(get_db)):
    products = db.query(Product).filter(Product.job_id == job_id).all()
    return [
        ProductResponse(
            id=p.id,
            title=p.title,
            source_url=p.source_url,
            description=p.description,
        )
        for p in products
        if q.lower() in (p.title or "").lower()
    ]


async def crawl_and_process(job_id: str, url: str, options: Dict):
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return

        job.status = "crawling"
        db.commit()

        crawler = UniversalCrawler(
            max_pages=options.get("max_pages", 5),
            max_products=options.get("max_products", 50),
        )

        products = await crawler.crawl(url)

        job.status = "parsing"
        job.counters["products_discovered"] = len(products)
        db.commit()

        for data in products:
            db.add(
                Product(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    source_url=data["source_url"],
                    title=data["title"],
                    description=data.get("description"),
                    category=data.get("category"),
                    raw_json=data,
                )
            )

        db.commit()

        job.status = "completed"
        job.counters["products_extracted"] = len(products)
        job.counters["products_indexed"] = len(products)
        job.finished_at = datetime.utcnow()
        db.commit()

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.finished_at = datetime.utcnow()
        db.commit()
    finally:
        db.close()
