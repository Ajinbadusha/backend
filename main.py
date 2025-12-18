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

import asyncio
import aiohttp
from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    Depends,
    BackgroundTasks,
    HTTPException,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from PIL import Image

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
# from celery_app import celery  # not used yet

# Initialize database schema
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Ecommerce Crawler API", version="1.0.0")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "service": "ecommerce-crawler-api",
        "version": "1.0.0"
    }

@app.get("/debug-routes")
async def debug_routes():
    """Debug endpoint to list all registered routes"""
    return [{"path": r.path, "methods": r.methods, "name": r.name} for r in app.routes]


# CORS for frontend - Updated for production
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:5173",  # Local development
        "http://localhost:3000",  # Alternative local port
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store WebSocket connections for real-time updates
connections: Dict[str, List[WebSocket]] = {}


class JobCreate(BaseModel):
    url: str
    options: Optional[Dict] = {
        "max_pages": 5,
        "max_products": 50,
        "follow_pagination": True,
        "follow_links": True,
        "download_images": True,
        "crawl_speed": "normal",  # slow, normal, fast
    }


class JobResponse(BaseModel):
    job_id: str
    status: str
    counters: Dict


class JobListItem(BaseModel):
    id: str
    input_url: str
    status: str
    created_at: datetime
    finished_at: Optional[datetime]
    counters: Dict


class ProductResponse(BaseModel):
    id: str
    title: str
    price: Optional[float]
    images: List[str]
    source_url: str
    description: Optional[str] = None
    match_reason: Optional[str] = None


def log_job_event(db: Session, job_id: str, level: str, message: str):
    """Add a log entry for a job."""
    log_entry = JobLog(
        id=str(uuid.uuid4()),
        job_id=job_id,
        level=level,
        message=message,
    )
    db.add(log_entry)
    db.commit()


def is_safe_url(url: str) -> bool:
    """
    Validate URL to prevent SSRF attacks.
    Blocks localhost, private IPs, and link-local addresses.
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname

        if not hostname:
            return False

        # Block localhost-style hosts
        if hostname in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}:
            return False

        # Try to interpret hostname directly as IP
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False
        except ValueError:
            # Not a literal IP, check common private ranges in hostnames
            if hostname.startswith("192.168.") or hostname.startswith("10.") or hostname.startswith("172."):
                return False

        return True
    except Exception:
        return False


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """
    WebSocket for live job status.

    Client connects to: /ws?job_id=<job_uuid>
    """
    await websocket.accept()

    if job_id not in connections:
        connections[job_id] = []
    connections[job_id].append(websocket)

    try:
        while True:
            db: Session = SessionLocal()
            try:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job:
                    await websocket.send_json(
                        {
                            "status": job.status,
                            "counters": job.counters or {},
                            "error": job.error,
                        }
                    )
            finally:
                db.close()

            await asyncio.sleep(2)  # update every 2 seconds
    except WebSocketDisconnect:
        if job_id in connections and websocket in connections[job_id]:
            connections[job_id].remove(websocket)


async def broadcast_status(job_id: str, db: Session):
    """
    Broadcast the latest job status and counters to all connected clients.
    """
    if job_id not in connections:
        return

    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        return

    payload = {
        "status": job.status,
        "counters": job.counters or {},
        "error": job.error,
    }

    for ws in list(connections[job_id]):
        try:
            await ws.send_json(payload)
        except Exception:
            try:
                connections[job_id].remove(ws)
            except ValueError:
                pass


@app.post("/jobs", response_model=JobResponse)
async def create_job(
    job_request: JobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    SOW 2.2.A - POST /jobs
    Create new crawl job with basic validation and duplicate detection.
    """
    # ---- URL validation & normalization ----
    parsed = urlparse(job_request.url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(
            status_code=400,
            detail="Invalid URL. Use http(s)://domain/path",
        )

    # Add SSRF protection
    if not is_safe_url(job_request.url):
        raise HTTPException(
            status_code=400,
            detail="Invalid URL: Cannot crawl internal/private network addresses",
        )

    # Normalize domain (strip www.) and rebuild URL
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    normalized_url = urlunparse(parsed._replace(netloc=netloc))

    # Update request URL to normalized form
    job_request.url = normalized_url
    options = job_request.options or {}

    # ---- Prevent duplicate identical jobs unless force_rerun ----
    force_rerun = bool(options.get("force_rerun", False))
    existing = (
        db.query(Job)
        .filter(Job.input_url == normalized_url)
        .order_by(Job.created_at.desc())
        .first()
    )
    if existing and not force_rerun:
        return {
            "job_id": existing.id,
            "status": existing.status,
            "counters": existing.counters or {},
        }

    job_id = str(uuid.uuid4())

    job = Job(
        id=job_id,
        input_url=normalized_url,
        domain=netloc,
        status="queued",
        options=options,
        counters={
            "pages_visited": 0,
            "products_discovered": 0,
            "products_extracted": 0,
            "images_downloaded": 0,
            "products_enriched": 0,
            "products_indexed": 0,
        },
    )

    db.add(job)
    db.commit()

    background_tasks.add_task(
        crawl_and_process, job_id, job_request.url, job_request.options
    )

    return {
        "job_id": job_id,
        "status": job.status,
        "counters": job.counters,
    }


@app.get("/jobs", response_model=List[JobListItem])
async def list_jobs(db: Session = Depends(get_db)):
    """
    Admin: list recent jobs with basic stats.
    """
    jobs = db.query(Job).order_by(Job.created_at.desc()).limit(50).all()
    return [
        JobListItem(
            id=j.id,
            input_url=j.input_url,
            status=j.status,
            created_at=j.created_at,
            finished_at=j.finished_at,
            counters=j.counters or {},
        )
        for j in jobs
    ]


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /jobs/{jobId}
    Get job status and counters
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job.id,
        "status": job.status,
        "counters": job.counters,
        "error": job.error,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
    }


@app.get("/jobs/{job_id}/logs")
async def get_logs(job_id: str, limit: int = 100, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /jobs/{jobId}/logs
    Return recent log entries for a job.
    """
    logs = (
        db.query(JobLog)
        .filter(JobLog.job_id == job_id)
        .order_by(JobLog.timestamp.desc())
        .limit(limit)
        .all()
    )
    # Return logs in chronological order
    return {
        "logs": [
            {
                "timestamp": log.timestamp.isoformat() if log.timestamp else None,
                "level": log.level,
                "message": log.message,
            }
            for log in reversed(logs)
        ]
    }


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - POST /jobs/{jobId}/cancel
    Cancel running job (best-effort)
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if job:
        job.status = "cancelled"
        db.commit()
    return {"status": "cancelled"}


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
    """
    SOW 2.2.A & 2.6 - GET /search
    Semantic search using vector embeddings with filter support.
    Falls back to keyword scoring if vectors are unavailable.
    """
    base_query = db.query(Product).filter(Product.job_id == job_id)

    if category:
        base_query = base_query.filter(Product.category.ilike(f"%{category}%"))
    if min_price is not None:
        base_query = base_query.filter(Product.price >= min_price)
    if max_price is not None:
        base_query = base_query.filter(Product.price <= max_price)
    if availability:
        base_query = base_query.filter(Product.availability.ilike(f"%{availability}%"))

    products = base_query.all()
    if not products:
        return []

    product_ids = [p.id for p in products]

    # Try vector-based semantic search first
    vectors = (
        db.query(ProductVector)
        .filter(ProductVector.product_id.in_(product_ids))
        .all()
    )
    vector_map = {v.product_id: v.embedding for v in vectors if v.embedding}

    scored: List[tuple[float, Product, Optional[str]]] = []

    if vector_map:
        try:
            enricher = AIEnrichment()
            query_vec = await enricher.embed_text(q)

            if not query_vec:
                # Embedding failed, fall back to keyword search
                print(f"Warning: Embedding generation failed for query: {q}")
            else:
                def cosine(a: List[float], b: List[float]) -> float:
                    if not a or not b:
                        return 0.0
                    dot = sum(x * y for x, y in zip(a, b))
                    na = math.sqrt(sum(x * x for x in a))
                    nb = math.sqrt(sum(x * x for x in b))
                    if na == 0 or nb == 0:
                        return 0.0
                    return dot / (na * nb)

                for prod in products:
                    vec = vector_map.get(prod.id)
                    if not vec:
                        continue
                    sim = cosine(query_vec, vec)
                    if sim <= 0:
                        continue

                    enrichment = (
                        db.query(ProductEnrichment)
                        .filter(ProductEnrichment.product_id == prod.id)
                        .first()
                    )
                    reason = (enrichment.visual_summary if enrichment else "") or (
                        prod.description or ""
                    )
                    reason = (reason or "")[:200]
                    scored.append((sim, prod, reason))

                scored.sort(reverse=True, key=lambda x: x[0])
        except Exception as e:
            print(f"Error in vector search: {e}. Falling back to keyword search.")
            # scored will remain empty, triggering keyword fallback below

    # Fallback: simple keyword overlap if no vectors / scores
    if not scored:
        query_words = q.lower().split()
        for prod in products:
            enrichment = (
                db.query(ProductEnrichment)
                .filter(ProductEnrichment.product_id == prod.id)
                .first()
            )
            enrichment_text = enrichment.enriched_text if enrichment else ""
            search_text = (
                (prod.title or "")
                + " "
                + (prod.description or "")
                + " "
                + enrichment_text
            ).lower()

            score = sum(1 for word in query_words if word in search_text)
            if score > 0:
                reason = (enrichment.visual_summary if enrichment else "") or (
                    prod.description or ""
                )
                reason = (reason or "")[:200]
                scored.append((float(score), prod, reason))

        scored.sort(reverse=True, key=lambda x: x[0])

    results: List[ProductResponse] = []
    for score, prod, reason in scored[:limit]:
        results.append(
            ProductResponse(
                id=prod.id,
                title=prod.title or "Unknown",
                price=prod.price,
                images=[img.storage_url for img in prod.images],
                source_url=prod.source_url,
                description=prod.description,
                match_reason=reason,
            )
        )

    return results


@app.get("/products/{product_id}")
async def get_product(product_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /products/{productId}
    Get full product record with enrichment
    """
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    enrichment = (
        db.query(ProductEnrichment)
        .filter(ProductEnrichment.product_id == product_id)
        .first()
    )

    return {
        "id": product.id,
        "title": product.title,
        "description": product.description,
        "price": product.price,
        "images": [
            {"url": img.storage_url, "hash": img.hash} for img in product.images
        ],
        "enrichment": {
            "visual_summary": enrichment.visual_summary if enrichment else None,
            "attributes": enrichment.attributes if enrichment else None,
        },
        "source_url": product.source_url,
    }


async def download_and_store_image(
    session: aiohttp.ClientSession, img_url: str, product_id: str
) -> Dict:
    """
    Downloads an image, calculates its hash, extracts dimensions, and returns metadata.
    In production, upload to S3. For demo, we reuse the original URL.
    """
    try:
        async with session.get(
            img_url, timeout=aiohttp.ClientTimeout(total=15)
        ) as response:
            if response.status != 200:
                return None

            image_bytes = await response.read()
            img_hash = hashlib.md5(image_bytes).hexdigest()

            # Extract image dimensions
            try:
                img = Image.open(BytesIO(image_bytes))
                width, height = img.size
            except Exception:
                width, height = None, None

            storage_url = img_url  # TODO: replace with S3 URL in production

            return {
                "storage_url": storage_url,
                "hash": img_hash,
                "source_url": img_url,
                "width": width,
                "height": height,
            }
    except Exception as e:
        print(f"Error downloading image {img_url}: {e}")
        return None


async def crawl_and_process(job_id: str, url: str, options: Dict):
    """
    Main background task - crawl, extract, enrich, index.
    Runs in-process for demo (FastAPI BackgroundTasks).
    """
    db: Session = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return

        log_job_event(db, job_id, "INFO", f"Starting crawl for {url}")

        # PHASE 1: CRAWL
        job.status = "crawling"
        job.started_at = datetime.utcnow()
        db.commit()
        await broadcast_status(job_id, db)

        crawler = UniversalCrawler(
            max_pages=options.get("max_pages", 5),
            max_products=options.get("max_products", 50),
        )

        products_data = await crawler.crawl(url)

        # Update crawl counters (pages + discovered products)
        job.counters["pages_visited"] = len(crawler.visited_urls)
        job.counters["products_discovered"] = len(products_data)
        db.commit()
        await broadcast_status(job_id, db)

        log_job_event(
            db,
            job_id,
            "INFO",
            f"Discovered {len(products_data)} products from crawl",
        )

        # PHASE 2: PARSE & EXTRACT
        job.status = "parsing"
        job.counters["products_extracted"] = len(products_data)
        db.commit()
        await broadcast_status(job_id, db)

        # PHASE 3: DOWNLOAD IMAGES (demo-grade: we persist ProductImage records)
        download_images = options.get("download_images", True)
        if download_images:
            job.status = "downloading"
            db.commit()
            await broadcast_status(job_id, db)

        # PHASE 4: ENRICH WITH AI (also creates ProductImage + vectors)
        job.status = "enriching"
        db.commit()
        await broadcast_status(job_id, db)
        log_job_event(db, job_id, "INFO", "Starting AI enrichment phase")

        enricher = AIEnrichment()

        seen_urls = set()

        for prod_data in products_data:
            try:
                src_url = prod_data["source_url"]
                if src_url in seen_urls:
                    continue
                seen_urls.add(src_url)

                # Try to get existing product or create a new one
                product = (
                    db.query(Product)
                    .filter(Product.source_url == src_url)
                    .first()
                )

                if not product:
                    product = Product(
                        id=str(uuid.uuid4()),
                        job_id=job_id,
                        source_url=src_url,
                        title=prod_data.get("title", "Unknown"),
                        description=prod_data.get("description"),
                        price=prod_data.get("price"),
                        currency=prod_data.get("currency"),
                        availability=prod_data.get("availability"),
                        category=prod_data.get("category"),
                        raw_json=prod_data,
                    )
                    db.add(product)
                    try:
                        db.commit()
                    except IntegrityError:
                        db.rollback()
                        # Another worker created it concurrently; fetch again
                        product = (
                            db.query(Product)
                            .filter(Product.source_url == src_url)
                            .first()
                        )
                        if not product:
                            continue  # Skip if still not found

                # Download and store images
                if download_images:
                    async with aiohttp.ClientSession() as http_session:
                        for img_url in prod_data.get("images", [])[:3]:
                            image_meta = await download_and_store_image(
                                http_session, img_url, product.id
                            )
                            if image_meta:
                                img_obj = ProductImage(
                                    id=str(uuid.uuid4()),
                                    product_id=product.id,
                                    **image_meta,
                                )
                                db.add(img_obj)
                                job.counters["images_downloaded"] = (
                                    job.counters.get("images_downloaded", 0) + 1
                                )
                        db.commit()
                        await broadcast_status(job_id, db)

                enrichment_data = await enricher.enrich_product(prod_data)
                enrichment = ProductEnrichment(
                    id=str(uuid.uuid4()),
                    product_id=product.id,
                    visual_summary=enrichment_data.get("visual_summary"),
                    attributes=enrichment_data.get("attributes"),
                    per_image_json=enrichment_data.get("per_image"),
                    enriched_text=enrichment_data.get("enriched_text"),
                )
                db.add(enrichment)
                db.commit()

                # Create vector embedding for semantic search
                embedding = await enricher.embed_text(enrichment.enriched_text)
                if embedding:
                    vector = ProductVector(
                        id=str(uuid.uuid4()),
                        product_id=product.id,
                        embedding=embedding,
                    )
                    db.add(vector)

                job.counters["products_enriched"] += 1
                db.commit()
                await broadcast_status(job_id, db)
            except Exception as e:
                print(f"Error enriching product: {e}")
                log_job_event(
                    db,
                    job_id,
                    "ERROR",
                    f"Failed to enrich product from {src_url}: {e}",
                )
                db.rollback()

        # PHASE 5: INDEX (placeholder)
        job.status = "indexing"
        job.counters["products_indexed"] = job.counters.get("products_enriched", 0)
        db.commit()
        await broadcast_status(job_id, db)
        log_job_event(
            db,
            job_id,
            "INFO",
            f"Indexing completed for {job.counters['products_indexed']} products",
        )

        # COMPLETE
        job.status = "completed"
        job.finished_at = datetime.utcnow()
        db.commit()
        await broadcast_status(job_id, db)

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.finished_at = datetime.utcnow()
        db.commit()
        await broadcast_status(job_id, db)
        print(f"Job {job_id} failed: {e}")
        # Best-effort logging; ignore failures here
        try:
            log_job_event(db, job_id, "ERROR", f"Job failed: {e}")
        except Exception:
            pass
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
