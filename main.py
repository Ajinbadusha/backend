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
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles  # NEW
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from PIL import Image, ImageDraw, ImageFont  # extended

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

# Serve static files for generated invoice images
app.mount("/static", StaticFiles(directory="."), name="static")

# --- Security Dependencies ---

# Read API key from environment; NEVER hard‑code secrets
API_KEY = os.getenv("OPENAI_API_KEY")


def get_api_key(api_key: str = Header(None, alias="X-API-Key")):
    if not API_KEY:
        # Optional: relax this in dev if you want
        raise HTTPException(
            status_code=500,
            detail="Server API key not configured",
        )

    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return api_key


# Root endpoint for deployment health check
@app.get("/")
@app.head("/")  # Explicitly allow HEAD requests for health checks
async def root():
    return {
        "message": "Ecommerce Crawler API is running. Access /docs for API documentation."
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "service": "ecommerce-crawler-api",
        "version": "1.0.0",
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
        "https://frontend-rymq.onrender.com",  # Production frontend URL
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
    Client connects to: /ws?job_id=
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
        base_query = base_query.filter(Product.category == category)
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
            scored = []

    # Fallback: simple keyword match if no vectors/scores
    if not scored:
        query_lower = q.lower()
        for prod in products:
            haystack = " ".join(
                [
                    prod.title or "",
                    prod.description or "",
                    prod.category or "",
                ]
            ).lower()
            if query_lower in haystack:
                reason = (prod.description or "")[:200]
                scored.append((1.0, prod, reason))

    scored = scored[:limit]

    results: List[ProductResponse] = []
    for _, prod, reason in scored:
        images = [img.storage_url or img.source_url for img in prod.images]
        results.append(
            ProductResponse(
                id=prod.id,
                title=prod.title or "Untitled",
                price=prod.price,
                images=images,
                source_url=prod.source_url,
                description=prod.description,
                match_reason=reason,
            )
        )

    return results


@app.get("/jobs/{job_id}/categories")
async def get_categories(job_id: str, db: Session = Depends(get_db)):
    """
    Return distinct non-null categories for a job.
    """
    cats = (
        db.query(Product.category)
        .filter(Product.job_id == job_id, Product.category.isnot(None))
        .distinct()
        .all()
    )
    return [c[0] for c in cats]

# ---------- NEW: invoice image generation ----------

INVOICE_DIR = "invoice_images"
os.makedirs(INVOICE_DIR, exist_ok=True)


@app.post("/products/{product_id}/invoice-image")
def generate_invoice_image(product_id: str, db: Session = Depends(get_db)):
    """
    Generate a composite 'invoice' image for a product (photo + details) and
    return a URL that the frontend can download directly.
    """
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Need at least one image
    if not product.images:
        raise HTTPException(
            status_code=400, detail="No images available for this product"
        )

    img_meta = product.images[0]
    img_path = img_meta.storage_url or img_meta.source_url
    if not img_path:
        raise HTTPException(status_code=400, detail="Invalid image path")

    # Open the product image (assumes local path; adjust if using remote URLs)
    try:
        base_image = Image.open(img_path).convert("RGB")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open image: {e}")

    width, height = base_image.size
    extra_h = 260
    canvas = Image.new("RGB", (width, height + extra_h), (248, 250, 252))
    canvas.paste(base_image, (0, 0))

    draw = ImageDraw.Draw(canvas)

    # Load fonts or fall back
    try:
        font_title = ImageFont.truetype("arial.ttf", 40)
        font_body = ImageFont.truetype("arial.ttf", 28)
    except Exception:
        font_title = ImageFont.load_default()
        font_body = ImageFont.load_default()

    x = 40
    y = height + 20

    title = product.title or "Product"
    price = product.price
    desc = product.description or ""
    category = product.category or ""
    availability = product.availability or ""

    draw.text((x, y), title, font=font_title, fill=(17, 24, 39))
    y += 60

    if price is not None:
        draw.text((x, y), f"Price: ₹{price}", font=font_body, fill=(37, 99, 235))
        y += 40

    if category:
        draw.text((x, y), f"Category: {category}", font=font_body, fill=(55, 65, 81))
        y += 34

    if availability:
        draw.text((x, y), f"Availability: {availability}", font=font_body, fill=(55, 65, 81))
        y += 34

    if desc:
        short = desc if len(desc) <= 220 else desc[:220] + "..."
        draw.text((x, y), f"Description: {short}", font=font_body, fill=(75, 85, 99))

    filename = f"{uuid.uuid4().hex}.png"
    out_path = os.path.join(INVOICE_DIR, filename)
    try:
        canvas.save(out_path, format="PNG", optimize=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save invoice image: {e}")

    invoice_url = f"/static/{INVOICE_DIR}/{filename}"
    return {"invoice_image_url": invoice_url}

# ---------- existing crawl_and_process and other logic below ----------

async def crawl_and_process(job_id: str, start_url: str, options: Dict):
    # ... keep your existing crawl/enrich/index implementation unchanged ...
    # (omitted here for brevity – copy from your current file)
    ...

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
