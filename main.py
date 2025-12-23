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
import requests
import redis.asyncio as redis  # async Redis client

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    Depends,
    BackgroundTasks,
    HTTPException,
    Header,
    Response,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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

# ---------- Redis ----------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)  # connection pool [web:209]

# Initialize database schema
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Ecommerce Crawler API", version="1.0.0")

# Serve static files (for invoice images etc.)
app.mount("/static", StaticFiles(directory="."), name="static")

# --- Security ---

API_KEY = os.getenv("OPENAI_API_KEY")


def get_api_key(api_key: str = Header(None, alias="X-API-Key")):
    if not API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Server API key not configured",
        )
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return api_key


@app.get("/")
@app.head("/")
async def root():
    return {
        "message": "Ecommerce Crawler API is running. Access /docs for API documentation."
    }


@app.get("/health")
async def health_check():
    # simple Redis ping (best-effort)
    ok = True
    try:
        await redis_client.set("health-check", "ok", ex=60)
    except Exception:
        ok = False

    return {
        "status": "healthy" if ok else "degraded",
        "service": "ecommerce-crawler-api",
        "version": "1.0.0",
        "redis": ok,
    }


@app.get("/debug-routes")
async def debug_routes():
    return [{"path": r.path, "methods": r.methods, "name": r.name} for r in app.routes]


FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:5173",
        "http://localhost:3000",
        "https://frontend-rymq.onrender.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connections
connections: Dict[str, List[WebSocket]] = {}


class JobCreate(BaseModel):
    url: str
    options: Optional[Dict] = {
        "max_pages": 5,
        "max_products": 50,
        "follow_pagination": True,
        "follow_links": True,
        "download_images": True,
        "crawl_speed": "normal",
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
    log_entry = JobLog(
        id=str(uuid.uuid4()),
        job_id=job_id,
        level=level,
        message=message,
    )
    db.add(log_entry)
    db.commit()


def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return False

        if hostname in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}:
            return False

        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False
        except ValueError:
            if hostname.startswith("192.168.") or hostname.startswith("10.") or hostname.startswith("172."):
                return False

        return True
    except Exception:
        return False


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
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
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        if job_id in connections and websocket in connections[job_id]:
            connections[job_id].remove(websocket)


async def broadcast_status(job_id: str, db: Session):
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
    parsed = urlparse(job_request.url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(
            status_code=400,
            detail="Invalid URL. Use http(s)://domain/path",
        )

    if not is_safe_url(job_request.url):
        raise HTTPException(
            status_code=400,
            detail="Invalid URL: Cannot crawl internal/private network addresses",
        )

    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    normalized_url = urlunparse(parsed._replace(netloc=netloc))

    job_request.url = normalized_url
    options = job_request.options or {}

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
    logs = (
        db.query(JobLog)
        .filter(JobLog.job_id == job_id)
        .order_by(JobLog.timestamp.desc())
        .limit(limit)
        .all()
    )
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
    job = db.query(Job).filter(Job.id == job_id).first()
    if job:
        job.status = "cancelled"
        db.commit()
    return {"status": "cancelled"}


# ---- Delete jobs and results ----

@app.delete("/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_job(job_id: str, db: Session = Depends(get_db)):
    # Delete child records first
    db.query(ProductVector).filter(
        ProductVector.product_id.in_(
            db.query(Product.id).filter(Product.job_id == job_id)
        )
    ).delete(synchronize_session=False)

    db.query(ProductEnrichment).filter(
        ProductEnrichment.product_id.in_(
            db.query(Product.id).filter(Product.job_id == job_id)
        )
    ).delete(synchronize_session=False)

    db.query(ProductImage).filter(
        ProductImage.product_id.in_(
            db.query(Product.id).filter(Product.job_id == job_id)
        )
    ).delete(synchronize_session=False)

    db.query(Page).filter(Page.job_id == job_id).delete(synchronize_session=False)
    db.query(Product).filter(Product.job_id == job_id).delete(synchronize_session=False)
    db.query(JobLog).filter(JobLog.job_id == job_id).delete(synchronize_session=False)

    deleted = db.query(Job).filter(Job.id == job_id).delete(synchronize_session=False)

    if not deleted:
        db.rollback()
        raise HTTPException(status_code=404, detail="Job not found")

    db.commit()
    return


@app.delete("/jobs", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_jobs(db: Session = Depends(get_db)):
    db.query(ProductVector).delete(synchronize_session=False)
    db.query(ProductEnrichment).delete(synchronize_session=False)
    db.query(ProductImage).delete(synchronize_session=False)
    db.query(Page).delete(synchronize_session=False)
    db.query(Product).delete(synchronize_session=False)
    db.query(JobLog).delete(synchronize_session=False)
    db.query(Job).delete(synchronize_session=False)
    db.commit()
    return


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

    if not scored:
        query_words = [w for w in q.lower().split() if w.strip()]
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
            if query_words:
                score = sum(1 for word in query_words if word in search_text)
                if score > 0:
                    reason = (enrichment.visual_summary if enrichment else "") or (
                        prod.description or ""
                    )
                    reason = (reason or "")[:200]
                    scored.append((float(score), prod, reason))
            else:
                reason = (enrichment.visual_summary if enrichment else "") or (
                    prod.description or ""
                )
                reason = (reason or "")[:200]
                scored.append((1.0, prod, reason))

        if not scored:
            for prod in products:
                enrichment = (
                    db.query(ProductEnrichment)
                    .filter(ProductEnrichment.product_id == prod.id)
                    .first()
                )
                reason = (enrichment.visual_summary if enrichment else "") or (
                    prod.description or ""
                )
                reason = (reason or "")[:200]
                scored.append((1.0, prod, reason))

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


@app.get("/search/download", dependencies=[Depends(get_api_key)])
async def download_search_results(
    job_id: str,
    q: str = "",
    category: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    availability: Optional[str] = None,
    db: Session = Depends(get_db),
):
    search_results = await search(
        job_id=job_id,
        q=q,
        limit=10000,
        category=category,
        min_price=min_price,
        max_price=max_price,
        availability=availability,
        db=db,
    )

    if not search_results:
        raise HTTPException(
            status_code=404, detail="No products found matching the criteria."
        )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "Title",
            "Price",
            "Source URL",
            "Description",
            "Match Reason (Semantic/Keyword)",
            "Image URL (First)",
        ]
    )

    for result in search_results:
        writer.writerow(
            [
                result.title,
                result.price,
                result.source_url,
                result.description.replace("\n", " ") if result.description else "",
                result.match_reason,
                result.images[0] if result.images else "",
            ]
        )

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=search_results_{job_id}.csv"
        },
    )


@app.get("/jobs/{job_id}/categories", response_model=List[str])
async def get_job_categories(job_id: str, db: Session = Depends(get_db)):
    categories = (
        db.query(Product.category)
        .filter(Product.job_id == job_id)
        .filter(Product.category.isnot(None))
        .distinct()
        .all()
    )
    return [c[0] for c in categories]


@app.get("/products/{product_id}")
async def get_product(product_id: str, db: Session = Depends(get_db)):
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
) -> Optional[Dict]:
    try:
        async with session.get(
            img_url, timeout=aiohttp.ClientTimeout(total=15)
        ) as response:
            if response.status != 200:
                return None

            image_bytes = await response.read()
            img_hash = hashlib.md5(image_bytes).hexdigest()

            try:
                img = Image.open(BytesIO(image_bytes))
                width, height = img.size
            except Exception:
                width, height = None, None

            backend_dir = os.path.dirname(__file__)
            download_dir = os.path.join(backend_dir, "download")
            os.makedirs(download_dir, exist_ok=True)

            ext = ".jpg"
            for candidate in [".jpg", ".jpeg", ".png", ".webp"]:
                if img_url.lower().endswith(candidate):
                    ext = candidate
                    break

            filename = f"{product_id}_{img_hash}{ext}"
            file_path = os.path.join(download_dir, filename)

            try:
                with open(file_path, "wb") as f:
                    f.write(image_bytes)
            except Exception as e:
                print(f"Error saving image {img_url} to disk: {e}")
                file_path = None

            storage_url = img_url

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


INVOICE_DIR = "invoice_images"
os.makedirs(INVOICE_DIR, exist_ok=True)


@app.post("/products/{product_id}/invoice-image")
def generate_invoice_image(product_id: str, db: Session = Depends(get_db)):
    """
    Generate an invoice-style composite image for a product.
    Downloads the first product image from its remote URL.
    """
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if not product.images:
        raise HTTPException(
            status_code=400, detail="No images available for this product"
        )

    img_meta = product.images[0]
    img_url = img_meta.storage_url or img_meta.source_url
    if not img_url:
        raise HTTPException(status_code=400, detail="Invalid image URL")

    # download remote image bytes
    try:
        resp = requests.get(img_url, timeout=15)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to fetch product image")
        image_bytes = resp.content
        base_image = Image.open(BytesIO(image_bytes)).convert("RGB")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open image: {e}")

    width, height = base_image.size
    extra_h = 260
    canvas = Image.new("RGB", (width, height + extra_h), (248, 250, 252))
    canvas.paste(base_image, (0, 0))

    draw = ImageDraw.Draw(canvas)
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


async def crawl_and_process(job_id: str, url: str, options: Dict):
    """
    Main background task - crawl, extract, enrich, index.
    """
    print(f"[crawl_and_process] starting job {job_id} for {url}")
    db: Session = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return

        log_job_event(db, job_id, "INFO", f"Starting crawl for {url}")

        job.status = "crawling"
        job.started_at = datetime.utcnow()
        db.commit()
        await broadcast_status(job_id, db)

        crawler = UniversalCrawler(
            max_pages=options.get("max_pages", 5),
            max_products=options.get("max_products", 50),
        )

        products_data = await crawler.crawl(url)

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

        job.status = "parsing"
        job.counters["products_extracted"] = len(products_data)
        db.commit()
        await broadcast_status(job_id, db)

        download_images = options.get("download_images", True)

        if download_images:
            job.status = "downloading"
            db.commit()
            await broadcast_status(job_id, db)

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
                        product = (
                            db.query(Product)
                            .filter(Product.source_url == src_url)
                            .first()
                        )

                if not product:
                    continue

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
        try:
            log_job_event(db, job_id, "ERROR", f"Job failed: {e}")
        except Exception:
            pass
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
