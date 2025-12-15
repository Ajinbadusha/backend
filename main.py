"""
FastAPI Backend - REST API + WebSocket
"""

import os
import uuid
from datetime import datetime
from typing import Dict, Optional, List

import asyncio
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
)
from crawler import UniversalCrawler
from enrichment import AIEnrichment
# from celery_app import celery  # not used yet

# Initialize database schema
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Ecommerce Crawler API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later if needed
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


class ProductResponse(BaseModel):
    id: str
    title: str
    price: Optional[float]
    images: List[str]
    source_url: str


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
            # Drop broken sockets silently
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
    Create new crawl job
    """
    job_id = str(uuid.uuid4())

    job = Job(
        id=job_id,
        input_url=job_request.url,
        domain=job_request.url.split("/")[2],
        status="queued",
        options=job_request.options,
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

    # Queue the crawl task (FastAPI background task for demo)
    background_tasks.add_task(
        crawl_and_process, job_id, job_request.url, job_request.options
    )

    return {
        "job_id": job_id,
        "status": job.status,
        "counters": job.counters,
    }


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
async def get_logs(job_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /jobs/{jobId}/logs
    Placeholder for log streaming.
    """
    return {"logs": []}


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
async def search(job_id: str, q: str, limit: int = 10, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /search
    Semantic search with simple text scoring (demo).
    """
    products = db.query(Product).filter(Product.job_id == job_id).all()

    scored: List[tuple[int, Product]] = []
    query_words = q.lower().split()

    for prod in products:
        enrichment = (
            db.query(ProductEnrichment)
            .filter(ProductEnrichment.product_id == prod.id)
            .first()
        )
        search_text = (
            (prod.title or "")
            + " "
            + (prod.description or "")
            + " "
            + (enrichment.enriched_text if enrichment else "")
        ).lower()

        score = sum(1 for word in query_words if word in search_text)
        if score > 0:
            scored.append((score, prod))

    scored.sort(reverse=True, key=lambda x: x[0])

    return [
        ProductResponse(
            id=prod.id,
            title=prod.title or "Unknown",
            price=prod.price,
            images=[img.storage_url for img in prod.images],
            source_url=prod.source_url,
        )
        for _, prod in scored[:limit]
    ]


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

        job.counters["products_discovered"] = len(products_data)
        db.commit()
        await broadcast_status(job_id, db)

        # PHASE 2: PARSE & EXTRACT
        job.status = "parsing"
        job.counters["products_extracted"] = len(products_data)
        db.commit()
        await broadcast_status(job_id, db)

        # PHASE 3: DOWNLOAD IMAGES (demo: just count)
        if options.get("download_images", True):
            job.status = "downloading"
            db.commit()
            await broadcast_status(job_id, db)

            # Demo: pretend 3 images per product
            job.counters["images_downloaded"] = len(products_data) * 3
            db.commit()
            await broadcast_status(job_id, db)

        # PHASE 4: ENRICH WITH AI
        job.status = "enriching"
        db.commit()
        await broadcast_status(job_id, db)

        enricher = AIEnrichment()

        for prod_data in products_data:
            try:
                product = Product(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    source_url=prod_data["source_url"],
                    title=prod_data.get("title", "Unknown"),
                    description=prod_data.get("description"),
                    price=prod_data.get("price"),
                    currency=prod_data.get("currency"),
                    availability=prod_data.get("availability"),
                    category=prod_data.get("category"),
                    raw_json=prod_data,
                )
                db.add(product)
                db.commit()

                # Add up to 3 images
                for i, img_url in enumerate(prod_data.get("images", [])[:3]):
                    img_obj = ProductImage(
                        id=str(uuid.uuid4()),
                        product_id=product.id,
                        source_url=img_url,
                        storage_url=img_url,
                        hash=str(i),
                    )
                    db.add(img_obj)
                db.commit()

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

                job.counters["products_enriched"] += 1
                db.commit()
                await broadcast_status(job_id, db)
            except Exception as e:
                print(f"Error enriching product: {e}")
                db.rollback()

        # PHASE 5: INDEX (placeholder)
        job.status = "indexing"
        job.counters["products_indexed"] = job.counters.get("products_enriched", 0)
        db.commit()
        await broadcast_status(job_id, db)

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
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
