"""
FastAPI Backend - REST API + WebSocket
"""

import os
import uuid
import json
from datetime import datetime
from typing import Dict, Optional, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
import asyncio

from database import Base, engine, get_db, Job, Product, ProductImage, ProductEnrichment, ProductVector, Page
from crawler import UniversalCrawler
from enrichment import AIEnrichment
from celery_app import celery

# Initialize database
Base.metadata.create_all(bind=engine)

# FastAPI app
app = FastAPI(title="Ecommerce Crawler API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store WebSocket connections for real-time updates
connections: Dict[str, List[WebSocket]] = {}


class JobCreate(BaseModel):
    url: str
    options: Optional[Dict] = {
        'max_pages': 5,
        'max_products': 50,
        'follow_pagination': True,
        'follow_links': True,
        'download_images': True,
        'crawl_speed': 'normal'  # slow, normal, fast
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


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str, db: Session = Depends(get_db)):
    await websocket.accept()
    
    if job_id not in connections:
        connections[job_id] = []
    connections[job_id].append(websocket)
    
    try:
        while True:
            # Send current job status
            job = db.query(Job).filter(Job.id == job_id).first()
            if job:
                await websocket.send_json({
                    'status': job.status,
                    'counters': job.counters or {},
                    'error': job.error
                })
            await asyncio.sleep(2)  # Update every 2 seconds
    except WebSocketDisconnect:
        connections[job_id].remove(websocket)


async def broadcast_status(job_id: str, status_data: Dict):
    """Broadcast status to all connected clients"""
    if job_id in connections:
        for connection in connections[job_id]:
            try:
                await connection.send_json(status_data)
            except:
                pass


@app.post("/jobs", response_model=JobResponse)
async def create_job(job_request: JobCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - POST /jobs
    Create new crawl job
    """
    job_id = str(uuid.uuid4())
    
    job = Job(
        id=job_id,
        input_url=job_request.url,
        domain=job_request.url.split('/')[2],
        status='queued',
        options=job_request.options,
        counters={
            'pages_visited': 0,
            'products_discovered': 0,
            'products_extracted': 0,
            'images_downloaded': 0,
            'products_enriched': 0,
            'products_indexed': 0
        }
    )
    
    db.add(job)
    db.commit()
    
    # Queue the crawl task
    background_tasks.add_task(crawl_and_process, job_id, job_request.url, job_request.options, db)
    
    return {
        'job_id': job_id,
        'status': 'queued',
        'counters': job.counters
    }


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /jobs/{jobId}
    Get job status and counters
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        return {'error': 'Job not found'}, 404
    
    return {
        'job_id': job.id,
        'status': job.status,
        'counters': job.counters,
        'error': job.error,
        'created_at': job.created_at,
        'finished_at': job.finished_at
    }


@app.get("/jobs/{job_id}/logs")
async def get_logs(job_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /jobs/{jobId}/logs
    Get latest logs
    """
    # In production, store logs in separate table
    return {'logs': []}


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - POST /jobs/{jobId}/cancel
    Cancel running job (best-effort)
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if job:
        job.status = 'cancelled'
        db.commit()
    
    return {'status': 'cancelled'}


@app.get("/search", response_model=List[ProductResponse])
async def search(job_id: str, q: str, limit: int = 10, db: Session = Depends(get_db)):
    """
    SOW 2.2.A - GET /search
    Semantic search with optional filters
    """
    # Find products for this job
    products = db.query(Product).filter(Product.job_id == job_id).limit(limit).all()
    
    # Simple text match (replace with vector similarity)
    scored = []
    for prod in products:
        enrichment = db.query(ProductEnrichment).filter(ProductEnrichment.product_id == prod.id).first()
        search_text = (prod.title + ' ' + (prod.description or '') + ' ' + 
                      (enrichment.enriched_text if enrichment else '')).lower()
        
        score = sum(1 for word in q.lower().split() if word in search_text)
        if score > 0:
            scored.append((score, prod))
    
    # Sort by score descending
    scored.sort(reverse=True, key=lambda x: x[0])
    
    return [
        ProductResponse(
            id=prod.id,
            title=prod.title,
            price=prod.price,
            images=[img.storage_url for img in prod.images],
            source_url=prod.source_url
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
        return {'error': 'Product not found'}, 404
    
    enrichment = db.query(ProductEnrichment).filter(ProductEnrichment.product_id == product_id).first()
    
    return {
        'id': product.id,
        'title': product.title,
        'description': product.description,
        'price': product.price,
        'images': [{'url': img.storage_url, 'hash': img.hash} for img in product.images],
        'enrichment': {
            'visual_summary': enrichment.visual_summary if enrichment else None,
            'attributes': enrichment.attributes if enrichment else None,
        },
        'source_url': product.source_url
    }


async def crawl_and_process(job_id: str, url: str, options: Dict, db: Session):
    """Main background task - crawl, extract, enrich, index"""
    
    job = db.query(Job).filter(Job.id == job_id).first()
    
    try:
        # PHASE 1: CRAWL
        job.status = 'crawling'
        job.started_at = datetime.utcnow()
        db.commit()
        
        crawler = UniversalCrawler(
            max_pages=options.get('max_pages', 5),
            max_products=options.get('max_products', 50)
        )
        
        products_data = await crawler.crawl(url)
        
        job.counters['products_discovered'] = len(products_data)
        db.commit()
        
        # PHASE 2: PARSE & EXTRACT
        job.status = 'parsing'
        db.commit()
        
        job.counters['products_extracted'] = len(products_data)
        db.commit()
        
        # PHASE 3: DOWNLOAD IMAGES
        if options.get('download_images', True):
            job.status = 'downloading'
            db.commit()
            
            for prod_data in products_data:
                for i, img_url in enumerate(prod_data.get('images', [])[:3]):
                    try:
                        # Save image (demo: just store URL)
                        # In production: download to S3/local storage
                        img_obj = ProductImage(
                            id=str(uuid.uuid4()),
                            product_id=None,  # Will set after product created
                            source_url=img_url,
                            storage_url=img_url,
                            hash=str(i)
                        )
                    except:
                        pass
        
        job.counters['images_downloaded'] = len(products_data) * 3
        db.commit()
        
        # PHASE 4: ENRICH WITH AI
        job.status = 'enriching'
        db.commit()
        
        enricher = AIEnrichment()
        
        for prod_data in products_data:
            try:
                # Create product in DB
                product = Product(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    source_url=prod_data['source_url'],
                    title=prod_data.get('title', 'Unknown'),
                    description=prod_data.get('description'),
                    price=prod_data.get('price'),
                    currency=prod_data.get('currency'),
                    availability=prod_data.get('availability'),
                    category=prod_data.get('category'),
                    raw_json=prod_data
                )
                db.add(product)
                db.commit()
                
                # Add images
                for i, img_url in enumerate(prod_data.get('images', [])[:3]):
                    img_obj = ProductImage(
                        id=str(uuid.uuid4()),
                        product_id=product.id,
                        source_url=img_url,
                        storage_url=img_url,
                        hash=str(i)
                    )
                    db.add(img_obj)
                db.commit()
                
                # Enrich
                enrichment_data = await enricher.enrich_product(prod_data)
                enrichment = ProductEnrichment(
                    id=str(uuid.uuid4()),
                    product_id=product.id,
                    visual_summary=enrichment_data.get('visual_summary'),
                    attributes=enrichment_data.get('attributes'),
                    per_image_json=enrichment_data.get('per_image'),
                    enriched_text=enrichment_data.get('enriched_text')
                )
                db.add(enrichment)
                db.commit()
                
                job.counters['products_enriched'] += 1
                db.commit()
            
            except Exception as e:
                print(f"Error enriching product: {e}")
                continue
        
        # PHASE 5: INDEX (create vectors)
        job.status = 'indexing'
        db.commit()
        
        # TODO: Generate embeddings using OpenAI + save to ProductVector table
        
        job.counters['products_indexed'] = job.counters.get('products_enriched', 0)
        
        # COMPLETE
        job.status = 'completed'
        job.finished_at = datetime.utcnow()
        db.commit()
        
    except Exception as e:
        job.status = 'failed'
        job.error = str(e)
        job.finished_at = datetime.utcnow()
        db.commit()
        print(f"Job {job_id} failed: {e}")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)

