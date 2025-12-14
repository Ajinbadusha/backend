from sqlalchemy import create_engine, Column, String, Integer, Float, JSON, DateTime, ForeignKey, LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from pgvector.sqlalchemy import Vector
from datetime import datetime
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://crawler:crawler_demo_pass@localhost:5432/ecommerce_db')
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Job(Base):
    __tablename__ = "jobs"
    
    id = Column(String, primary_key=True)  # UUID
    input_url = Column(String, nullable=False)
    domain = Column(String)
    status = Column(String, default="queued")  # queued, crawling, parsing, downloading, enriching, indexing, completed, failed
    options = Column(JSON)  # {max_pages, max_products, follow_pagination, follow_links, download_images, crawl_speed}
    counters = Column(JSON, default={})  # {pages_visited, products_discovered, products_extracted, images_downloaded, products_enriched, products_indexed}
    error = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    
    pages = relationship("Page", back_populates="job")
    products = relationship("Product", back_populates="job")


class Page(Base):
    __tablename__ = "pages"
    
    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey('jobs.id'))
    url = Column(String, nullable=False)
    status = Column(String)  # success, failed, skipped
    fetched_at = Column(DateTime, default=datetime.utcnow)
    html_snapshot = Column(LargeBinary, nullable=True)  # For debugging
    
    job = relationship("Job", back_populates="pages")
    products = relationship("Product", back_populates="page")


class Product(Base):
    __tablename__ = "products"
    
    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey('jobs.id'))
    page_id = Column(String, ForeignKey('pages.id'), nullable=True)
    source_url = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    price = Column(Float, nullable=True)
    currency = Column(String, nullable=True)
    availability = Column(String, nullable=True)
    category = Column(String, nullable=True)
    raw_json = Column(JSON)  # Full extracted data
    created_at = Column(DateTime, default=datetime.utcnow)
    
    job = relationship("Job", back_populates="products")
    page = relationship("Page", back_populates="products")
    images = relationship("ProductImage", back_populates="product")
    enrichment = relationship("ProductEnrichment", back_populates="product", uselist=False)
    vector = relationship("ProductVector", back_populates="product", uselist=False)


class ProductImage(Base):
    __tablename__ = "product_images"
    
    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey('products.id'))
    source_url = Column(String)
    storage_url = Column(String)  # Local or S3 path
    hash = Column(String)  # MD5 for deduplication
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    
    product = relationship("Product", back_populates="images")


class ProductEnrichment(Base):
    __tablename__ = "product_enrichment"
    
    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey('products.id'))
    visual_summary = Column(String)  # "Red cotton t-shirt with graphic print"
    attributes = Column(JSON)  # {category, colors, material, fit, pattern, etc}
    per_image_json = Column(JSON)  # {image_id: {caption, extracted_attrs}}
    enriched_text = Column(String)  # Unified search document
    
    product = relationship("Product", back_populates="enrichment")


class ProductVector(Base):
    __tablename__ = "product_vectors"
    
    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey('products.id'), unique=True)
    embedding = Column(Vector(1536))  # OpenAI ada-002 dimension (or 384 for smaller models)
    
    product = relationship("Product", back_populates="vector")


def init_db():
    """Create all tables"""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

