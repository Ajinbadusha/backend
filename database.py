# backend/database.py
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    Float,
    JSON,
    DateTime,
    ForeignKey,
    LargeBinary,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

# Use DATABASE_URL from environment (Render) or a local default
# IMPORTANT: Render uses postgres:// but SQLAlchemy 1.4+ requires postgresql://
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://crawler:crawler_demo_pass@localhost:5432/ecommerce_db",
)

# Fix for Render: postgres:// -> postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# TABLE 1: Jobs
class Job(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True)  # UUID
    input_url = Column(String, nullable=False)
    domain = Column(String)
    status = Column(String, default="queued")  # queued, crawling, parsing, ...
    options = Column(JSON)  # {max_pages, max_products, ...}
    counters = Column(JSON, default={})
    error = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    pages = relationship("Page", back_populates="job")
    products = relationship("Product", back_populates="job")


# TABLE 2: Pages (crawled URLs)
class Page(Base):
    __tablename__ = "pages"

    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey("jobs.id"))
    url = Column(String, nullable=False)
    status = Column(String)  # success, failed, skipped
    fetched_at = Column(DateTime, default=datetime.utcnow)
    html_snapshot = Column(LargeBinary, nullable=True)  # optional HTML snapshot

    job = relationship("Job", back_populates="pages")
    products = relationship("Product", back_populates="page")


# TABLE 3: Products (extracted product data)
class Product(Base):
    __tablename__ = "products"

    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey("jobs.id"))
    page_id = Column(String, ForeignKey("pages.id"), nullable=True)
    source_url = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    price = Column(Float, nullable=True)
    currency = Column(String, nullable=True)
    availability = Column(String, nullable=True)
    category = Column(String, nullable=True)
    raw_json = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)

    job = relationship("Job", back_populates="products")
    page = relationship("Page", back_populates="products")
    images = relationship("ProductImage", back_populates="product")
    enrichment = relationship("ProductEnrichment", back_populates="product", uselist=False)
    vector = relationship("ProductVector", back_populates="product", uselist=False)


# TABLE 4: Product Images
class ProductImage(Base):
    __tablename__ = "product_images"

    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey("products.id"))
    source_url = Column(String)
    storage_url = Column(String)  # Local or S3 path
    hash = Column(String)  # MD5 for deduplication
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)

    product = relationship("Product", back_populates="images")


# TABLE 5: Product Enrichment
class ProductEnrichment(Base):
    __tablename__ = "product_enrichment"

    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey("products.id"))
    visual_summary = Column(String)  # AI-generated description
    attributes = Column(JSON)  # {category, colors, material, ...}
    per_image_json = Column(JSON)  # per-image captions & attributes
    enriched_text = Column(String)  # unified search document

    product = relationship("Product", back_populates="enrichment")


# TABLE 6: Product Vectors (embeddings stored as JSON, no pgvector)
class ProductVector(Base):
    __tablename__ = "product_vectors"

    id = Column(String, primary_key=True)
    product_id = Column(String, ForeignKey("products.id"), unique=True, index=True)
    # store the embedding as a JSON array of floats instead of VECTOR(1536)
    embedding = Column(JSON, nullable=True)

    product = relationship("Product", back_populates="vector")


# TABLE 7: Job Logs - basic log entries per job
class JobLog(Base):
    __tablename__ = "job_logs"

    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey("jobs.id"))
    timestamp = Column(DateTime, default=datetime.utcnow)
    level = Column(String)  # INFO, WARNING, ERROR
    message = Column(String)

    job = relationship("Job")


def init_db():
    """Create all tables."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Yield a database session (FastAPI dependency)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
