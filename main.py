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

    # 🔧 FIX 1: Safe category filter
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

    query_words = q.lower().split()
    for prod in products:
        text = f"{prod.title or ''} {prod.description or ''}".lower()
        score = sum(1 for w in query_words if w in text)
        if score > 0:
            scored.append((score, prod))

    # 🔧 FIX 2: fallback (never empty)
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