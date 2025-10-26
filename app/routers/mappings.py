from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import ItemMappingRule, ItemMappingOutput, Product, Item
from ..schemas import AISuggestRequest, AISuggestResponse, AIApplyRequest
from ..utils.slugify import slugify
from ..db import get_session
from sqlmodel import select



router = APIRouter(prefix="/mappings", tags=["mappings"])


@router.get("/rules")
def list_rules(limit: int = Query(default=500, ge=1, le=5000), pattern: str | None = Query(default=None)):
	with get_session() as session:
		q = select(ItemMappingRule).order_by(ItemMappingRule.priority.desc(), ItemMappingRule.id.desc())
		if pattern:
			q = q.where(ItemMappingRule.source_pattern == pattern)
		rules = session.exec(q.limit(limit)).all()
		result = []
		for r in rules:
			outs = session.exec(select(ItemMappingOutput).where(ItemMappingOutput.rule_id == r.id)).all()
			result.append({
				"id": r.id,
				"source_pattern": r.source_pattern,
				"match_mode": r.match_mode,
				"priority": r.priority,
				"is_active": r.is_active,
				"outputs": [
					{
						"id": o.id,
						"item_id": o.item_id,
						"product_id": o.product_id,
						"size": o.size,
						"color": o.color,
                        # pack/pair removed
						"quantity": o.quantity,
						"unit_price": o.unit_price,
					}
					for o in outs
				],
			})
		return {"rules": result}


@router.get("/table")
def rules_table(request: Request, limit: int = Query(default=1000, ge=1, le=5000), pattern: str | None = Query(default=None)):
	with get_session() as session:
		q = select(ItemMappingRule).order_by(ItemMappingRule.priority.desc(), ItemMappingRule.id.desc())
		if pattern:
			q = q.where(ItemMappingRule.source_pattern == pattern)
		rules = session.exec(q.limit(limit)).all()
		result = []
		for r in rules:
			outs = session.exec(select(ItemMappingOutput).where(ItemMappingOutput.rule_id == r.id)).all()
			result.append((r, outs))
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"mappings_table.html",
			{"request": request, "rows": result},
		)


@router.post("/rules")
def create_rule(body: Dict[str, Any]):
	pattern = body.get("source_pattern")
	mode = body.get("match_mode") or "exact"
	priority = int(body.get("priority") or 0)
	outputs: List[Dict[str, Any]] = body.get("outputs") or []
	if not pattern:
		raise HTTPException(status_code=400, detail="source_pattern required")
	with get_session() as session:
		r = ItemMappingRule(source_pattern=str(pattern), match_mode=str(mode), priority=priority, is_active=True)
		session.add(r)
		session.flush()
		for out in outputs:
			o_rec = ItemMappingOutput(
				rule_id=r.id or 0,
				item_id=out.get("item_id"),
				product_id=out.get("product_id"),
				size=out.get("size"),
				color=out.get("color"),
				quantity=out.get("quantity") or 1,
				unit_price=out.get("unit_price"),
			)
			session.add(o_rec)
		return {"id": r.id}


@router.put("/rules/{rule_id}")
def update_rule(rule_id: int, body: Dict[str, Any]):
	with get_session() as session:
		r = session.exec(select(ItemMappingRule).where(ItemMappingRule.id == rule_id)).first()
		if not r:
			raise HTTPException(status_code=404, detail="Rule not found")
		for f in ("source_pattern", "match_mode", "priority", "is_active", "notes"):
			if f in body:
				setattr(r, f, body.get(f))
		if "outputs" in body:
			# simplistic replace-all for outputs
			outs = session.exec(select(ItemMappingOutput).where(ItemMappingOutput.rule_id == r.id)).all()
			for o in outs:
				session.delete(o)
			session.flush()
			for out in (body.get("outputs") or []):
				o_rec = ItemMappingOutput(
					rule_id=r.id or 0,
					item_id=out.get("item_id"),
					product_id=out.get("product_id"),
					size=out.get("size"),
					color=out.get("color"),
					quantity=out.get("quantity") or 1,
					unit_price=out.get("unit_price"),
				)
				session.add(o_rec)
		return {"status": "ok"}


@router.patch("/rules/{rule_id}")
def patch_rule(rule_id: int, body: Dict[str, Any]):
    with get_session() as session:
        r = session.exec(select(ItemMappingRule).where(ItemMappingRule.id == rule_id)).first()
        if not r:
            raise HTTPException(status_code=404, detail="Rule not found")
        for f in ("source_pattern", "match_mode", "priority", "is_active", "notes"):
            if f in body:
                setattr(r, f, body.get(f))
        return {"status": "ok"}


@router.put("/rules/{rule_id}/outputs")
def replace_outputs(rule_id: int, outputs: List[Dict[str, Any]]):
    with get_session() as session:
        r = session.exec(select(ItemMappingRule).where(ItemMappingRule.id == rule_id)).first()
        if not r:
            raise HTTPException(status_code=404, detail="Rule not found")
        outs = session.exec(select(ItemMappingOutput).where(ItemMappingOutput.rule_id == rule_id)).all()
        for o in outs:
            session.delete(o)
        session.flush()
        for out in (outputs or []):
            session.add(ItemMappingOutput(
                rule_id=rule_id,
                item_id=out.get("item_id"),
                product_id=out.get("product_id"),
                size=out.get("size"),
                color=out.get("color"),
                quantity=out.get("quantity") or 1,
                unit_price=out.get("unit_price"),
            ))
        return {"status": "ok"}


# --- AI assisted mapping endpoints ---

@router.post("/ai/suggest", response_model=AISuggestResponse)
def ai_suggest(req: AISuggestRequest, request: Request) -> AISuggestResponse:
    ai = getattr(request.app.state, "ai", None)
    if not ai or not getattr(ai, "enabled", False):
        raise HTTPException(status_code=503, detail="AI not configured")

    # Build concise prompt with constraints and examples (Turkish context)
    system = (
        "Sen bir stok ve sipariş eşleştirme yardımcısısın. "
        "Girdi: Eşleşmeyen ürün patternleri. Çıktı: JSON olarak ürün önerileri ve eşleme kuralları. "
        "match_mode: exact|icontains|regex. Birim 'adet'. Sadece geçerli JSON döndür."
    )
    # Provide compact input as JSON string to the model
    import json as _json
    body = {
        "unmatched_patterns": [
            {
                "pattern": p.pattern,
                "count": p.count,
                "samples": p.samples,
                "suggested_price": p.suggested_price,
            }
            for p in req.unmatched_patterns
        ],
        "known_products": (req.context or {}).get("products") if req.context else None,
        "schema": {
            "products_to_create": [
                {"name": "str", "default_unit": "adet", "default_price": "float|null"}
            ],
            "mappings_to_create": [
                {
                    "source_pattern": "str",
                    "match_mode": "exact|icontains|regex",
                    "priority": "int",
                    "outputs": [
                        {"product_name": "str?", "item_sku": "str?", "size": "str?", "color": "str?", "quantity": "int", "unit_price": "float|null"}
                    ],
                }
            ],
            "notes": "str?",
            "warnings": ["str"],
        },
    }
    user = "Lütfen sadece JSON döndür. Girdi:" + "\n" + _json.dumps(body, ensure_ascii=False)

    raw = ai.generate_json(system_prompt=system, user_prompt=user)

    # Validate via Pydantic and normalize
    try:
        validated = AISuggestResponse(**raw)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid AI response: {e}")

    # Deduplicate products_to_create by slug against existing products
    with get_session() as session:
        existing = session.exec(select(Product).limit(10000)).all()
        existing_slugs = {p.slug for p in existing}
        uniq: list[dict] = []
        seen: set[str] = set()
        for p in validated.products_to_create:
            slug = slugify(p.name)
            if slug in existing_slugs or slug in seen:
                continue
            seen.add(slug)
            uniq.append({"name": p.name, "default_unit": p.default_unit, "default_price": p.default_price})
        validated.products_to_create = [
            ProductCreateSuggestion(**u)  # type: ignore[name-defined]
            for u in uniq
        ]

    return validated


@router.post("/ai/apply")
def ai_apply(body: AIApplyRequest):
    sug = body.suggestions
    created_products: dict[str, int] = {}

    with get_session() as session:
        # Create products if requested
        if body.create_products:
            for p in (sug.products_to_create or []):
                name = p.name
                slug = slugify(name)
                existing = session.exec(select(Product).where(Product.slug == slug)).first()
                if existing:
                    created_products[slug] = existing.id or 0
                    continue
                rec = Product(name=name, slug=slug, default_unit=p.default_unit or "adet", default_price=p.default_price)
                session.add(rec)
                session.flush()
                if rec.id:
                    created_products[slug] = rec.id

        # Create mapping rules if requested
        created_rules: list[int] = []
        if body.create_rules:
            for r in (sug.mappings_to_create or []):
                rule = ItemMappingRule(
                    source_pattern=r.source_pattern,
                    match_mode=r.match_mode,
                    priority=r.priority,
                    is_active=True,
                )
                session.add(rule)
                session.flush()
                # outputs
                for out in r.outputs:
                    item_id = None
                    product_id = None
                    if out.item_sku:
                        it = session.exec(select(Item).where(Item.sku == out.item_sku)).first()
                        item_id = it.id if it else None
                    if not item_id and out.product_name:
                        pslug = slugify(out.product_name)
                        prod = session.exec(select(Product).where(Product.slug == pslug)).first()
                        if not prod and pslug in created_products:
                            # resolve newly created product
                            pid = created_products[pslug]
                            prod = session.get(Product, pid)
                        product_id = prod.id if prod else None
                    session.add(ItemMappingOutput(
                        rule_id=rule.id or 0,
                        item_id=item_id,
                        product_id=product_id,
                        size=out.size,
                        color=out.color,
                        quantity=out.quantity or 1,
                        unit_price=out.unit_price,
                    ))
                created_rules.append(rule.id or 0)

        return {"created_products": created_products, "created_rules": created_rules}

