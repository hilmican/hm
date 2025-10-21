from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import ItemMappingRule, ItemMappingOutput


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


