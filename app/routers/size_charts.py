from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from sqlmodel import select

from ..db import get_session
from ..models import SizeChart, SizeChartEntry, ProductSizeChart, Product
from ..schemas import (
	SizeChartCreate,
	SizeChartUpdate,
	SizeChartEntryCreate,
	SizeChartEntryUpdate,
	ProductSizeChartAssign,
	SizeChartGridUpsert,
)


router = APIRouter(prefix="/size-charts", tags=["size-charts"])


def _load_entries(session, chart_ids: List[int]) -> Dict[int, List[SizeChartEntry]]:
	if not chart_ids:
		return {}
	rows = session.exec(
		select(SizeChartEntry)
		.where(SizeChartEntry.size_chart_id.in_(chart_ids))
		.order_by(SizeChartEntry.id.asc())
	).all()
	result: Dict[int, List[SizeChartEntry]] = {}
	for entry in rows:
		result.setdefault(entry.size_chart_id, []).append(entry)
	return result


@router.get("")
@router.get("/")
def list_size_charts(include_entries: bool = Query(default=True)):
	with get_session() as session:
		charts = session.exec(select(SizeChart).order_by(SizeChart.id.desc())).all()
		entry_map = _load_entries(session, [c.id for c in charts if c.id] if include_entries else [])
		return {
			"size_charts": [
				{
					"id": c.id,
					"name": c.name,
					"description": c.description,
					"entries": [
						{
							"id": e.id,
							"size_label": e.size_label,
							"height_min": e.height_min,
							"height_max": e.height_max,
							"weight_min": e.weight_min,
							"weight_max": e.weight_max,
							"notes": e.notes,
						}
						for e in entry_map.get(c.id, [])
					]
					if include_entries
					else [],
				}
				for c in charts
			]
		}


@router.get("/table")
def size_charts_table(request: Request):
	with get_session() as session:
		charts = session.exec(select(SizeChart).order_by(SizeChart.name.asc())).all()
		entry_map = _load_entries(session, [c.id for c in charts if c.id])
		entry_map_json = {
			cid: jsonable_encoder(
				[
					{
						"id": e.id,
						"size_label": e.size_label,
						"height_min": e.height_min,
						"height_max": e.height_max,
						"weight_min": e.weight_min,
						"weight_max": e.weight_max,
						"notes": e.notes,
					}
					for e in rows
				]
			)
			for cid, rows in entry_map.items()
		}
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"size_charts.html",
		{
			"request": request,
			"charts": charts,
			"entries": entry_map_json,
		},
	)


@router.get("/assign")
def product_size_chart_table(request: Request):
	with get_session() as session:
		products = session.exec(select(Product).order_by(Product.name.asc())).all()
		charts = session.exec(select(SizeChart).order_by(SizeChart.name.asc())).all()
		assignments = session.exec(select(ProductSizeChart)).all()
	assignment_map = {a.product_id: a.size_chart_id for a in assignments if a.product_id and a.size_chart_id}
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"product_size_chart.html",
		{
			"request": request,
			"products": products,
			"charts": charts,
			"assignment_map": assignment_map,
		},
	)


@router.post("")
def create_size_chart(body: SizeChartCreate):
	if not body.name:
		raise HTTPException(status_code=400, detail="name required")
	with get_session() as session:
		existing = session.exec(select(SizeChart).where(SizeChart.name == body.name)).first()
		if existing:
			raise HTTPException(status_code=409, detail="Size chart already exists")
		c = SizeChart(name=body.name, description=body.description)
		session.add(c)
		session.flush()
		return {"id": c.id, "name": c.name, "description": c.description}


@router.put("/{chart_id}")
def update_size_chart(chart_id: int, body: SizeChartUpdate):
	with get_session() as session:
		c = session.exec(select(SizeChart).where(SizeChart.id == chart_id)).first()
		if not c:
			raise HTTPException(status_code=404, detail="Size chart not found")
		if body.name:
			dup = session.exec(select(SizeChart).where(SizeChart.name == body.name, SizeChart.id != chart_id)).first()
			if dup:
				raise HTTPException(status_code=409, detail="Another chart with this name exists")
			c.name = body.name
		if body.description is not None:
			c.description = body.description
		return {"status": "ok", "id": c.id}


@router.delete("/{chart_id}")
def delete_size_chart(chart_id: int):
	with get_session() as session:
		c = session.exec(select(SizeChart).where(SizeChart.id == chart_id)).first()
		if not c:
			raise HTTPException(status_code=404, detail="Size chart not found")
		# Clean up assignments and entries
		assignments = session.exec(
			select(ProductSizeChart).where(ProductSizeChart.size_chart_id == chart_id)
		).all()
		for a in assignments:
			session.delete(a)
		entries = session.exec(
			select(SizeChartEntry).where(SizeChartEntry.size_chart_id == chart_id)
		).all()
		for e in entries:
			session.delete(e)
		session.delete(c)
		return {"status": "ok"}


@router.get("/{chart_id}")
def get_size_chart(chart_id: int):
	with get_session() as session:
		c = session.exec(select(SizeChart).where(SizeChart.id == chart_id)).first()
		if not c:
			raise HTTPException(status_code=404, detail="Size chart not found")
		entries = session.exec(
			select(SizeChartEntry).where(SizeChartEntry.size_chart_id == chart_id).order_by(SizeChartEntry.id.asc())
		).all()
		return {
			"id": c.id,
			"name": c.name,
			"description": c.description,
			"entries": [
				{
					"id": e.id,
					"size_label": e.size_label,
					"height_min": e.height_min,
					"height_max": e.height_max,
					"weight_min": e.weight_min,
					"weight_max": e.weight_max,
					"notes": e.notes,
				}
				for e in entries
			],
		}


@router.post("/{chart_id}/entries")
def create_entry(chart_id: int, body: SizeChartEntryCreate):
	if not body.size_label:
		raise HTTPException(status_code=400, detail="size_label required")
	with get_session() as session:
		c = session.exec(select(SizeChart).where(SizeChart.id == chart_id)).first()
		if not c:
			raise HTTPException(status_code=404, detail="Size chart not found")
		entry = SizeChartEntry(
			size_chart_id=chart_id,
			size_label=body.size_label.strip(),
			height_min=body.height_min,
			height_max=body.height_max,
			weight_min=body.weight_min,
			weight_max=body.weight_max,
			notes=body.notes,
		)
		session.add(entry)
		session.flush()
		return {"id": entry.id, "size_chart_id": chart_id}


@router.put("/{chart_id}/entries/{entry_id}")
def update_entry(chart_id: int, entry_id: int, body: SizeChartEntryUpdate):
	with get_session() as session:
		entry = session.exec(
			select(SizeChartEntry).where(SizeChartEntry.id == entry_id, SizeChartEntry.size_chart_id == chart_id)
		).first()
		if not entry:
			raise HTTPException(status_code=404, detail="Entry not found")
		if body.size_label is not None:
			entry.size_label = body.size_label.strip()
		if body.height_min is not None:
			entry.height_min = body.height_min
		if body.height_max is not None:
			entry.height_max = body.height_max
		if body.weight_min is not None:
			entry.weight_min = body.weight_min
		if body.weight_max is not None:
			entry.weight_max = body.weight_max
		if body.notes is not None:
			entry.notes = body.notes
		return {"status": "ok", "id": entry.id}


@router.delete("/{chart_id}/entries/{entry_id}")
def delete_entry(chart_id: int, entry_id: int):
	with get_session() as session:
		entry = session.exec(
			select(SizeChartEntry).where(SizeChartEntry.id == entry_id, SizeChartEntry.size_chart_id == chart_id)
		).first()
		if not entry:
			raise HTTPException(status_code=404, detail="Entry not found")
		session.delete(entry)
		return {"status": "ok"}


@router.post("/{chart_id}/assign-product")
def assign_product(chart_id: int, body: ProductSizeChartAssign):
	if not body.product_id:
		raise HTTPException(status_code=400, detail="product_id required")
	if body.size_chart_id and body.size_chart_id != chart_id:
		raise HTTPException(status_code=400, detail="Path chart_id and payload size_chart_id must match")
	with get_session() as session:
		chart = session.exec(select(SizeChart).where(SizeChart.id == chart_id)).first()
		if not chart:
			raise HTTPException(status_code=404, detail="Size chart not found")
		product = session.exec(select(Product).where(Product.id == body.product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
		# enforce one-to-one assignment per product (replace existing)
		existing = session.exec(
			select(ProductSizeChart).where(ProductSizeChart.product_id == body.product_id)
		).first()
		if existing:
			existing.size_chart_id = chart_id
		else:
			link = ProductSizeChart(product_id=body.product_id, size_chart_id=chart_id)
			session.add(link)
		return {"status": "ok", "product_id": body.product_id, "size_chart_id": chart_id}


@router.post("/{chart_id}/grid")
def upsert_grid(chart_id: int, body: SizeChartGridUpsert):
	with get_session() as session:
		chart = session.exec(select(SizeChart).where(SizeChart.id == chart_id)).first()
		if not chart:
			raise HTTPException(status_code=404, detail="Size chart not found")
		if not body.height_bands or not body.weight_bands:
			raise HTTPException(status_code=400, detail="height_bands and weight_bands required")
		if not body.grid:
			raise HTTPException(status_code=400, detail="grid required")
		try:
			hb = sorted(set(int(x) for x in body.height_bands if x is not None))
			wb = sorted(set(int(x) for x in body.weight_bands if x is not None))
		except Exception:
			raise HTTPException(status_code=400, detail="height_bands/weight_bands must be integers")
		if len(body.grid) != len(wb):
			raise HTTPException(status_code=400, detail="grid row count must match weight_bands length")
		col_count = len(hb)
		for row in body.grid:
			if len(row) != col_count:
				raise HTTPException(status_code=400, detail="each grid row must match height_bands length")

		def _ranges(bands: list[int]) -> list[tuple[int, int | None]]:
			ranges = []
			for idx, start in enumerate(bands):
				end = bands[idx + 1] - 1 if idx + 1 < len(bands) else None
				ranges.append((start, end))
			return ranges

		h_ranges = _ranges(hb)
		w_ranges = _ranges(wb)

		# Build new entries; only proceed if there is at least one non-empty cell
		# Use a set to avoid duplicates when same cell value is repeated
		new_entries: list[SizeChartEntry] = []
		seen_keys = set()
		for w_idx, w_range in enumerate(w_ranges):
			for h_idx, h_range in enumerate(h_ranges):
				val_raw = body.grid[w_idx][h_idx]
				if val_raw is None:
					continue
				val = str(val_raw).strip()
				if not val:
					continue
				key = (
					chart_id,
					val,
					h_range[0],
					h_range[1],
					w_range[0],
					w_range[1],
				)
				if key in seen_keys:
					continue
				seen_keys.add(key)
				new_entries.append(
					SizeChartEntry(
						size_chart_id=chart_id,
						size_label=val,
						height_min=h_range[0],
						height_max=h_range[1],
						weight_min=w_range[0],
						weight_max=w_range[1],
					)
				)
		if not new_entries:
			raise HTTPException(status_code=400, detail="Grid is empty; provide at least one size cell")

		# clear existing entries then insert new ones
		existing_entries = session.exec(
			select(SizeChartEntry).where(SizeChartEntry.size_chart_id == chart_id)
		).all()
		for e in existing_entries:
			session.delete(e)
		for entry in new_entries:
			session.add(entry)

		count = len(new_entries)

		return {"status": "ok", "inserted": count}

