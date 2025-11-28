from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Form
from sqlmodel import select

from ..db import get_session
from ..models import Product, ProductQA
from ..services.embeddings import (
	embedding_to_json,
	generate_embedding,
	search_all_product_qas,
	search_product_qas,
)

router = APIRouter(prefix="/products", tags=["product_qa"])


@router.get("/qas/all")
def list_all_qas(is_active: Optional[bool] = Query(None), limit: int = Query(default=500, ge=1, le=5000)):
	"""List all Q&As across all products."""
	with get_session() as session:
		stmt = select(ProductQA)
		if is_active is not None:
			stmt = stmt.where(ProductQA.is_active == is_active)  # noqa: E712
		stmt = stmt.order_by(ProductQA.created_at.desc()).limit(limit)
		
		qas = session.exec(stmt).all()
		
		# Load product names for display
		product_ids = {qa.product_id for qa in qas}
		products = {}
		if product_ids:
			products_list = session.exec(select(Product).where(Product.id.in_(product_ids))).all()  # type: ignore
			products = {p.id: p for p in products_list}
		
		return {
			"qas": [
				{
					"id": qa.id,
					"product_id": qa.product_id,
					"product_name": products.get(qa.product_id).name if products.get(qa.product_id) else f"Product {qa.product_id}",
					"question": qa.question,
					"answer": qa.answer,
					"is_active": qa.is_active,
					"has_embedding": bool(qa.embedding_json),
					"created_at": qa.created_at.isoformat() if qa.created_at else None,
					"updated_at": qa.updated_at.isoformat() if qa.updated_at else None,
				}
				for qa in qas
			],
		}


@router.post("/qas/search/all")
def search_all_qas_endpoint(
	query: str = Form(...),
	limit: int = Form(10),
	min_similarity: float = Form(0.7),
):
	"""Search Q&As across all products using semantic similarity."""
	if not query or not query.strip():
		raise HTTPException(status_code=400, detail="query is required")
	
	results = search_all_product_qas(query, limit=limit, min_similarity=min_similarity)
	
	# Load product names for display
	product_ids = {qa.product_id for qa, _ in results}
	products = {}
	if product_ids:
		with get_session() as session:
			products_list = session.exec(select(Product).where(Product.id.in_(product_ids))).all()  # type: ignore
			products = {p.id: p for p in products_list}
	
	return {
		"query": query,
		"matches": [
			{
				"id": qa.id,
				"product_id": qa.product_id,
				"product_name": products.get(qa.product_id).name if products.get(qa.product_id) else f"Product {qa.product_id}",
				"question": qa.question,
				"answer": qa.answer,
				"similarity": round(similarity, 4),
			}
			for qa, similarity in results
		],
	}


@router.get("/qas/table")
def all_qas_table(request: Request, is_active: Optional[bool] = None):
	"""Render Q&A overview/management UI for all products."""
	with get_session() as session:
		stmt = select(ProductQA)
		if is_active is not None:
			stmt = stmt.where(ProductQA.is_active == is_active)  # noqa: E712
		stmt = stmt.order_by(ProductQA.created_at.desc()).limit(500)
		
		qas = session.exec(stmt).all()
		
		# Load product names
		product_ids = {qa.product_id for qa in qas}
		products = {}
		if product_ids:
			products_list = session.exec(select(Product).where(Product.id.in_(product_ids))).all()  # type: ignore
			products = {p.id: p for p in products_list}
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"product_qa_all.html",
			{"request": request, "qas": qas, "products": products},
		)


@router.get("/{product_id}/qas")
def list_product_qas(product_id: int, is_active: Optional[bool] = Query(None)):
	"""List all Q&As for a product."""
	with get_session() as session:
		# Verify product exists
		product = session.exec(select(Product).where(Product.id == product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
		
		# Build query
		stmt = select(ProductQA).where(ProductQA.product_id == product_id)
		if is_active is not None:
			stmt = stmt.where(ProductQA.is_active == is_active)  # noqa: E712
		stmt = stmt.order_by(ProductQA.created_at.desc())
		
		qas = session.exec(stmt).all()
		
		return {
			"product_id": product_id,
			"qas": [
				{
					"id": qa.id,
					"question": qa.question,
					"answer": qa.answer,
					"is_active": qa.is_active,
					"has_embedding": bool(qa.embedding_json),
					"created_at": qa.created_at.isoformat() if qa.created_at else None,
					"updated_at": qa.updated_at.isoformat() if qa.updated_at else None,
				}
				for qa in qas
			],
		}


@router.post("/{product_id}/qas")
def create_product_qa(
	product_id: int,
	question: Optional[str] = Form(None),
	questions: Optional[str] = Form(None),  # JSON array of questions
	answer: str = Form(...),
	is_active: bool = Form(True),
):
	"""Create one or more Q&As for a product. Automatically generates embeddings.
	
	If 'questions' (JSON array) is provided, creates multiple Q&A entries with the same answer.
	Otherwise, uses 'question' for backward compatibility (single Q&A).
	"""
	with get_session() as session:
		# Verify product exists
		product = session.exec(select(Product).where(Product.id == product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
		
		if not answer or not answer.strip():
			raise HTTPException(status_code=400, detail="answer is required")
		
		# Determine which questions to use
		question_list: List[str] = []
		if questions:
			# Parse JSON array of questions
			try:
				question_list = json.loads(questions)
				if not isinstance(question_list, list):
					raise HTTPException(status_code=400, detail="questions must be a JSON array")
				# Filter out empty questions
				question_list = [q.strip() for q in question_list if q and q.strip()]
			except json.JSONDecodeError:
				raise HTTPException(status_code=400, detail="Invalid JSON in questions field")
		elif question:
			# Single question for backward compatibility
			if not question.strip():
				raise HTTPException(status_code=400, detail="question is required")
			question_list = [question.strip()]
		else:
			raise HTTPException(status_code=400, detail="Either 'question' or 'questions' is required")
		
		if not question_list:
			raise HTTPException(status_code=400, detail="At least one non-empty question is required")
		
		# Create Q&A entries for each question
		created_qas = []
		answer_text = answer.strip()
		
		for q_text in question_list:
			# Generate embedding from question
			# We use question for embedding since that's what we'll match against
			embedding = generate_embedding(q_text)
			embedding_json = embedding_to_json(embedding)
			
			qa = ProductQA(
				product_id=product_id,
				question=q_text,
				answer=answer_text,
				embedding_json=embedding_json,
				is_active=is_active,
			)
			
			session.add(qa)
			session.flush()
			
			created_qas.append({
				"id": qa.id,
				"product_id": qa.product_id,
				"question": qa.question,
				"answer": qa.answer,
				"is_active": qa.is_active,
				"has_embedding": bool(qa.embedding_json),
			})
		
		# Return single Q&A for backward compatibility if only one was created
		if len(created_qas) == 1:
			return created_qas[0]
		
		# Return list of created Q&As
		return {
			"created": created_qas,
			"count": len(created_qas),
		}


@router.put("/qas/{qa_id}")
def update_product_qa(
	qa_id: int,
	question: Optional[str] = None,
	answer: Optional[str] = None,
	is_active: Optional[bool] = None,
):
	"""Update a Q&A. Regenerates embedding if question or answer changed."""
	with get_session() as session:
		qa = session.exec(select(ProductQA).where(ProductQA.id == qa_id)).first()
		if not qa:
			raise HTTPException(status_code=404, detail="Q&A not found")
		
		needs_embedding_update = False
		
		if question is not None:
			if not question.strip():
				raise HTTPException(status_code=400, detail="question cannot be empty")
			if qa.question != question.strip():
				qa.question = question.strip()
				needs_embedding_update = True
		
		if answer is not None:
			if not answer.strip():
				raise HTTPException(status_code=400, detail="answer cannot be empty")
			qa.answer = answer.strip()
			# Regenerate embedding if answer changed too (to keep them in sync)
			needs_embedding_update = True
		
		if is_active is not None:
			qa.is_active = is_active
		
		if needs_embedding_update:
			# Regenerate embedding from question
			embedding = generate_embedding(qa.question)
			qa.embedding_json = embedding_to_json(embedding)
		
		qa.updated_at = dt.datetime.utcnow()
		
		session.add(qa)
		session.flush()
		
		return {
			"id": qa.id,
			"product_id": qa.product_id,
			"question": qa.question,
			"answer": qa.answer,
			"is_active": qa.is_active,
			"has_embedding": bool(qa.embedding_json),
		}


@router.delete("/qas/{qa_id}")
def delete_product_qa(qa_id: int):
	"""Delete a Q&A."""
	with get_session() as session:
		qa = session.exec(select(ProductQA).where(ProductQA.id == qa_id)).first()
		if not qa:
			raise HTTPException(status_code=404, detail="Q&A not found")
		
		session.delete(qa)
		return {"status": "ok"}


@router.post("/qas/{qa_id}/regenerate-embedding")
def regenerate_qa_embedding(qa_id: int):
	"""Manually regenerate the embedding for a Q&A."""
	with get_session() as session:
		qa = session.exec(select(ProductQA).where(ProductQA.id == qa_id)).first()
		if not qa:
			raise HTTPException(status_code=404, detail="Q&A not found")
		
		embedding = generate_embedding(qa.question)
		qa.embedding_json = embedding_to_json(embedding)
		qa.updated_at = dt.datetime.utcnow()
		
		session.add(qa)
		session.flush()
		
		return {
			"id": qa.id,
			"has_embedding": bool(qa.embedding_json),
		}


@router.post("/{product_id}/qas/search")
def search_product_qas_endpoint(
	product_id: int,
	query: str = Form(...),
	limit: int = Form(5),
	min_similarity: float = Form(0.7),
):
	"""Search Q&As for a product using semantic similarity."""
	if not query or not query.strip():
		raise HTTPException(status_code=400, detail="query is required")
	
	with get_session() as session:
		# Verify product exists
		product = session.exec(select(Product).where(Product.id == product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
	
	results = search_product_qas(product_id, query, limit=limit, min_similarity=min_similarity)
	
	return {
		"product_id": product_id,
		"query": query,
		"matches": [
			{
				"id": qa.id,
				"question": qa.question,
				"answer": qa.answer,
				"similarity": round(similarity, 4),
			}
			for qa, similarity in results
		],
	}


@router.get("/{product_id}/qas/table")
def product_qa_table(request: Request, product_id: int):
	"""Render Q&A management UI for a product."""
	with get_session() as session:
		product = session.exec(select(Product).where(Product.id == product_id)).first()
		if not product:
			raise HTTPException(status_code=404, detail="Product not found")
		
		qas = session.exec(
			select(ProductQA)
			.where(ProductQA.product_id == product_id)
			.order_by(ProductQA.created_at.desc())
		).all()
		
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"product_qa.html",
			{"request": request, "product": product, "qas": qas},
		)

