from __future__ import annotations

import json
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

from ..db import get_session
from ..models import ProductQA

log = logging.getLogger("embeddings")


def _get_openai_client() -> Optional[OpenAI]:
	"""Get OpenAI client if API key is configured."""
	api_key = os.getenv("OPENAI_API_KEY")
	if not api_key:
		return None
	return OpenAI(api_key=api_key)


def generate_embedding(text: str) -> Optional[List[float]]:
	"""
	Generate embedding for given text using OpenAI text-embedding-3-small model.
	
	Returns:
		List of floats (1536 dimensions) or None if API call fails.
	"""
	if not text or not text.strip():
		return None
	
	client = _get_openai_client()
	if not client:
		log.warning("OpenAI API key not configured, cannot generate embeddings")
		return None
	
	try:
		response = client.embeddings.create(
			model="text-embedding-3-small",
			input=text.strip(),
		)
		if response.data and len(response.data) > 0:
			return response.data[0].embedding
	except Exception as exc:
		log.error("Failed to generate embedding: %s", exc)
		return None
	return None


def embedding_to_json(embedding: Optional[List[float]]) -> Optional[str]:
	"""Convert embedding list to JSON string for storage."""
	if not embedding:
		return None
	try:
		return json.dumps(embedding)
	except Exception:
		return None


def json_to_embedding(embedding_json: Optional[str]) -> Optional[List[float]]:
	"""Parse embedding JSON string back to list of floats."""
	if not embedding_json:
		return None
	try:
		data = json.loads(embedding_json)
		if isinstance(data, list):
			return [float(x) for x in data]
	except Exception:
		pass
	return None


def cosine_similarity(a: List[float], b: List[float]) -> float:
	"""
	Compute cosine similarity between two embedding vectors.
	
	Returns:
		Similarity score between -1 and 1 (typically 0 to 1 for embeddings).
	"""
	if not a or not b or len(a) != len(b):
		return 0.0
	
	dot_product = sum(x * y for x, y in zip(a, b))
	magnitude_a = math.sqrt(sum(x * x for x in a))
	magnitude_b = math.sqrt(sum(x * x for x in b))
	
	if magnitude_a == 0.0 or magnitude_b == 0.0:
		return 0.0
	
	return dot_product / (magnitude_a * magnitude_b)


def search_product_qas(
	product_id: int,
	query_text: str,
	*,
	limit: int = 5,
	min_similarity: float = 0.7,
) -> List[Tuple[ProductQA, float]]:
	"""
	Search for matching Q&As for a product using semantic similarity.
	
	Args:
		product_id: Product ID to search within
		query_text: Query text to find similar Q&As
		limit: Maximum number of results to return
		min_similarity: Minimum similarity score threshold (0.0 to 1.0)
	
	Returns:
		List of (ProductQA, similarity_score) tuples, sorted by similarity descending.
	"""
	if not query_text or not query_text.strip():
		return []
	
	# Generate embedding for query
	query_embedding = generate_embedding(query_text)
	if not query_embedding:
		log.warning("Failed to generate embedding for query: %s", query_text[:100])
		return []
	
	# Load all active Q&As for this product
	with get_session() as session:
		from sqlmodel import select as _select
		
		qas = session.exec(
			_select(ProductQA)
			.where(ProductQA.product_id == product_id)
			.where(ProductQA.is_active == True)  # noqa: E712
		).all()
		
		results: List[Tuple[ProductQA, float]] = []
		
		for qa in qas:
			if not qa.embedding_json:
				continue
			
			qa_embedding = json_to_embedding(qa.embedding_json)
			if not qa_embedding:
				continue
			
			similarity = cosine_similarity(query_embedding, qa_embedding)
			if similarity >= min_similarity:
				results.append((qa, similarity))
		
		# Sort by similarity descending
		results.sort(key=lambda x: x[1], reverse=True)
		
		# Return top N
		return results[:limit]


def search_all_product_qas(
	query_text: str,
	*,
	limit: int = 5,
	min_similarity: float = 0.7,
) -> List[Tuple[ProductQA, float]]:
	"""
	Search for matching Q&As across all products using semantic similarity.
	
	Args:
		query_text: Query text to find similar Q&As
		limit: Maximum number of results to return
		min_similarity: Minimum similarity score threshold (0.0 to 1.0)
	
	Returns:
		List of (ProductQA, similarity_score) tuples, sorted by similarity descending.
	"""
	if not query_text or not query_text.strip():
		return []
	
	# Generate embedding for query
	query_embedding = generate_embedding(query_text)
	if not query_embedding:
		log.warning("Failed to generate embedding for query: %s", query_text[:100])
		return []
	
	# Load all active Q&As
	with get_session() as session:
		from sqlmodel import select as _select
		
		qas = session.exec(
			_select(ProductQA).where(ProductQA.is_active == True)  # noqa: E712
		).all()
		
		results: List[Tuple[ProductQA, float]] = []
		
		for qa in qas:
			if not qa.embedding_json:
				continue
			
			qa_embedding = json_to_embedding(qa.embedding_json)
			if not qa_embedding:
				continue
			
			similarity = cosine_similarity(query_embedding, qa_embedding)
			if similarity >= min_similarity:
				results.append((qa, similarity))
		
		# Sort by similarity descending
		results.sort(key=lambda x: x[1], reverse=True)
		
		# Return top N
		return results[:limit]

