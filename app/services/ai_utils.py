from __future__ import annotations

import re
import logging
from typing import Dict, List, Optional, Any

from sqlmodel import select

from ..db import get_session
from ..models import Product, Item

size_log = logging.getLogger("ai.size_matrix")
DECIMAL_HEIGHT_PATTERN = re.compile(r'(?<!\d)1[\s\.,/-]+([5-9][0-9])(?!\d)', re.IGNORECASE)
NUMBER_WORDS = {
	"sıfır": 0,
	"bir": 1,
	"bi": 1,
	"iki": 2,
	"üç": 3,
	"dört": 4,
	"bes": 5,
	"beş": 5,
	"alti": 6,
	"altı": 6,
	"yedi": 7,
	"sekiz": 8,
	"dokuz": 9,
	"on": 10,
	"onbir": 11,
	"on bir": 11,
	"oniki": 12,
	"on iki": 12,
	"onüç": 13,
	"on üç": 13,
	"ondört": 14,
	"on dört": 14,
	"onbes": 15,
	"on beş": 15,
	"onaltı": 16,
	"on altı": 16,
	"onyedi": 17,
	"on yedi": 17,
	"onsekiz": 18,
	"on sekiz": 18,
	"ondokuz": 19,
	"on dokuz": 19,
	"yirmi": 20,
	"otuz": 30,
	"kirk": 40,
	"kırk": 40,
	"elli": 50,
	"altmis": 60,
	"altmış": 60,
	"atmıs": 60,
	"atmış": 60,
	"yetmis": 70,
	"yetmiş": 70,
	"seksen": 80,
	"doksan": 90,
}

NUMERIC_SIZE_MATRIX: Dict[int, List[tuple[int, str]]] = {
	160: [
		(65, "30"),
		(70, "31"),
		(75, "32"),
		(80, "33"),
		(85, "34"),
		(90, "36"),
		(95, "38"),
		(100, "38"),
		(105, "40"),
		(110, "42"),
		(115, "42"),
		(120, "42"),
	],
	170: [
		(65, "30"),
		(70, "31"),
		(75, "32"),
		(80, "32"),
		(85, "33"),
		(90, "34"),
		(95, "36"),
		(100, "38"),
		(105, "38"),
		(110, "40"),
		(115, "40"),
		(120, "42"),
	],
	180: [
		(65, "30"),
		(70, "31"),
		(75, "32"),
		(80, "32"),
		(85, "33"),
		(90, "34"),
		(95, "36"),
		(100, "38"),
		(105, "38"),
		(110, "40"),
		(115, "40"),
		(120, "42"),
	],
	190: [
		(65, "30"),
		(70, "31"),
		(75, "32"),
		(80, "32"),
		(85, "33"),
		(90, "34"),
		(95, "34"),
		(100, "36"),
		(105, "38"),
		(110, "40"),
		(115, "40"),
		(120, "42"),
	],
}

LETTER_SIZE_MATRIX: Dict[int, List[tuple[int, str]]] = {
	160: [
		(50, "S"),
		(55, "S"),
		(60, "S"),
		(65, "S"),
		(70, "M"),
		(75, "L"),
		(80, "L"),
		(85, "XL"),
		(90, "XL"),
	],
	170: [
		(50, "S"),
		(55, "S"),
		(60, "S"),
		(65, "S"),
		(70, "M"),
		(75, "M"),
		(80, "L"),
		(85, "L"),
		(90, "XL"),
		(95, "XL"),
	],
	180: [
		(50, "S"),
		(55, "S"),
		(60, "S"),
		(65, "S"),
		(70, "M"),
		(75, "M"),
		(80, "L"),
		(85, "L"),
		(90, "XL"),
		(95, "XL"),
	],
	190: [
		(50, "S"),
		(55, "S"),
		(60, "S"),
		(65, "S"),
		(70, "M"),
		(75, "M"),
		(80, "M"),
		(85, "L"),
		(90, "XL"),
		(95, "XL"),
	],
}


def _closest_height_row(matrix: Dict[int, List[tuple[int, str]]], height_cm: int) -> Optional[int]:
	if not matrix:
		return None
	return min(matrix.keys(), key=lambda h: abs(h - height_cm))


def _lookup_matrix_size(
	matrix: Dict[int, List[tuple[int, str]]],
	height_cm: int,
	weight_kg: int,
) -> Optional[str]:
	if not (height_cm and weight_kg):
		return None
	row_height = _closest_height_row(matrix, height_cm)
	if row_height is None:
		return None
	columns = sorted(matrix.get(row_height, []), key=lambda entry: entry[0])
	if not columns:
		return None
	for max_weight, size in columns:
		if weight_kg <= max_weight:
			return size
	return None

def parse_height_weight(message: str) -> Dict[str, Optional[int]]:
	"""
	Parse height and weight from a customer message.
	
	Looks for number pairs that could represent height (150-200 cm) and weight (50-120 kg).
	Common formats: "179,76", "179 76", "179/76", "boy 179 kilo 76", etc.
	
	Returns: {"height_cm": int | None, "weight_kg": int | None}
	"""
	if not message or not isinstance(message, str):
		return {"height_cm": None, "weight_kg": None}
	
	if not message or not isinstance(message, str):
		return {"height_cm": None, "weight_kg": None}
	
	lowered = message.lower()
	for word, val in NUMBER_WORDS.items():
		if word in lowered:
			lowered = lowered.replace(word, str(val))
	
	# Normalize decimal-like heights (e.g., "1.75", "1 75") into 3-digit cm values
	normalized_message = DECIMAL_HEIGHT_PATTERN.sub(lambda m: f"1{m.group(1)}", lowered)
	
	# Extract all numbers from the normalized message
	numbers = re.findall(r'\d+', normalized_message)
	
	if len(numbers) < 2:
		return {"height_cm": None, "weight_kg": None}
	
	# Try to find height (150-200 range) and weight (50-120 range)
	height_cm: Optional[int] = None
	weight_kg: Optional[int] = None
	
	for i, num_str in enumerate(numbers):
		try:
			num = int(num_str)
			# Check if it could be height (150-200)
			if 150 <= num <= 200 and height_cm is None:
				height_cm = num
			# Check if it could be weight (50-120)
			elif 50 <= num <= 120 and weight_kg is None:
				weight_kg = num
		except (ValueError, TypeError):
			continue
	
	# If we found one but not the other, try adjacent numbers
	if height_cm and not weight_kg:
		# Look for weight in adjacent positions
		for i, num_str in enumerate(numbers):
			try:
				num = int(num_str)
				if num == height_cm and i + 1 < len(numbers):
					# Check next number
					try:
						next_num = int(numbers[i + 1])
						if 50 <= next_num <= 120:
							weight_kg = next_num
							break
					except (ValueError, TypeError):
						pass
			except (ValueError, TypeError):
				continue
	
	if weight_kg and not height_cm:
		# Look for height in adjacent positions
		for i, num_str in enumerate(numbers):
			try:
				num = int(num_str)
				if num == weight_kg and i > 0:
					# Check previous number
					try:
						prev_num = int(numbers[i - 1])
						if 150 <= prev_num <= 200:
							height_cm = prev_num
							break
					except (ValueError, TypeError):
						pass
			except (ValueError, TypeError):
				continue
	
	return {"height_cm": height_cm, "weight_kg": weight_kg}


def calculate_size_suggestion(height_cm: int, weight_kg: int, product_id: int) -> Optional[str]:
	"""
	Calculate size suggestion based on height and weight for a product.
	
	Uses the explicit size matrices provided by the business team.
	Returns: size string (e.g., "M", "L", "32") or None if cannot determine
	"""
	if not height_cm or not weight_kg or not product_id:
		return None
	
	# Get product to check available sizes
	with get_session() as session:
		try:
			product = session.get(Product, product_id)
			if not product:
				return None
			
			# Get all available sizes for this product
			items = session.exec(
				select(Item).where(Item.product_id == product_id)
			).all()
			
			available_sizes = set()
			for item in items:
				if item.size:
					available_sizes.add(item.size.strip().upper())
			
			if not available_sizes:
				return None
			
			# Determine if we're dealing with letter sizes (S/M/L) or numeric sizes (30/31/32)
			has_letter_sizes = any(s in available_sizes for s in ["S", "M", "L", "XL", "XXL", "XS", "XXXL", "3XL"])
			has_numeric_sizes = any(s.isdigit() for s in available_sizes)
			
			if has_letter_sizes:
				size = _lookup_matrix_size(LETTER_SIZE_MATRIX, height_cm, weight_kg)
				size_log.info(
					"size_matrix letter lookup product_id=%s height_cm=%s weight_kg=%s -> %s",
					product_id,
					height_cm,
					weight_kg,
					size,
				)
				if size and size.upper() in available_sizes:
					return size.upper()
				return None
			
			if has_numeric_sizes:
				size = _lookup_matrix_size(NUMERIC_SIZE_MATRIX, height_cm, weight_kg)
				size_log.info(
					"size_matrix numeric lookup product_id=%s height_cm=%s weight_kg=%s -> %s",
					product_id,
					height_cm,
					weight_kg,
					size,
				)
				if size and size in available_sizes:
					return size
				return None
			
			return None
			
		except Exception:
			return None


def detect_color_count(stock: List[Dict[str, Any]]) -> bool:
	"""
	Detect if product has multiple colors based on stock list.
	
	Returns True if product has multiple distinct colors, False otherwise.
	"""
	if not stock or not isinstance(stock, list):
		return False
	
	colors: set[str] = set()
	for entry in stock:
		if not isinstance(entry, dict):
			continue
		color = entry.get("color")
		if color:
			# Normalize color string
			color_str = str(color).strip().upper()
			if color_str:
				colors.add(color_str)
	
	# If we have more than one distinct color, product has multiple colors
	return len(colors) > 1

