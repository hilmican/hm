from __future__ import annotations

import re
from typing import Dict, List, Optional, Any

from sqlmodel import select

from ..db import get_session
from ..models import Product, Item


def parse_height_weight(message: str) -> Dict[str, Optional[int]]:
	"""
	Parse height and weight from a customer message.
	
	Looks for number pairs that could represent height (150-200 cm) and weight (50-120 kg).
	Common formats: "179,76", "179 76", "179/76", "boy 179 kilo 76", etc.
	
	Returns: {"height_cm": int | None, "weight_kg": int | None}
	"""
	if not message or not isinstance(message, str):
		return {"height_cm": None, "weight_kg": None}
	
	# Extract all numbers from the message
	numbers = re.findall(r'\d+', message)
	
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
	
	This is a basic heuristic implementation. For more accurate sizing,
	this should be replaced with product-specific size tables stored in the database.
	
	For now, uses a simple BMI-based approach for S/M/L/XL/XXL sizes.
	For numeric sizes (30, 31, 32, etc.), uses a different heuristic.
	
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
				# Use BMI-based heuristic for letter sizes
				# BMI = weight (kg) / (height (m))^2
				height_m = height_cm / 100.0
				bmi = weight_kg / (height_m * height_m)
				
				# Simple BMI to size mapping (can be refined with actual size tables)
				if bmi < 18.5:
					# Underweight - try S or M
					if "S" in available_sizes:
						return "S"
					elif "M" in available_sizes:
						return "M"
				elif bmi < 22:
					# Normal weight - M or L
					if "M" in available_sizes:
						return "M"
					elif "L" in available_sizes:
						return "L"
				elif bmi < 25:
					# Slightly overweight - L or XL
					if "L" in available_sizes:
						return "L"
					elif "XL" in available_sizes:
						return "XL"
				else:
					# Overweight - XL or XXL
					if "XL" in available_sizes:
						return "XL"
					elif "XXL" in available_sizes:
						return "XXL"
					elif "L" in available_sizes:
						return "L"
				
				# Fallback: return first available size
				sorted_sizes = sorted(available_sizes, key=lambda x: (
					{"XS": 0, "S": 1, "M": 2, "L": 3, "XL": 4, "XXL": 5, "XXXL": 6, "3XL": 6}.get(x, 99),
					x
				))
				return sorted_sizes[0] if sorted_sizes else None
			
			elif has_numeric_sizes:
				# For numeric sizes (waist sizes), use a simple height/weight heuristic
				# This is very approximate and should be replaced with actual size tables
				# For now, estimate based on BMI
				height_m = height_cm / 100.0
				bmi = weight_kg / (height_m * height_m)
				
				# Convert numeric sizes to integers for comparison
				numeric_sizes = sorted([int(s) for s in available_sizes if s.isdigit()])
				
				if not numeric_sizes:
					return None
				
				# Rough estimate: lower BMI = smaller waist, higher BMI = larger waist
				if bmi < 20:
					# Smaller sizes
					return str(numeric_sizes[0]) if numeric_sizes else None
				elif bmi < 23:
					# Medium sizes
					mid_idx = len(numeric_sizes) // 2
					return str(numeric_sizes[mid_idx]) if numeric_sizes else None
				else:
					# Larger sizes
					return str(numeric_sizes[-1]) if numeric_sizes else None
			
			# Unknown size format - return first available
			return list(available_sizes)[0] if available_sizes else None
			
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

