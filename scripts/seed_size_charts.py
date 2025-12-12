"""
Seed size charts from AI matrices into named size charts.

Targets:
- "Ceket Standart"  -> LETTER_SIZE_MATRIX
- "Pantolon Standart" -> NUMERIC_SIZE_MATRIX

Usage:
  python scripts/seed_size_charts.py
"""

from typing import Dict, List, Tuple, Optional

from sqlmodel import select

from app.db import get_session
from app.models import SizeChart, SizeChartEntry
from app.services.ai_utils import LETTER_SIZE_MATRIX, NUMERIC_SIZE_MATRIX


def _expand_bands(values: List[int], step: int = 5) -> List[int]:
	if not values:
		return []
	vmin, vmax = min(values), max(values)
	return list(range(vmin, vmax + 1, step))


def build_bands_and_grid(matrix: Dict[int, List[Tuple[int, str]]], step: int = 5) -> Tuple[List[int], List[int], List[List[str]]]:
	"""Expand height/weight bands by 5s and fill grid using closest height row."""
	heights_raw = sorted(matrix.keys())
	heights = _expand_bands(heights_raw, step=step)

	# weights: derive from the first row breakpoints
	first_row = matrix[heights_raw[0]]
	weight_breaks = sorted({max_w for max_w, _ in first_row})
	weights = _expand_bands(weight_breaks, step=step)

	def lookup_size(row: List[Tuple[int, str]], weight: int) -> str:
		for max_w, size in row:
			if weight <= max_w:
				return size
		return row[-1][1]

	def closest_height_row(h: int) -> int:
		return min(heights_raw, key=lambda x: abs(x - h))

	grid: List[List[str]] = []
	for w_start in weights:
		row_vals = []
		for h in heights:
			ref_h = closest_height_row(h)
			row_vals.append(lookup_size(matrix[ref_h], w_start))
		grid.append(row_vals)
	return heights, weights, grid


def upsert_chart(name: str, matrix: Dict[int, List[Tuple[int, str]]]) -> None:
	h_bands, w_bands, grid = build_bands_and_grid(matrix)
	with get_session() as session:
		chart = session.exec(select(SizeChart).where(SizeChart.name == name)).first()
		if not chart:
			raise SystemExit(f"Size chart '{name}' not found")
		# clear existing entries
		existing = session.exec(select(SizeChartEntry).where(SizeChartEntry.size_chart_id == chart.id)).all()
		for e in existing:
			session.delete(e)
		session.flush()
		# build ranges using same convention as upsert_grid (start, next_start-1)
		def to_ranges(bands: List[int]) -> List[Tuple[int, Optional[int]]]:
			r = []
			for idx, start in enumerate(bands):
				end = bands[idx + 1] - 1 if idx + 1 < len(bands) else None
				r.append((start, end))
			return r
		h_ranges = to_ranges(h_bands)
		w_ranges = to_ranges(w_bands)
		for w_idx, w_range in enumerate(w_ranges):
			for h_idx, h_range in enumerate(h_ranges):
				size_label = grid[w_idx][h_idx]
				entry = SizeChartEntry(
					size_chart_id=chart.id,
					size_label=size_label,
					height_min=h_range[0],
					height_max=h_range[1],
					weight_min=w_range[0],
					weight_max=w_range[1],
				)
				session.add(entry)
		print(f"[ok] {name}: {len(h_ranges) * len(w_ranges)} cells written")


def main() -> None:
	upsert_chart("Ceket Standart", LETTER_SIZE_MATRIX)
	upsert_chart("Pantolon Standart", NUMERIC_SIZE_MATRIX)


if __name__ == "__main__":
	main()

