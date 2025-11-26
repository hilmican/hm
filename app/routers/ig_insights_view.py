from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query, Request

from ..services import ig_insights

router = APIRouter(prefix="/ig/insights", tags=["instagram-insights"])


@router.get("/dashboard")
def insights_dashboard(request: Request):
	templates = request.app.state.templates
	return templates.TemplateResponse("ig_insights.html", {"request": request})


@router.get("/overview")
def overview(period: str = Query("day")):
	try:
		snapshot = ig_insights.ensure_insights(
			"account",
			subject_id=None,
			metrics=["impressions", "reach", "profile_views"],
			params={"period": period},
		)
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"insights_fetch_failed: {exc}")
	return json.loads(snapshot.payload_json)


@router.get("/engagement")
def engagement(period: str = Query("day")):
	"""
	Proxy endpoint returning the same cached payload but gives UI a dedicated endpoint.
	"""
	try:
		snapshot = ig_insights.ensure_insights(
			"account",
			subject_id=None,
			metrics=["impressions", "reach", "profile_views"],
			params={"period": period},
		)
	except Exception as exc:
		raise HTTPException(status_code=502, detail=f"insights_fetch_failed: {exc}")
	data = json.loads(snapshot.payload_json)
	# Derive simple engagement trend for UI convenience
	series = []
	for entry in data.get("data", []):
		name = entry.get("name")
		for value in entry.get("values", []):
			series.append(
				{
					"metric": name,
					"value": value.get("value"),
					"end_time": value.get("end_time"),
				}
			)
	return {"series": series}

