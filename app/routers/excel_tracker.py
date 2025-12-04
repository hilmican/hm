from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse
from sqlmodel import select
from sqlalchemy import func
from pathlib import Path
import ast

from ..db import get_session
from ..models import Order, ImportRow, ImportRun, Client

router = APIRouter()

# Project root is two levels up from this file: app/routers/excel_tracker.py -> app/ -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BIZIM_DIR = PROJECT_ROOT / "bizimexcellerimiz"
KARGO_DIR = PROJECT_ROOT / "kargocununexcelleri"
IADE_DIR = PROJECT_ROOT / "iadeler"


@router.get("/excel-tracker", response_class=HTMLResponse)
def excel_tracker_page(request: Request, order_id: int | None = None, q: str | None = None):
    """Page to search orders and see which Excel files they came from."""
    if not request.session.get("uid"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    orders_data = []
    
    with get_session() as session:
        orders = []
        
        if order_id:
            # Direct order ID search
            order = session.exec(select(Order).where(Order.id == order_id)).first()
            if order:
                orders = [order]
        elif q:
            # Search by client name or phone
            q_norm = (q or "").strip()
            if q_norm:
                digits = "".join(ch for ch in q_norm if ch.isdigit())
                clients = []
                if digits and len(digits) >= 3:
                    clients = session.exec(
                        select(Client)
                        .where(Client.phone != None)
                        .where(Client.phone.contains(digits))
                        .limit(20)
                    ).all()
                if not clients:
                    # Case-insensitive name search using func.lower and LIKE
                    clients = session.exec(
                        select(Client)
                        .where(func.lower(Client.name).like(f"%{q_norm.lower()}%"))
                        .limit(20)
                    ).all()
                
                if clients:
                    client_ids = [c.id for c in clients if c.id]
                    orders = session.exec(
                        select(Order)
                        .where(Order.client_id.in_(client_ids))
                        .order_by(Order.id.desc())
                        .limit(50)
                    ).all()
        
        # For each order, find all ImportRows that matched it
        for order in orders:
            if not order.id:
                continue
            
            import_rows = session.exec(
                select(ImportRow)
                .where(ImportRow.matched_order_id == order.id)
                .order_by(ImportRow.id.asc())
            ).all()
            
            rows_data = []
            for ir in import_rows:
                run = session.exec(select(ImportRun).where(ImportRun.id == ir.import_run_id)).first()
                if not run:
                    continue
                
                # Determine file path
                if run.source == "bizim":
                    file_path = BIZIM_DIR / run.filename
                elif run.source == "kargo":
                    file_path = KARGO_DIR / run.filename
                elif run.source == "returns":
                    file_path = IADE_DIR / run.filename
                else:
                    file_path = None
                
                file_exists = file_path.exists() if file_path else False
                
                # Parse mapped_json
                mapped_data = {}
                try:
                    mapped_data = ast.literal_eval(ir.mapped_json) if ir.mapped_json else {}
                except Exception:
                    pass
                
                rows_data.append({
                    "import_row_id": ir.id,
                    "import_run_id": ir.import_run_id,
                    "row_index": ir.row_index,
                    "status": ir.status,
                    "message": ir.message,
                    "source": run.source,
                    "filename": run.filename,
                    "data_date": run.data_date,
                    "started_at": run.started_at,
                    "file_exists": file_exists,
                    "file_path": str(file_path) if file_path else None,
                    "mapped_data": mapped_data,
                })
            
            if rows_data:
                client = session.exec(select(Client).where(Client.id == order.client_id)).first() if order.client_id else None
                orders_data.append({
                    "order": order,
                    "client": client,
                    "import_rows": rows_data,
                })
    
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "excel_tracker.html",
        {
            "request": request,
            "orders_data": orders_data,
            "order_id": order_id,
            "q": q or "",
        },
    )


@router.get("/excel-tracker/download/{run_id}")
def download_excel_file(run_id: int, request: Request):
    """Download the Excel file for a specific ImportRun."""
    if not request.session.get("uid"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    with get_session() as session:
        run = session.exec(select(ImportRun).where(ImportRun.id == run_id)).first()
        if not run:
            raise HTTPException(status_code=404, detail="ImportRun not found")
        
        # Determine file path
        if run.source == "bizim":
            file_path = BIZIM_DIR / run.filename
        elif run.source == "kargo":
            file_path = KARGO_DIR / run.filename
        elif run.source == "returns":
            file_path = IADE_DIR / run.filename
        else:
            raise HTTPException(status_code=404, detail="Unknown source")
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        
        return FileResponse(
            path=str(file_path),
            filename=run.filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

