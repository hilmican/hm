#!/usr/bin/env python3
import sys
sys.path.insert(0, '/app')
from app.db import get_session
from app.models import Order, ImportRow, ImportRun
from sqlmodel import select
import re
from datetime import datetime

with get_session() as session:
    order = session.exec(select(Order).where(Order.id == 6125)).first()
    if order:
        print(f"Order 6125 before:")
        print(f"  source={order.source}, data_date={order.data_date}, shipment_date={order.shipment_date}")
        
        # Find kargo ImportRows
        kargo_rows = session.exec(
            select(ImportRow)
            .join(ImportRun, ImportRow.import_run_id == ImportRun.id)
            .where(ImportRow.matched_order_id == 6125, ImportRun.source == 'kargo')
            .order_by(ImportRun.started_at.desc())
        ).all()
        
        for ir in kargo_rows[:1]:
            run = session.exec(select(ImportRun).where(ImportRun.id == ir.import_run_id)).first()
            if run:
                match = re.match(r'^(\d{4}-\d{2}-\d{2})', run.filename)
                if match:
                    filename_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                    print(f"\n  Kargo import: {run.filename} -> date={filename_date}")
                    
                    if order.data_date == order.shipment_date:
                        print(f"  ✓ Should fix: data_date ({order.data_date}) equals shipment_date")
                        print(f"  → Will update to: {filename_date}")
                    elif filename_date != order.data_date and order.source == "bizim":
                        print(f"  ✓ Should fix: bizim order matched by kargo")
                        print(f"  → Will update from {order.data_date} to {filename_date}")




