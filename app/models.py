import datetime as dt
from typing import Optional

from sqlmodel import Field, SQLModel


class Client(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    phone: Optional[str] = Field(default=None, index=True)
    email: Optional[str] = None
    tax_id: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = Field(default=None, index=True)
    height_cm: Optional[int] = Field(default=None, description="Client height in centimeters")
    weight_kg: Optional[int] = Field(default=None, description="Client weight in kilograms")
    unique_key: Optional[str] = Field(default=None, index=True, unique=True)
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str = Field(index=True, unique=True)
    name: str = Field(index=True)
    unit: Optional[str] = None
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tracking_no: Optional[str] = Field(default=None, index=True)
    client_id: int = Field(foreign_key="client.id")
    item_id: Optional[int] = Field(default=None, foreign_key="item.id")
    quantity: Optional[int] = 1
    unit_price: Optional[float] = None
    total_amount: Optional[float] = None
    shipment_date: Optional[dt.date] = Field(default=None, index=True)
    status: Optional[str] = Field(default=None, index=True)
    notes: Optional[str] = None
    source: str = Field(index=True, description="bizim|kargo")


class Payment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    amount: float
    # avoid shadowing the field name 'date' with the type name; use dt.date explicitly
    date: Optional[dt.date] = Field(default=None, index=True)
    method: Optional[str] = None
    reference: Optional[str] = None


class StockMovement(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="item.id")
    direction: str = Field(description="in|out")
    quantity: int
    related_order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class ImportRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str = Field(index=True, description="bizim|kargo")
    filename: str
    started_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    completed_at: Optional[dt.datetime] = None
    row_count: int = 0
    created_clients: int = 0
    updated_clients: int = 0
    created_items: int = 0
    created_orders: int = 0
    created_payments: int = 0
    unmatched_count: int = 0
    errors_json: Optional[str] = None


class ImportRow(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    import_run_id: int = Field(foreign_key="importrun.id")
    row_index: int
    row_hash: str = Field(index=True)
    mapped_json: str
    status: str = Field(index=True, description="created|updated|skipped|unmatched|error")
    message: Optional[str] = None
    matched_client_id: Optional[int] = Field(default=None, foreign_key="client.id")
    matched_order_id: Optional[int] = Field(default=None, foreign_key="order.id")


class ReconcileTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    import_row_id: int = Field(foreign_key="importrow.id")
    candidates_json: str
    chosen_id: Optional[int] = None
    resolved_at: Optional[dt.datetime] = None