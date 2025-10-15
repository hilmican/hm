from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from datetime import datetime, date

from sqlmodel import Field, SQLModel, Relationship

# Forward references for type checking without runtime evaluation
if TYPE_CHECKING:
    from typing import List


class Client(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    phone: Optional[str] = Field(default=None, index=True)
    email: Optional[str] = None
    tax_id: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = Field(default=None, index=True)
    unique_key: Optional[str] = Field(default=None, index=True, unique=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships (optional - add if needed)
    orders: List["Order"] = Relationship(back_populates="client")
    payments: List["Payment"] = Relationship(back_populates="client")


class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str = Field(index=True, unique=True)
    name: str = Field(index=True)
    unit: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships (optional)
    orders: List["Order"] = Relationship(back_populates="item")


class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tracking_no: Optional[str] = Field(default=None, index=True)
    client_id: int = Field(foreign_key="client.id")
    item_id: Optional[int] = Field(default=None, foreign_key="item.id")
    quantity: Optional[int] = 1
    unit_price: Optional[float] = None
    total_amount: Optional[float] = None
    shipment_date: Optional[date] = Field(default=None, index=True)
    status: Optional[str] = Field(default=None, index=True)
    notes: Optional[str] = None
    source: str = Field(index=True, description="bizim|kargo")
    
    # Relationships
    client: Optional["Client"] = Relationship(back_populates="orders")
    item: Optional["Item"] = Relationship(back_populates="orders")
    payments: List["Payment"] = Relationship(back_populates="order")


class Payment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    amount: float
    date: Optional[date] = Field(default=None, index=True)
    method: Optional[str] = None
    reference: Optional[str] = None
    
    # Relationships
    client: Optional["Client"] = Relationship(back_populates="payments")
    order: Optional["Order"] = Relationship(back_populates="payments")


class StockMovement(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="item.id")
    direction: str = Field(description="in|out")
    quantity: int
    related_order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Relationships
    item: Optional["Item"] = Relationship(back_populates="stock_movements")
    order: Optional["Order"] = Relationship(back_populates="stock_movements")


class ImportRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str = Field(index=True, description="bizim|kargo")
    filename: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    row_count: int = 0
    created_clients: int = 0
    updated_clients: int = 0
    created_items: int = 0
    created_orders: int = 0
    created_payments: int = 0
    unmatched_count: int = 0
    errors_json: Optional[str] = None
    
    # Relationship
    rows: List["ImportRow"] = Relationship(back_populates="import_run")


class ImportRow(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    import_run_id: int = Field(foreign_key="importrun.id")
    row_index: int
    row_hash: str = Field(index=True)
    mapped_json: str
    status: str = Field(index=True, description="created|updated|skipped|unmatched|error")
    message: Optional[str] = None
    matched_client_id: Optional[int] = Field(
        default=None, 
        foreign_key="client.id"
    )
    matched_order_id: Optional[int] = Field(
        default=None, 
        foreign_key="order.id"
    )
    
    # Relationships
    import_run: Optional["ImportRun"] = Relationship(back_populates="rows")
    client: Optional["Client"] = Relationship(sa_column=matched_client_id)
    order: Optional["Order"] = Relationship(sa_column=matched_order_id)
    reconcile_tasks: List["ReconcileTask"] = Relationship(back_populates="import_row")


class ReconcileTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    import_row_id: int = Field(foreign_key="importrow.id")
    candidates_json: str
    chosen_id: Optional[int] = None
    resolved_at: Optional[datetime] = None
    
    # Relationship
    import_row: Optional["ImportRow"] = Relationship(back_populates="reconcile_tasks")