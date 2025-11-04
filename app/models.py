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
    status: Optional[str] = Field(default=None, index=True, description="missing-bizim|missing-kargo|merged")
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
    # variant fields
    product_id: Optional[int] = Field(default=None, foreign_key="product.id", index=True)
    size: Optional[str] = Field(default=None, index=True)
    color: Optional[str] = Field(default=None, index=True)
    price: Optional[float] = None
    cost: Optional[float] = None
    status: Optional[str] = Field(default=None, index=True, description="active|inactive")
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
    total_cost: Optional[float] = Field(default=None, index=True)
    shipping_fee: Optional[float] = Field(default=None, index=True)
    shipment_date: Optional[dt.date] = Field(default=None, index=True)
    data_date: Optional[dt.date] = Field(default=None, index=True)
    # date when return or switch (iade/degisim) happened
    return_or_switch_date: Optional[dt.date] = Field(default=None, index=True)
    status: Optional[str] = Field(default=None, index=True)
    notes: Optional[str] = None
    source: str = Field(index=True, description="bizim|kargo")


class OrderItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int = Field(foreign_key="order.id", index=True)
    item_id: int = Field(foreign_key="item.id", index=True)
    quantity: int = 1
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class Payment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    amount: float
    # avoid shadowing the field name 'date' with the type name; use dt.date explicitly
    date: Optional[dt.date] = Field(default=None, index=True)
    method: Optional[str] = None
    reference: Optional[str] = None
    # fees and net amount (amount - sum(fees))
    fee_komisyon: Optional[float] = Field(default=0.0)
    fee_hizmet: Optional[float] = Field(default=0.0)
    fee_kargo: Optional[float] = Field(default=0.0)
    fee_iade: Optional[float] = Field(default=0.0)
    fee_erken_odeme: Optional[float] = Field(default=0.0)
    net_amount: Optional[float] = Field(default=0.0, index=True)


class OrderEditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int = Field(foreign_key="order.id", index=True)
    editor_user_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    action: Optional[str] = Field(default=None, index=True)
    changes_json: Optional[str] = None
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)


class StockMovement(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="item.id")
    direction: str = Field(description="in|out")
    quantity: int
    related_order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    reason: Optional[str] = None
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    slug: str = Field(index=True, unique=True)
    default_unit: Optional[str] = Field(default="adet")
    default_color: Optional[str] = None
    default_price: Optional[float] = None
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class ImportRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: str = Field(index=True, description="bizim|kargo")
    filename: str
    data_date: Optional[dt.date] = None
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


class ItemMappingRule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source_pattern: str = Field(index=True)
    match_mode: str = Field(default="exact", index=True, description="exact|icontains|regex")
    priority: int = Field(default=0, index=True)
    notes: Optional[str] = None
    is_active: bool = Field(default=True, index=True)
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class ItemMappingOutput(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    rule_id: int = Field(foreign_key="itemmappingrule.id", index=True)
    # Either directly reference an item, or define variant attributes under a product
    item_id: Optional[int] = Field(default=None, foreign_key="item.id", index=True)
    product_id: Optional[int] = Field(default=None, foreign_key="product.id", index=True)
    size: Optional[str] = Field(default=None)
    color: Optional[str] = Field(default=None)
    quantity: int = 1
    unit_price: Optional[float] = None


class ReconcileTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    import_row_id: int = Field(foreign_key="importrow.id")
    candidates_json: str
    chosen_id: Optional[int] = None
    resolved_at: Optional[dt.datetime] = None


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str
    role: str = Field(default="admin", index=True)
    failed_attempts: int = 0
    locked_until: Optional[dt.datetime] = Field(default=None, index=True)
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class Message(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ig_sender_id: Optional[str] = Field(default=None, index=True)
    ig_recipient_id: Optional[str] = Field(default=None, index=True)
    ig_message_id: Optional[str] = Field(default=None, index=True, unique=True)
    text: Optional[str] = None
    attachments_json: Optional[str] = None
    timestamp_ms: Optional[int] = Field(default=None, index=True)
    raw_json: Optional[str] = None
    conversation_id: Optional[str] = Field(default=None, index=True)
    direction: Optional[str] = Field(default=None, index=True, description="in|out")
    sender_username: Optional[str] = Field(default=None, index=True)
    # ads/referral metadata (best-effort)
    ad_id: Optional[str] = Field(default=None, index=True)
    ad_link: Optional[str] = Field(default=None)
    ad_title: Optional[str] = Field(default=None)
    referral_json: Optional[str] = Field(default=None)
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)