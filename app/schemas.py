from enum import Enum
from typing import Optional, Any
from datetime import date
from pydantic import BaseModel, Field


class SourceEnum(str, Enum):
	bizim = "bizim"
	kargo = "kargo"


class ImportPreviewRequest(BaseModel):
	source: SourceEnum
	filename: Optional[str] = Field(default=None, description="Specific file to import; if omitted, server may auto-pick.")


class ImportPreviewResponse(BaseModel):
	source: SourceEnum
	filename: str
	row_count: int
	sample: list[dict[str, Any]]


class ImportCommitRequest(BaseModel):
	source: SourceEnum
	filename: str


class ImportRunSummary(BaseModel):
	id: int
	source: SourceEnum
	filename: str
	row_count: int
	created_clients: int
	updated_clients: int
	created_items: int
	created_orders: int
	created_payments: int
	unmatched_count: int


# --- Typed row schemas and whitelists ---

class BizimRow(BaseModel):
	record_type: str = "bizim"
	name: Optional[str] = None
	phone: Optional[str] = None
	address: Optional[str] = None
	city: Optional[str] = None
	item_name: Optional[str] = None
	quantity: Optional[int] = None
	unit_price: Optional[float] = None
	total_amount: Optional[float] = None
	shipment_date: Optional[date] = None
	tracking_no: Optional[str] = None
	notes: Optional[str] = None

	class Config:
		extra = 'forbid'


class KargoRow(BaseModel):
	record_type: str = "kargo"
	name: Optional[str] = None
	address: Optional[str] = None
	city: Optional[str] = None
	tracking_no: Optional[str] = None
	shipment_date: Optional[date] = None
	delivery_date: Optional[date] = None
	payment_amount: Optional[float] = None
	total_amount: Optional[float] = None
	quantity: Optional[int] = None
	payment_method: Optional[str] = None
	fee_komisyon: Optional[float] = 0.0
	fee_hizmet: Optional[float] = 0.0
	fee_kargo: Optional[float] = 0.0
	fee_iade: Optional[float] = 0.0
	fee_erken_odeme: Optional[float] = 0.0
	alici_kodu: Optional[str] = None
	notes: Optional[str] = None

	class Config:
		extra = 'forbid'


BIZIM_ALLOWED_KEYS = set(BizimRow.__fields__.keys())
KARGO_ALLOWED_KEYS = set(KargoRow.__fields__.keys())
