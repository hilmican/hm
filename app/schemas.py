from enum import Enum
from typing import Optional, Any
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
