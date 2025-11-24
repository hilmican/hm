import datetime as dt
from typing import Optional

from sqlmodel import Field, SQLModel
from sqlalchemy import UniqueConstraint, Text, Column, BigInteger


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
    merged_into_client_id: Optional[int] = Field(default=None, foreign_key="client.id", index=True)
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
    paid_by_bank_transfer: Optional[bool] = Field(default=False, index=True, description="IBAN ödeme: True ise kargo ücreti sadece taban (89) olarak alınır")
    shipment_date: Optional[dt.date] = Field(default=None, index=True)
    data_date: Optional[dt.date] = Field(default=None, index=True)
    # date when return or switch (iade/degisim) happened
    return_or_switch_date: Optional[dt.date] = Field(default=None, index=True)
    status: Optional[str] = Field(default=None, index=True)
    notes: Optional[str] = None
    source: str = Field(index=True, description="bizim|kargo")
    # Link to Instagram conversation id (e.g., "dm:<ig_user_id>")
    ig_conversation_id: Optional[str] = Field(default=None, index=True)


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
    # AI prompt fields (optional, per-product overrides)
    ai_system_msg: Optional[str] = Field(default=None, sa_column=Column(Text))
    ai_prompt_msg: Optional[str] = Field(default=None, sa_column=Column(Text))
    ai_tags: Optional[str] = Field(default=None, description="JSON array of keywords for focus detection")
    ai_variant_exclusions: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="JSON or comma separated variant (color/size) exclusions for AI replies",
    )
    pretext_id: Optional[int] = Field(
        default=None,
        foreign_key="ai_pretext.id",
        index=True,
        description="Which pretext to use for this product (prepended to system message)",
    )
    ai_reply_sending_enabled: bool = Field(
        default=True,
        description="Whether AI can actually send replies for this product (shadow replies always run)",
    )
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class ProductImage(SQLModel, table=True):
    __tablename__ = "product_images"

    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", index=True)

    # Absolute/public URL, typically built from IMAGE_CDN_BASE_URL + \"products/{sku}/image-x.jpg\"
    url: str = Field(description="Public image URL for this product")

    # Optional variant key such as \"krem\", \"acik-gri\" or \"krem-m\"
    variant_key: Optional[str] = Field(
        default=None,
        index=True,
        description="Variant/color key used for variant-aware image selection",
    )

    # General gallery order
    position: int = Field(
        default=1,
        index=True,
        description="Display order within the product's gallery",
    )

    # AI configuration
    ai_send: bool = Field(
        default=True,
        index=True,
        description="If true, AI is allowed to send this image in replies",
    )
    ai_send_order: Optional[int] = Field(
        default=None,
        index=True,
        description="Relative order when AI sends multiple images (1,2,3,...).",
    )

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
    mapped_json: str = Field(sa_column=Column(Text))
    status: str = Field(index=True, description="created|updated|skipped|unmatched|error")
    message: Optional[str] = Field(default=None, sa_column=Column(Text))
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
    preferred_language: Optional[str] = Field(default=None, index=True)
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
    timestamp_ms: Optional[int] = Field(default=None, sa_column=Column(BigInteger))
    raw_json: Optional[str] = Field(default=None, sa_column=Column(Text))
    # Internal FK to Conversation.id (no longer stores Graph ids or dm:<id> strings)
    conversation_id: Optional[int] = Field(
        default=None, foreign_key="conversations.id", index=True
    )
    direction: Optional[str] = Field(default=None, index=True, description="in|out")
    sender_username: Optional[str] = Field(default=None, index=True)
    # story reply metadata (optional)
    story_id: Optional[str] = Field(default=None, index=True)
    story_url: Optional[str] = Field(default=None)
    # AI assistant lifecycle (optional)
    ai_status: Optional[str] = Field(default=None, index=True, description="draft|sent|error")
    ai_json: Optional[str] = Field(default=None, sa_column=Column(Text))
    # ads/referral metadata (best-effort)
    ad_id: Optional[str] = Field(default=None, index=True)
    ad_link: Optional[str] = Field(default=None)
    ad_title: Optional[str] = Field(default=None)
    ad_image_url: Optional[str] = Field(default=None)
    ad_name: Optional[str] = Field(default=None)
    referral_json: Optional[str] = Field(default=None)
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


# Additional tables used by ingestion/workers that were previously created via raw SQL

class Attachment(SQLModel, table=True):
    __tablename__ = "attachments"
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(foreign_key="message.id", index=True)
    kind: str
    graph_id: Optional[str] = None
    position: Optional[int] = None
    mime: Optional[str] = None
    size_bytes: Optional[int] = None
    checksum_sha256: Optional[str] = None
    storage_path: Optional[str] = None
    thumb_path: Optional[str] = None
    fetched_at: Optional[dt.datetime] = None
    fetch_status: Optional[str] = Field(default=None, index=True)
    fetch_error: Optional[str] = None


class Conversation(SQLModel, table=True):
    """
    Canonical Instagram DM conversation entity.

    - Internal `id` is the primary key used by the app and FKs.
    - `igba_id` + `ig_user_id` identify the page/user pair.
    - `graph_conversation_id` stores the external Graph thread id when known.
    - Inbox / AI summary fields mirror what used to live in ai_conversations.
    """

    __tablename__ = "conversations"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Page / user mapping
    igba_id: str = Field(index=True, description="Instagram business account (page) id")
    ig_user_id: str = Field(index=True, description="Other party Instagram user id")
    graph_conversation_id: Optional[str] = Field(
        default=None,
        index=True,
        description="Facebook/Instagram Graph conversation/thread id",
    )

    # Conversation lifecycle / inbox summary
    last_message_id: Optional[int] = Field(
        default=None,
        description="Last message.id seen for this conversation (for quick linking)",
    )
    last_message_timestamp_ms: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger),
        description="Timestamp in ms since epoch of the last message",
    )
    last_message_text: Optional[str] = Field(default=None)
    last_message_direction: Optional[str] = Field(
        default=None, description="in|out direction of the last message"
    )
    last_sender_username: Optional[str] = Field(default=None)
    ig_sender_id: Optional[str] = Field(
        default=None, description="ig_sender_id of the last message"
    )
    ig_recipient_id: Optional[str] = Field(
        default=None, description="ig_recipient_id of the last message"
    )
    last_ad_id: Optional[str] = Field(default=None, description="Last ad id seen (deprecated, use last_link_id)")
    last_ad_link: Optional[str] = Field(default=None, description="Deprecated, use last_link_id")
    last_ad_title: Optional[str] = Field(default=None, description="Deprecated")
    last_link_type: Optional[str] = Field(default=None, description="Type of last link: 'ad' or 'post'")
    last_link_id: Optional[str] = Field(default=None, description="Last link id (ad_id or post_id)")

    # Hydration / unread state
    last_message_at: Optional[dt.datetime] = Field(
        default=None, index=True, description="Fallback last message time (datetime)"
    )
    unread_count: int = Field(default=0, index=True)
    hydrated_at: Optional[dt.datetime] = Field(
        default=None,
        index=True,
        description="When this conversation was last hydrated from Graph",
    )


class IGUser(SQLModel, table=True):
    """
    Canonical Instagram user/contact entity.

    - Internal `id` is used as primary key.
    - `ig_user_id` is the external Instagram user id (unique).
    - Contact / CRM and AI-enrichment fields live here to avoid duplication
      across multiple conversations for the same user.
    """

    __tablename__ = "ig_users"

    id: Optional[int] = Field(default=None, primary_key=True)
    ig_user_id: str = Field(index=True, unique=True)

    # Basic profile
    username: Optional[str] = Field(default=None, index=True)
    name: Optional[str] = None
    profile_pic_url: Optional[str] = None
    last_seen_at: Optional[dt.datetime] = Field(default=None, index=True)
    fetched_at: Optional[dt.datetime] = Field(default=None, index=True)
    fetch_status: Optional[str] = Field(default=None, index=True)
    fetch_error: Optional[str] = None

    # Contact / CRM fields (moved from conversations / ai_conversations)
    contact_name: Optional[str] = Field(
        default=None,
        index=True,
        description="Extracted contact full name for this user",
    )
    contact_phone: Optional[str] = Field(
        default=None,
        index=True,
        description="Extracted phone number for this user",
    )
    contact_address: Optional[str] = Field(
        default=None,
        description="Extracted shipping/billing address for this user",
    )
    linked_order_id: Optional[int] = Field(
        default=None,
        foreign_key="order.id",
        index=True,
        description="Most relevant linked order for this user (if any)",
    )
    ai_status: Optional[str] = Field(
        default=None,
        index=True,
        description="High-level AI enrichment status for this user",
    )
    ai_json: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Last AI enrichment payload for this user (JSON)",
    )


class IGAiDebugRun(SQLModel, table=True):
    __tablename__ = "ig_ai_debug_run"
    id: Optional[int] = Field(default=None, primary_key=True)
    # Store internal conversation id as string for backward compatibility; callers pass int->str
    conversation_id: str = Field(index=True)
    job_id: Optional[int] = Field(default=None, index=True)
    ai_run_id: Optional[int] = Field(default=None, index=True)
    status: str = Field(default="pending", index=True)
    ai_model: Optional[str] = Field(default=None)
    system_prompt: Optional[str] = Field(default=None, sa_column=Column(Text))
    user_prompt: Optional[str] = Field(default=None, sa_column=Column(Text))
    raw_response: Optional[str] = Field(default=None, sa_column=Column(Text))
    extracted_json: Optional[str] = Field(default=None, sa_column=Column(Text))
    logs_json: Optional[str] = Field(default=None, sa_column=Column(Text))
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text))
    created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)
    started_at: Optional[dt.datetime] = Field(default=None, index=True)
    completed_at: Optional[dt.datetime] = Field(default=None, index=True)


class Job(SQLModel, table=True):
    __tablename__ = "jobs"
    id: Optional[int] = Field(default=None, primary_key=True)
    kind: str = Field(index=True)
    key: str = Field(index=True)
    run_after: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)
    attempts: int = 0
    max_attempts: int = 8
    payload: Optional[str] = None

    __table_args__ = (UniqueConstraint("kind", "key", name="uq_jobs_kind_key"),)


class AiShadowState(SQLModel, table=True):
	__tablename__ = "ai_shadow_state"
	# Canonical FK to conversations.id
	conversation_id: int = Field(primary_key=True, foreign_key="conversations.id")
	last_inbound_ms: Optional[int] = Field(default=None, sa_column=Column(BigInteger))
	next_attempt_at: Optional[dt.datetime] = Field(default=None, index=True)
	postpone_count: int = Field(default=0, index=True)
	status: Optional[str] = Field(default="pending", index=True, description="pending|running|suggested|paused|exhausted|error")
	ai_images_sent: bool = Field(default=False, description="Whether AI already scheduled product images for this conversation")
	state_json: Optional[str] = Field(default=None, sa_column=Column(Text))
	updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)


class AiShadowReply(SQLModel, table=True):
	__tablename__ = "ai_shadow_reply"
	id: Optional[int] = Field(default=None, primary_key=True)
	# Canonical FK to conversations.id
	conversation_id: int = Field(index=True, foreign_key="conversations.id")
	reply_text: Optional[str] = Field(default=None, sa_column=Column(Text))
	model: Optional[str] = None
	confidence: Optional[float] = None
	reason: Optional[str] = None
	json_meta: Optional[str] = Field(default=None, sa_column=Column(Text))
	actions_json: Optional[str] = Field(default=None, sa_column=Column(Text), description="Serialized list of automated actions (e.g., send_product_images)")
	state_json: Optional[str] = Field(default=None, sa_column=Column(Text), description="Serialized per-conversation AI state snapshot")
	attempt_no: Optional[int] = 0
	status: Optional[str] = Field(default="suggested", index=True, description="suggested|dismissed|expired|error")
	created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)


class SystemSetting(SQLModel, table=True):
	__tablename__ = "system_settings"
	key: str = Field(primary_key=True, description="Setting key")
	value: str = Field(description="Setting value (JSON-encoded if needed)")
	description: Optional[str] = Field(default=None, description="Human-readable description")
	updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)


class CostType(SQLModel, table=True):
	id: Optional[int] = Field(default=None, primary_key=True)
	name: str = Field(index=True, unique=True, description="Type name, e.g., Ads, Rent, Shipping supplies")
	created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)


class Cost(SQLModel, table=True):
	id: Optional[int] = Field(default=None, primary_key=True)
	type_id: int = Field(foreign_key="costtype.id", index=True)
	amount: float
	date: Optional[dt.date] = Field(default=None, index=True)
	details: Optional[str] = Field(default=None, sa_column=Column(Text))
	created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow, index=True)


class AIPretext(SQLModel, table=True):
	"""
	Pretext templates that can be prepended to AI system messages.
	Allows per-product customization of the system prompt prefix.
	"""
	__tablename__ = "ai_pretext"

	id: Optional[int] = Field(default=None, primary_key=True)
	name: str = Field(index=True, description="Pretext name/identifier")
	content: str = Field(sa_column=Column(Text), description="Pretext content to prepend to system message")
	is_default: bool = Field(default=False, index=True, description="Default pretext (used when product has no pretext_id)")
	created_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)
	updated_at: dt.datetime = Field(default_factory=dt.datetime.utcnow)