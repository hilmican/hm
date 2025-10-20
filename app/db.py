from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy import text

DB_PATH = Path("data/app.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


def init_db() -> None:
	SQLModel.metadata.create_all(engine)
	# lightweight migrations for existing SQLite DBs
	with engine.begin() as conn:
		def column_exists(table: str, column: str) -> bool:
			rows = conn.exec_driver_sql(f"PRAGMA table_info('{table}')").fetchall()
			# PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
			return any(r[1] == column for r in rows)

		# Client.height_cm / weight_kg
		if not column_exists("client", "height_cm"):
			conn.exec_driver_sql("ALTER TABLE client ADD COLUMN height_cm INTEGER")
		if not column_exists("client", "weight_kg"):
			conn.exec_driver_sql("ALTER TABLE client ADD COLUMN weight_kg INTEGER")

		# Order.data_date (DATE)
		if not column_exists("order", "data_date"):
			conn.exec_driver_sql('ALTER TABLE "order" ADD COLUMN data_date DATE')

		# ImportRun.data_date (DATE)
		if not column_exists("importrun", "data_date"):
			conn.exec_driver_sql("ALTER TABLE importrun ADD COLUMN data_date DATE")

		# OrderItem table (lightweight create-if-missing for SQLite)
		# Detect by presence of a known column on the table name
		try:
			rows = conn.exec_driver_sql("PRAGMA table_info('orderitem')").fetchall()
			orderitem_exists = any(rows)
		except Exception:
			orderitem_exists = False
		if not orderitem_exists:
			conn.exec_driver_sql(
				"""
				CREATE TABLE IF NOT EXISTS orderitem (
					id INTEGER PRIMARY KEY,
					order_id INTEGER,
					item_id INTEGER,
					quantity INTEGER DEFAULT 1,
					created_at DATETIME,
					FOREIGN KEY(order_id) REFERENCES "order"(id),
					FOREIGN KEY(item_id) REFERENCES item(id)
				)
				"""
			)

		# Inventory/Variant fields on item
		if not column_exists("item", "product_id"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN product_id INTEGER")
		if not column_exists("item", "size"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN size TEXT")
		if not column_exists("item", "color"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN color TEXT")
		if not column_exists("item", "pack_type"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN pack_type TEXT")
		if not column_exists("item", "pair_multiplier"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN pair_multiplier INTEGER DEFAULT 1")
		if not column_exists("item", "price"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN price REAL")
		if not column_exists("item", "status"):
			conn.exec_driver_sql("ALTER TABLE item ADD COLUMN status TEXT")

		# User table and columns (created by metadata, but ensure columns exist for old DBs)
		if not column_exists("user", "username"):
			# create table if absent by invoking metadata create again (safe) then fallback columns
			SQLModel.metadata.create_all(engine)
		if not column_exists("user", "password_hash"):
			conn.exec_driver_sql("ALTER TABLE user ADD COLUMN password_hash TEXT")
		if not column_exists("user", "role"):
			conn.exec_driver_sql("ALTER TABLE user ADD COLUMN role TEXT")
		if not column_exists("user", "failed_attempts"):
			conn.exec_driver_sql("ALTER TABLE user ADD COLUMN failed_attempts INTEGER DEFAULT 0")
		if not column_exists("user", "locked_until"):
			conn.exec_driver_sql("ALTER TABLE user ADD COLUMN locked_until DATETIME")

			# Client.status
			if not column_exists("client", "status"):
				conn.exec_driver_sql("ALTER TABLE client ADD COLUMN status TEXT")

			# Payment fee fields and net_amount
			for col, coltype in [
				("fee_komisyon", "REAL"),
				("fee_hizmet", "REAL"),
				("fee_kargo", "REAL"),
				("fee_iade", "REAL"),
				("fee_erken_odeme", "REAL"),
				("net_amount", "REAL"),
			]:
				if not column_exists("payment", col):
					conn.exec_driver_sql(f"ALTER TABLE payment ADD COLUMN {col} {coltype} DEFAULT 0")


@contextmanager
def get_session() -> Iterator[Session]:
	session = Session(engine)
	try:
		yield session
		session.commit()
	except Exception:
		session.rollback()
		raise
	finally:
		session.close()


def reset_db() -> None:
    """Reset DB but preserve users.

    Backs up rows from the `user` table (if it exists), recreates the DB,
    then restores the users to keep credentials intact.
    """
    # Backup existing users before dropping DB
    existing_users = []
    try:
        from .models import User  # local import to avoid circulars at module import time
        try:
            from sqlmodel import Session as _Session, select as _select
            with _Session(engine) as _sess:
                try:
                    rows = _sess.exec(_select(User)).all()
                    for u in rows:
                        existing_users.append({
                            "id": u.id,
                            "username": u.username,
                            "password_hash": u.password_hash,
                            "role": u.role,
                            "failed_attempts": u.failed_attempts,
                            "locked_until": u.locked_until,
                            "created_at": u.created_at,
                            "updated_at": u.updated_at,
                        })
                except Exception:
                    # table may not exist; ignore
                    pass
        except Exception:
            pass
    except Exception:
        # models import failed; proceed without backup
        pass

    engine.dispose()
    if DB_PATH.exists():
        DB_PATH.unlink()
    SQLModel.metadata.create_all(engine)

    # Restore users if any
    if existing_users:
        try:
            from .models import User  # re-import after re-create
            with Session(engine) as _sess:
                for data in existing_users:
                    try:
                        _sess.add(User(**data))
                    except Exception:
                        # If explicit id insertion fails, drop id and retry
                        data_no_id = dict(data)
                        data_no_id.pop("id", None)
                        _sess.add(User(**data_no_id))
                _sess.commit()
        except Exception:
            # If restore fails, continue with empty users rather than aborting reset
            pass
