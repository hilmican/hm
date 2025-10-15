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
	"""Dangerous: delete SQLite file and recreate empty schema."""
	engine.dispose()
	if DB_PATH.exists():
		DB_PATH.unlink()
	SQLModel.metadata.create_all(engine)
