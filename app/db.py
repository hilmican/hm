from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlmodel import SQLModel, create_engine, Session

DB_PATH = Path("data/app.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


def init_db() -> None:
	SQLModel.metadata.create_all(engine)


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
