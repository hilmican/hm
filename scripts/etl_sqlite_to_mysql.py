import argparse
import os
import sqlite3
from typing import List

from sqlmodel import SQLModel, create_engine
import app.models  # ensure models are imported so SQLModel.metadata is populated
from sqlalchemy import text


def get_tables(conn: sqlite3.Connection) -> List[str]:
	c = conn.cursor()
	rows = c.execute(
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	).fetchall()
	return [r[0] for r in rows]


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
	try:
		rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
		return [r[1] for r in rows]
	except Exception:
		return []


def main() -> None:
	parser = argparse.ArgumentParser(description="Migrate data from a SQLite DB into a MySQL DB using current SQLModel schema")
	parser.add_argument("--sqlite", required=True, help="Path to SQLite .db file")
	parser.add_argument("--mysql-url", required=True, help="SQLAlchemy MySQL URL, e.g., mysql+pymysql://user:pass@host:port/db?charset=utf8mb4")
	args = parser.parse_args()

	# Source (SQLite)
	src = sqlite3.connect(f"file:{args.sqlite}?mode=ro", uri=True)
	src.row_factory = sqlite3.Row

	# Destination (MySQL)
	engine = create_engine(args.mysql_url, pool_pre_ping=True)
	# Create tables per current models
	SQLModel.metadata.create_all(engine)

	# Disable FK checks during load
	with engine.begin() as conn:
		try:
			conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=0")
		except Exception:
			pass

		tables = get_tables(src)
		for t in tables:
			cols = get_columns(src, t)
			if not cols:
				print(f"[SKIP] {t}: no columns")
				continue
			col_list = ", ".join(f"`{c}`" for c in cols)
			placeholders = ", ".join(["%s"] * len(cols))
			# Fetch rows in batches to limit memory
			offset = 0
			batch = 1000
			total = 0
			while True:
				cols_sqlite = ", ".join(["\"" + c + "\"" for c in cols])
				rows = src.execute(f"SELECT {cols_sqlite} FROM '{t}' LIMIT ? OFFSET ?", (batch, offset)).fetchall()
				if not rows:
					break
				values = [tuple(r[c] for c in cols) for r in rows]
				try:
					conn.exec_driver_sql(
						f"INSERT IGNORE INTO `{t}` ({col_list}) VALUES ({placeholders})",
						values,
					)
					total += len(values)
				except Exception as e:
					print(f"[ERR] {t} offset {offset}: {e}")
					break
				offset += batch
			print(f"[OK] {t}: inserted {total}")

		try:
			conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=1")
		except Exception:
			pass

	print("done")


if __name__ == "__main__":
	main()


