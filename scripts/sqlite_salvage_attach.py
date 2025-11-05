import sqlite3
from pathlib import Path
from sqlmodel import SQLModel, create_engine

SRC = Path('/app/data/app.db')
DST = Path('/app/data/app.salv_attach.db')

# Known tables from app schema
TABLES = [
	"client","item","order","orderitem","payment","product",
	"importrun","importrow","stockmovement","user",
	"conversations","message","attachments","jobs","ig_accounts","ig_users","raw_events","ig_ai_run"
]

def safe_cols(conn: sqlite3.Connection, table: str) -> list[str]:
	try:
		rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
		return [r[1] for r in rows]
	except sqlite3.Error:
		return []


def main() -> None:
	# Initialize destination DB with fresh schema using a new engine (avoid global engine that points to corrupted DB)
	if DST.exists():
		DST.unlink()
	eng = create_engine(f"sqlite:///{DST}")
	SQLModel.metadata.create_all(eng)
	# Open connections
	dst = sqlite3.connect(str(DST))
	try:
		src = sqlite3.connect("file:/app/data/app.db?mode=ro", uri=True)
	except sqlite3.Error as e:
		print(f"[ERR] open src failed: {e}")
		return
	# Copy per table
	for t in TABLES:
		try:
			src_cols = safe_cols(src, t)
			dst_cols = safe_cols(dst, t)
		except Exception:
			src_cols, dst_cols = [], []
		common = [c for c in src_cols if c in dst_cols]
		if not common:
			print(f"[SKIP] {t}: no common columns")
			continue
		cols_sql = ", ".join(f'"{c}"' for c in common)
		try:
			rows = src.execute(f"SELECT {cols_sql} FROM " + '"' + t + '"').fetchall()
			dst.executemany(
				f"INSERT OR IGNORE INTO " + '"' + t + '"' + f" ({cols_sql}) VALUES({','.join(['?']*len(common))})",
				rows,
			)
			dst.commit()
			print(f"[OK] {t}: copied={len(rows)}")
		except sqlite3.Error as e:
			print(f"[ERR] {t}: {e}")
			continue
	print("copy done")

if __name__ == "__main__":
	main()


