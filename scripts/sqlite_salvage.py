import sqlite3
import os

SRC = "/app/data/app.db"
DST = "/app/data/app.salvage.db"


def log(msg: str) -> None:
	try:
		print(msg, flush=True)
	except Exception:
		pass


def get_tables(cur: sqlite3.Cursor):
	tabs = []
	for name, typ, sql in cur.execute(
		"SELECT name, type, sql FROM sqlite_master WHERE type IN (\"table\",\"view\") AND name NOT LIKE \"sqlite_%\";"
	).fetchall():
		if typ == "table":
			tabs.append((name, sql))
	return tabs


def ensure_schema(dst_conn: sqlite3.Connection, tables):
	dc = dst_conn.cursor()
	for name, sql in tables:
		try:
			if sql and isinstance(sql, str):
				dc.execute(sql)
		except Exception:
			try:
				dc.execute(f"CREATE TABLE IF NOT EXISTS \"{name}\"(_salvage_row BLOB)")
			except Exception:
				pass
	dst_conn.commit()


def columns(cur: sqlite3.Cursor, table: str):
	cols = []
	try:
		for cid, cname, typ, notnull, dflt, pk in cur.execute(f"PRAGMA table_info(\"{table}\")"):
			cols.append(cname)
	except Exception:
		pass
	return cols


def bounds(cur: sqlite3.Cursor, table: str):
	try:
		lo, hi = cur.execute(f"SELECT MIN(rowid), MAX(rowid) FROM \"{table}\"").fetchone()
		return lo, hi
	except Exception:
		return None, None


def quote_ident_list(cols):
	out = []
	for c in cols:
		c2 = c.replace('"', '""')
		out.append('"' + c2 + '"')
	return ",".join(out)


def copy_range(src_cur: sqlite3.Cursor, dst_cur: sqlite3.Cursor, table: str, col_list, lo, hi):
	copied = 0
	skipped = 0
	if lo is None or hi is None:
		return 0, 0

	def attempt(a, b):
		nonlocal copied, skipped
		if a is None or b is None or a > b:
			return
		try:
			qcols = quote_ident_list(col_list) if col_list else "*"
			rows = src_cur.execute(
				f"SELECT {qcols} FROM \"{table}\" WHERE rowid BETWEEN ? AND ?",
				(a, b),
			).fetchall()
			if rows:
				placeholders = ",".join(["?"] * (len(col_list) if col_list else len(rows[0])))
				if col_list:
					dst_cur.executemany(
						f"INSERT INTO \"{table}\" ({qcols}) VALUES({placeholders})",
						rows,
					)
				else:
					dst_cur.executemany(
						f"INSERT INTO \"{table}\" VALUES({placeholders})",
						rows,
					)
				copied += len(rows)
			return
		except Exception:
			if a == b:
				skipped += 1
				return
			mid = (a + b) // 2
			attempt(a, mid)
			attempt(mid + 1, b)

	attempt(int(lo), int(hi))
	return copied, skipped


def main() -> None:
	src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
	dst = sqlite3.connect(DST)
	src.row_factory = sqlite3.Row

	stabs = get_tables(src.cursor())
	log(f"[SALVAGE] found {len(stabs)} tables")
	ensure_schema(dst, stabs)

	print("TABLE\tCOPIED\tSKIPPED")
	for name, sql in stabs:
		scur = src.cursor()
		dcur = dst.cursor()
		cols = columns(scur, name)
		lo, hi = bounds(scur, name)
		if lo is None:
			print(f"{name}\t0\t0")
			continue
		c, s = copy_range(scur, dcur, name, cols, lo, hi)
		dst.commit()
		print(f"{name}\t{c}\t{s}")
		log(f"[SALVAGE] {name}: copied={c} skipped={s}")

	try:
		ss = os.stat(SRC).st_size
		ds = os.stat(DST).st_size
		log(f"[SALVAGE] sizes: src={ss} dst={ds}")
	except Exception:
		pass


if __name__ == "__main__":
	main()


