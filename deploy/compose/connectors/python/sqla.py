"""Connector check for SQLAlchemy 2 (Core and ORM) over PyMySQL or mysqlclient.

check.py runs this for SPRIG_CONNECTOR "sqlalchemy-pymysql" or
"sqlalchemy-mysqlclient". It prints one JSON line per check, like the driver
checks (../README.md). The URL's dialect follows the persona, as an
application would pick it: mysql+<driver>:// for the 3306 persona and
mariadb+<driver>:// for 3307. SQLAlchemy refuses mariadb:// against a server
that isn't MariaDB, and on mysql:// it decides MariaDB-or-not from the
version string, so the `connect` check asserts what it concluded.
"""

import datetime
import decimal
import json
import os
import sys
import time
import traceback

import sqlalchemy as sa
from sqlalchemy import event, inspect, select, text
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, selectinload

CONNECTOR = os.environ["SPRIG_CONNECTOR"]
DRIVER = CONNECTOR.split("-", 1)[1]  # pymysql | mysqlclient
TLS = os.environ["SPRIG_TLS"]
PERSONA = os.environ.get("SPRIG_PERSONA", "mariadb")
PREFIX = "t_%s_%s" % (CONNECTOR.replace("-", "_"), TLS)
SKIPPED = set(os.environ.get("SPRIG_CONN_SKIP_CHECKS", "").replace(",", " ").split())


class Skip(Exception):
    """The server can't show it; reported as skip."""


def emit(check, status, detail="", ms=0.0):
    rec = {"connector": CONNECTOR, "tls": TLS, "check": check, "status": status,
           "detail": str(detail)[:300], "ms": round(ms, 1)}
    print(json.dumps(rec, ensure_ascii=False), flush=True)


def expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


def url():
    scheme = "%s+%s" % ("mysql" if PERSONA == "mysql" else "mariadb", "pymysql" if DRIVER == "pymysql" else "mysqldb")
    return sa.engine.URL.create(scheme, username=os.environ["SPRIG_USER"], password=os.environ["SPRIG_PASSWORD"],
                                host=os.environ["SPRIG_HOST"], port=int(os.environ["SPRIG_PORT"]),
                                database=os.environ["SPRIG_DATABASE"], query={"charset": "utf8mb4"})


def connect_args():
    if DRIVER == "pymysql":
        return {"ssl": {"check_hostname": False}} if TLS == "required" else {"ssl_disabled": True}
    return {"ssl_mode": "REQUIRED" if TLS == "required" else "DISABLED"}


def make_engine(**kw):
    e = sa.create_engine(url(), connect_args=connect_args(), **kw)

    @event.listens_for(e, "connect")
    def _utc(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute("SET time_zone = '+00:00'")
        cur.close()

    return e


engine = None

# --- checks ------------------------------------------------------------------


def c_connect():
    global engine
    engine = make_engine(pool_size=2)
    with engine.connect() as c:
        cipher = c.execute(text("SHOW SESSION STATUS LIKE 'Ssl_cipher'")).one()[1]
    if TLS == "required":
        expect(cipher, "TLS required but the session isn't encrypted")
    else:
        expect(not cipher, "TLS disabled but the session is encrypted (%s)" % cipher)
    d = engine.dialect
    expect(d.is_mariadb == (PERSONA == "mariadb"),
           "dialect %s decided is_mariadb=%s for persona %s" % (d.name, d.is_mariadb, PERSONA))
    return "SQLAlchemy %s, dialect %s+%s, is_mariadb=%s, server %s, cipher=%s" % (
        sa.__version__, d.name, d.driver, d.is_mariadb, ".".join(map(str, d.server_version_info)), cipher or "none")


def c_core():
    md = sa.MetaData()
    t = sa.Table(
        PREFIX + "_core", md,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("i8", mysql.TINYINT), sa.Column("big", sa.BigInteger),
        sa.Column("ubig", mysql.BIGINT(unsigned=True)), sa.Column("dec", sa.Numeric(20, 6)),
        sa.Column("dbl", sa.Double), sa.Column("flag", sa.Boolean),
        sa.Column("s", sa.String(64)), sa.Column("txt", sa.Text), sa.Column("bin", sa.LargeBinary),
        sa.Column("d", sa.Date), sa.Column("dt", mysql.DATETIME(fsp=6)), sa.Column("tm", sa.Time),
        sa.Column("color", sa.Enum("red", "green", name="color")), sa.Column("doc", sa.JSON),
        mysql_charset="utf8mb4",
    )
    md.drop_all(engine)
    md.create_all(engine)
    row = dict(i8=-128, big=9223372036854775807, ubig=18446744073709551615, dec=decimal.Decimal("-12345678901234.567890"),
               dbl=-0.00225, flag=True, s="héllo wörld 🌱", txt="Ünïcödé 日本語 🚀 'q' \"d\" back\\slash",
               bin=bytes(range(256)), d=datetime.date(2024, 2, 29), dt=datetime.datetime(2024, 2, 29, 23, 59, 58, 123456),
               tm=datetime.time(12, 34, 56), color="green", doc={"a": 1, "b": [True, None], "s": "🌱"})
    nulls = {k: None for k in row}
    with engine.begin() as c:
        res = c.execute(t.insert(), [row, nulls])  # executemany
        expect(res.rowcount == 2, "inserted %s rows" % res.rowcount)
        got = c.execute(select(t).where(t.c.id == sa.bindparam("id")), {"id": 1}).mappings().one()
        bad = ["%s: got %r want %r" % (k, got[k], v) for k, v in row.items() if got[k] != v]
        expect(not bad, "; ".join(bad))
        got = c.execute(select(t).where(t.c.s.is_(None))).mappings().one()
        expect(all(got[k] is None for k in row), "NULL row: %r" % dict(got))
        c.execute(t.update().where(t.c.id == 1).values(s="updated", big=t.c.big - 1))
        expect(c.execute(select(t.c.s, t.c.big).where(t.c.id == 1)).one() == ("updated", 9223372036854775806), "update")
        n = c.execute(t.delete().where(t.c.id == 2)).rowcount
        expect(n == 1, "deleted %s" % n)
    md.drop_all(engine)
    return "15 types, executemany, update, delete"


class Base(DeclarativeBase):
    pass


class Author(Base):
    __tablename__ = PREFIX + "_author"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"}
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(64), unique=True)
    books: Mapped[list["Book"]] = relationship(back_populates="author", cascade="all, delete-orphan",
                                               order_by="Book.id")


class Book(Base):
    __tablename__ = PREFIX + "_book"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"}
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(sa.String(128))
    price: Mapped[decimal.Decimal] = mapped_column(sa.Numeric(10, 2))
    published: Mapped[datetime.datetime | None] = mapped_column(mysql.DATETIME(fsp=6))
    author_id: Mapped[int] = mapped_column(sa.ForeignKey(Author.id, ondelete="CASCADE"))
    author: Mapped[Author] = relationship(back_populates="books")


def c_orm():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        a = Author(name="Ünïcödé 🌱", books=[Book(title="one", price=decimal.Decimal("9.99")),
                                              Book(title="two", price=decimal.Decimal("0.01"),
                                                   published=datetime.datetime(2024, 2, 29, 1, 2, 3, 456789))])
        s.add(a)
        s.commit()
        aid = a.id
        expect(aid and all(b.id for b in a.books), "ids not assigned: %r" % [b.id for b in a.books])
    with Session(engine) as s:
        a = s.scalars(select(Author).options(selectinload(Author.books)).where(Author.id == aid)).one()
        expect([b.title for b in a.books] == ["one", "two"], "books %r" % [b.title for b in a.books])
        expect(a.books[1].published == datetime.datetime(2024, 2, 29, 1, 2, 3, 456789), "published %r" % a.books[1].published)
        a.books[0].price = decimal.Decimal("10.50")
        del a.books[1]
        s.commit()
    with Session(engine) as s:
        prices = s.scalars(select(Book.price).join(Book.author).where(Author.name == "Ünïcödé 🌱")).all()
        expect(prices == [decimal.Decimal("10.50")], "after update and orphan delete: %r" % prices)
        s.delete(s.get(Author, aid))
        s.commit()
        expect(s.scalar(select(sa.func.count()).select_from(Book)) == 0, "cascade left books")
    Base.metadata.drop_all(engine)
    return "insert graph, selectinload, update, delete-orphan, cascade"


def c_transaction():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(Author(name="rolled back"))
        s.flush()
        s.rollback()
        expect(s.scalar(select(sa.func.count()).select_from(Author)) == 0, "row survived rollback")
        s.add(Author(name="kept"))
        sp = s.begin_nested()  # SAVEPOINT
        s.add(Author(name="savepoint"))
        s.flush()
        sp.rollback()
        s.commit()
    with engine.connect() as c:  # another session
        names = c.execute(select(Author.name)).scalars().all()
    expect(names == ["kept"], "committed %r" % names)
    Base.metadata.drop_all(engine)
    return "rollback, SAVEPOINT rollback, commit"


def c_reflection():
    t = PREFIX + "_refl"
    p = PREFIX + "_refl_parent"
    with engine.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS %s, %s" % (t, p)))
    try:
        return reflect(t, p)
    finally:
        with engine.begin() as c:
            c.execute(text("DROP TABLE IF EXISTS %s, %s" % (t, p)))


def reflect(t, p):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE %s (id INT PRIMARY KEY) ENGINE = InnoDB" % p))
        c.execute(text(
            "CREATE TABLE %s ("
            " id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
            " code VARCHAR(32) NOT NULL COMMENT 'business key',"
            " amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,"
            " created DATETIME(6) NULL,"
            " doc JSON NULL,"
            " parent_id INT NULL,"
            " PRIMARY KEY (id),"
            " UNIQUE KEY uq_code (code),"
            " KEY ix_created (created),"
            " CONSTRAINT %s_fk FOREIGN KEY (parent_id) REFERENCES %s (id) ON DELETE SET NULL"
            ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT 'reflected'" % (t, t, p)))
    tbl = sa.Table(t, sa.MetaData(), autoload_with=engine)
    insp = inspect(engine)
    cols = {c["name"]: c for c in insp.get_columns(t)}
    types = {n: str(c["type"]) for n, c in cols.items()}
    expect(list(cols) == ["id", "code", "amount", "created", "doc", "parent_id"], "columns %r" % list(cols))
    expect(types["id"] == "BIGINT" and cols["id"]["type"].unsigned and types["code"] == "VARCHAR(32)"
           and types["amount"] == "DECIMAL(10, 2)" and types["created"] == "DATETIME"
           and cols["created"]["type"].fsp == 6 and types["parent_id"] == "INTEGER", "types %r" % types)
    expect(cols["code"]["comment"] == "business key" and cols["id"]["autoincrement"], "column details %r" % cols["code"])
    # MySQL has a JSON type; MariaDB's JSON is LONGTEXT with a json_valid() CHECK.
    expect(types["doc"] == "JSON" or (PERSONA == "mariadb" and types["doc"] == "LONGTEXT"), "doc reflected as %s" % types["doc"])
    pk = insp.get_pk_constraint(t)["constrained_columns"]
    uniques = [(u["name"], u["column_names"]) for u in insp.get_unique_constraints(t)]
    idx = {i["name"]: i["column_names"] for i in insp.get_indexes(t)}
    fks = insp.get_foreign_keys(t)
    expect(pk == ["id"], "pk %r" % pk)
    expect(("uq_code", ["code"]) in uniques, "uniques %r" % uniques)
    expect(idx.get("ix_created") == ["created"], "indexes %r" % idx)
    expect(len(fks) == 1 and fks[0]["referred_table"] == p and fks[0]["constrained_columns"] == ["parent_id"]
           and fks[0]["options"].get("ondelete") == "SET NULL", "foreign keys %r" % fks)
    expect(insp.get_table_comment(t)["text"] == "reflected", "table comment %r" % insp.get_table_comment(t))
    expect(t in insp.get_table_names(), "table not listed")
    with engine.begin() as c:  # the reflected Table works for DML
        c.execute(tbl.insert().values(code="a", doc=None))
        expect(c.execute(select(tbl.c.amount)).scalar_one() == decimal.Decimal("0.00"), "server default")
    return "6 columns, PK, unique, index, FK ON DELETE SET NULL, comments; doc as %s" % types["doc"]


def c_pool_reconnect():
    """pool_pre_ping: KILL the pooled connection, the next checkout must reconnect."""
    e = make_engine(pool_size=1, max_overflow=0, pool_pre_ping=True)
    try:
        with e.connect() as c:
            id1 = c.execute(text("SELECT CONNECTION_ID()")).scalar_one()
        with engine.connect() as k:
            k.execute(text("KILL %d" % id1))
            # Wait until the killed session has left the processlist (its socket is closed).
            for _ in range(100):
                if k.execute(text("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = %d" % id1)).scalar_one() == 0:
                    break
                time.sleep(0.05)
        time.sleep(0.1)
        with e.connect() as c:
            id2 = c.execute(text("SELECT CONNECTION_ID()")).scalar_one()
            expect(c.execute(text("SELECT :v + 1"), {"v": 41}).scalar_one() == 42, "query after reconnect")
        expect(id1 != id2, "the killed connection %s was handed out again" % id1)
        return "pool_pre_ping: connection %s killed, checkout reconnected as %s" % (id1, id2)
    finally:
        e.dispose()


CHECKS = [("connect", c_connect), ("core", c_core), ("orm", c_orm), ("transaction", c_transaction),
          ("reflection", c_reflection), ("pool_reconnect", c_pool_reconnect)]


def main():
    for name, fn in CHECKS:
        if name in SKIPPED:
            continue
        if engine is None and name != "connect":
            emit(name, "fail", "no connection")
            continue
        t0 = time.monotonic()
        try:
            emit(name, "pass", fn() or "", (time.monotonic() - t0) * 1000)
        except Skip as e:
            emit(name, "skip", str(e))
        except Exception as e:  # noqa: BLE001 - every failure is a result
            tb = traceback.format_exc().strip().splitlines()[-1]
            emit(name, "fail", "%s: %s | %s" % (type(e).__name__, e, tb), (time.monotonic() - t0) * 1000)
            if name == "connect":
                globals()["engine"] = None
    return 0


if __name__ == "__main__":
    sys.exit(main())
