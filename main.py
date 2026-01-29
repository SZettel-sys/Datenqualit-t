import os
import re
import json
import csv
import io
import html
import uuid
import urllib.parse
import httpx
import time
import time
import asyncpg
import unicodedata
from typing import Any, Optional

import zipfile
import xml.etree.ElementTree as ET
from fastapi import FastAPI, Request, Body, UploadFile, File, Query, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime, timezone, timedelta

app = FastAPI()

def _html_error_page(title: str, message: str, detail: str = "") -> str:
    detail_html = f"<pre style='white-space:pre-wrap;background:#0b1220;color:#e2e8f0;padding:14px;border-radius:12px;overflow:auto'>{html.escape(detail)}</pre>" if detail else ""
    return page_shell(title, f"""
      <div class='card'>
        <h2 style='margin:0 0 10px'>{html.escape(message)}</h2>
        <p style='margin:0 0 12px; color:rgba(15,23,42,.75)'>Bitte prüfe die Server-Logs. Unten sind Details für die Fehlersuche.</p>
        {detail_html}
        <div style='margin-top:14px'>
          <a class='chip chip-primary' href='/overview'>Zur Übersicht</a>
        </div>
      </div>
    """)

@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    # In PROD keep it short; if DEBUG=1, include traceback.
    debug = os.getenv("DEBUG", "").strip() in ("1", "true", "True", "yes", "YES")
    tb = ""
    if debug:
        import traceback as _tb
        tb = "".join(_tb.format_exception(type(exc), exc, exc.__traceback__))
    else:
        tb = f"{type(exc).__name__}: {exc}"
    return HTMLResponse(_html_error_page("Interner Fehler", f"{type(exc).__name__}", tb), status_code=500)


########################################################################
#
# Konfiguration - allgemein
#
########################################################################

CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_V2_URL = "https://api.pipedrive.com/api/v2"

def _normalize_pipedrive_app_url(url: str) -> str:
    """Normalize Pipedrive app base URL.

    Some environments provide a sandbox subdomain (e.g. '<company>-sandbox.pipedrive.com').
    We always want the non-sandbox host in the generated record links.

    This function is intentionally defensive: it strips trailing slashes and accidental path parts
    if the value is provided without a scheme.
    """
    try:
        from urllib.parse import urlparse, urlunparse

        raw = (url or "").strip()
        if not raw:
            return "https://app.pipedrive.com"

        u = urlparse(raw)

        scheme = u.scheme or "https"
        netloc = u.netloc or u.path  # allow passing host without scheme
        netloc = netloc.strip().strip("/")

        # If someone passed a host + path without scheme (e.g. 'foo.pipedrive.com/person'),
        # keep only the host part.
        if "/" in netloc:
            netloc = netloc.split("/", 1)[0]

        # strip possible credentials/port handling
        parts = netloc.split("@")
        hostport = parts[-1]
        userinfo = "@".join(parts[:-1])

        host, sep, port = hostport.partition(":")

        # remove sandbox markers robustly (handles '-sandbox' and '.sandbox' variants)
        host = host.replace("-sandbox", "")
        host = host.replace(".sandbox", "")
        # extra hardening: handle 'sandbox.' prefix or 'sandbox-' prefix
        host = re.sub(r"^sandbox[\.-]", "", host)
        host = re.sub(r"[\.-]sandbox(?=\.)", "", host)


        hostport2 = host + (sep + port if sep else "")
        netloc2 = (userinfo + "@" if userinfo else "") + hostport2
        return urlunparse((scheme, netloc2, "", "", "", ""))
    except Exception:
        return url

PIPEDRIVE_APP_URL = _normalize_pipedrive_app_url(os.getenv("PIPEDRIVE_APP_URL", "https://app.pipedrive.com"))

def pipedrive_person_url(person_id: int) -> str:
    return f"{PIPEDRIVE_APP_URL}/person/{int(person_id)}"

def pipedrive_org_url(org_id: int) -> str:
    return f"{PIPEDRIVE_APP_URL}/organization/{int(org_id)}"

user_tokens: dict[str, str] = {}

########################################################################
#
# DB - Anbindung
#
########################################################################
DB_URL = os.getenv("DATABASE_URL")
db_pool: Optional[asyncpg.Pool] = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None


async def init_db():
    if not db_pool:
        raise RuntimeError("db_pool ist nicht initialisiert")

    async with db_pool.acquire() as conn:
        # Tabellen anlegen
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS persons_cache (
          id BIGINT PRIMARY KEY,
          first_name TEXT,
          last_name TEXT,
          gender TEXT,
          email TEXT,
          du_sie TEXT,
          position TEXT,
          linkedin_url TEXT,
          org_id BIGINT,
          update_time TIMESTAMPTZ,
          label_ids BIGINT[]
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS orgs_cache (
          id BIGINT PRIMARY KEY,
          name TEXT,
          address TEXT,
          website TEXT,
          update_time TIMESTAMPTZ
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
          entity TEXT PRIMARY KEY,
          last_update_time TIMESTAMPTZ NOT NULL,
          last_cursor TEXT,
          full_in_progress BOOLEAN NOT NULL DEFAULT FALSE
        );
        """)

        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS bulk_staging (
          token TEXT PRIMARY KEY,
          entity_type TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL,
          rows JSONB NOT NULL
        );
        """)
# Schema-Migration (bestehende DBs)
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS first_name TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS last_name TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS gender TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS email TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS du_sie TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS position TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS linkedin_url TEXT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS org_id BIGINT;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS update_time TIMESTAMPTZ;")
        await conn.execute("ALTER TABLE persons_cache ADD COLUMN IF NOT EXISTS label_ids BIGINT[];")

        await conn.execute("ALTER TABLE orgs_cache ADD COLUMN IF NOT EXISTS name TEXT;")
        await conn.execute("ALTER TABLE orgs_cache ADD COLUMN IF NOT EXISTS address TEXT;")
        await conn.execute("ALTER TABLE orgs_cache ADD COLUMN IF NOT EXISTS website TEXT;")
        await conn.execute("ALTER TABLE orgs_cache ADD COLUMN IF NOT EXISTS update_time TIMESTAMPTZ;")

        await conn.execute("ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS last_cursor TEXT;")
        await conn.execute("ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS full_in_progress BOOLEAN NOT NULL DEFAULT FALSE;")

        # Indizes
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_first_name
        ON persons_cache (id)
        WHERE (first_name IS NULL OR btrim(first_name) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_last_name
        ON persons_cache (id)
        WHERE (last_name IS NULL OR btrim(last_name) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_email
        ON persons_cache (id)
        WHERE (email IS NULL OR btrim(email) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_gender
        ON persons_cache (id)
        WHERE (gender IS NULL OR btrim(gender) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_du_sie
        ON persons_cache (id)
        WHERE (du_sie IS NULL OR btrim(du_sie) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_position
        ON persons_cache (id)
        WHERE (position IS NULL OR btrim(position) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_linkedin
        ON persons_cache (id)
        WHERE (linkedin_url IS NULL OR btrim(linkedin_url) = '');
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_org_id
        ON persons_cache (org_id);
        """)
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_orgs_name
        ON orgs_cache (name);
        """)


async def get_sync_time(entity: str) -> datetime:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT last_update_time FROM sync_state WHERE entity=$1", entity)
        if row and row["last_update_time"]:
            return row["last_update_time"]
        t0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
        await conn.execute(
            "INSERT INTO sync_state(entity, last_update_time) VALUES($1, $2) ON CONFLICT(entity) DO NOTHING",
            entity, t0
        )
        return t0


async def set_sync_time(entity: str, t: datetime):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sync_state(entity, last_update_time)
            VALUES($1, $2)
            ON CONFLICT(entity) DO UPDATE SET last_update_time=EXCLUDED.last_update_time
        """, entity, t)


async def get_sync_cursor(entity: str) -> tuple[Optional[str], bool]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT last_cursor, full_in_progress FROM sync_state WHERE entity=$1", entity)
        if not row:
            return None, False
        return row["last_cursor"], bool(row["full_in_progress"])


async def set_sync_cursor(entity: str, cursor: Optional[str], in_progress: bool):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sync_state(entity, last_update_time, last_cursor, full_in_progress)
            VALUES($1, $2, $3, $4)
            ON CONFLICT(entity) DO UPDATE SET
              last_cursor = EXCLUDED.last_cursor,
              full_in_progress = EXCLUDED.full_in_progress
        """, entity, datetime(1970, 1, 1, tzinfo=timezone.utc), cursor, in_progress)


@app.on_event("startup")
async def _startup():
    global db_pool
    if DB_URL:
        db_pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
        await init_db()

########################################################################
#
# Static
#
########################################################################
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

########################################################################
#
# Root
#
########################################################################
@app.get("/")
def root():
    return RedirectResponse("/overview")

########################################################################
#
# LogIn / OAuth
#
########################################################################

@app.get("/login")
def login():
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    )


@app.get("/oauth/callback")
async def oauth_callback(code: str):
    async with httpx.AsyncClient(timeout=30.0) as client:
        token_resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return HTMLResponse(f"<h3>❌ Fehler beim Login: {token_data}</h3>")
    user_tokens["default"] = access_token
    return RedirectResponse("/overview")


@app.get("/logout")
def logout():
    user_tokens.pop("default", None)
    return RedirectResponse("/overview")


def get_headers() -> dict:
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}


def html_escape(s: str) -> str:
    s = s or ""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

########################################################################
#
# Konstanten / Field-Keys / Name-Validation
#
########################################################################

CSS_VERSION = "6"  # hochzählen bei CSS/HTML Änderungen
FREELANCER_ORG_NAME = "Freelancer"

# Kontakt-Feldkeys (Custom Fields)
PD_PERSON_GENDER_KEY = "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72"
PD_PERSON_DU_SIE_KEY = "1fde2275ff2973c9062d64f1612122384b5902cf"
PD_PERSON_POSITION_KEY = "4585e5de11068a3bccf02d8b93c126bcf5c257ff"
PD_PERSON_LINKEDIN_KEY = "25563b12f847a280346bba40deaf527af82038cc"

# Titel-Erkennung im Vornamen
TITLE_PREFIX_REGEX = re.compile(
    r"^\s*(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|herr|frau)\b",
    re.IGNORECASE,
)

# Erlaubte Sonderzeichen in Namen: Leerzeichen, Bindestrich, Punkt, Apostroph (gerade/typografisch)
# Akzente sind über Unicode-Letter/Mark abgedeckt.
NAME_ALLOWED_PUNCT = set([
    " ", "-", "‐", "‑", "–", "—",
    ".", "'", "’", "ʼ", "´", "`"
])



# Postgres allowed-name regex (used for fast COUNT queries on /overview)
# Hinweis: Die Detail-Listen (invalidchars) nutzen weiterhin die Python-Unicode-Validierung,
# weil Postgres-RegEx/Collation je nach Setup leicht abweichen kann.
PG_NAME_ALLOWED_PATTERN = r"^[[:alpha:][:space:]\.\'’‘´`ʼ\-‐‑‒–—−]+$"




def _csv_text(data: str, filename: str) -> Response:
    """Helfer: liefert CSV als Download."""
    return Response(
        content=data,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _parse_ids_param(ids: str) -> list[int]:
    out: list[int] = []
    for part in (ids or "").split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    # dedupe, keep order
    seen: set[int] = set()
    uniq: list[int] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


async def db_fetch_persons_bulk(ids: list[int]) -> dict[int, dict]:
    """Liefert Personen-Datensätze aus dem Cache als Mapping {id: row_dict}."""
    if not ids:
        return {}
    sql = """
    SELECT
      p.id, p.first_name, p.last_name, p.gender, p.email, p.du_sie, p.position, p.linkedin_url,
      p.org_id,
      COALESCE(o.name, '') AS org_name
    FROM persons_cache p
    LEFT JOIN orgs_cache o ON o.id = p.org_id
    WHERE p.id = ANY($1::BIGINT[])
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, ids)
    return {int(r["id"]): dict(r) for r in rows}


async def db_fetch_orgs_bulk(ids: list[int]) -> dict[int, dict]:
    if not ids:
        return {}
    sql = """
    SELECT id, name, address, website
    FROM orgs_cache
    WHERE id = ANY($1::BIGINT[])
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, ids)
    return {int(r["id"]): dict(r) for r in rows}


@app.get("/dq/bulk/csv")
async def dq_bulk_csv_export(entity: str = Query(...), ids: str = Query(...)):
    """
    CSV-Export für ausgewählte Datensätze.

    entity:
      - person
      - organization
    ids: comma separated
    """
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return JSONResponse({"ok": False, "error": "DB nicht initialisiert"}, status_code=500)

    entity = (entity or "").strip().lower()
    id_list = _parse_ids_param(ids)
    if entity not in ("person", "organization"):
        return JSONResponse({"ok": False, "error": "entity muss 'person' oder 'organization' sein"}, status_code=400)
    if not id_list:
        return JSONResponse({"ok": False, "error": "ids leer"}, status_code=400)

    sio = io.StringIO()
    writer = csv.writer(sio, delimiter=";")

    if entity == "person":
        rows_by_id = await db_fetch_persons_bulk(id_list)
        writer.writerow(["id","first_name","last_name","email","gender","du_sie","position","linkedin_url","org_name"])
        for pid in id_list:
            r = rows_by_id.get(pid) or {}
            writer.writerow([
                pid,
                (r.get("first_name") or ""),
                (r.get("last_name") or ""),
                (r.get("email") or ""),
                (r.get("gender") or ""),
                (r.get("du_sie") or ""),
                (r.get("position") or ""),
                (r.get("linkedin_url") or ""),
                (r.get("org_name") or ""),
            ])
        return _csv_text(sio.getvalue(), f"bulk_persons_{len(id_list)}.csv")

    rows_by_id = await db_fetch_orgs_bulk(id_list)
    writer.writerow(["id", "name", "address", "website"])
    for oid in id_list:
        r = rows_by_id.get(oid) or {}
        writer.writerow([
            oid,
            (r.get("name") or ""),
            (r.get("address") or ""),
            (r.get("website") or ""),
        ])
    return _csv_text(sio.getvalue(), f"bulk_orgs_{len(id_list)}.csv")


@app.get("/dq/bulk/xlsx/selected")
async def dq_bulk_xlsx_export_selected(entity: str = Query(...), ids: str = Query(...), field_key: str = Query("")):
    """
    Excel-Export (XLSX) für ausgewählte Datensätze.

    entity:
      - person
      - organization
    ids: comma separated
    """
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return JSONResponse({"ok": False, "error": "DB nicht initialisiert"}, status_code=500)

    entity = (entity or "").strip().lower()
    id_list = _parse_ids_param(ids)
    if entity not in ("person", "organization"):
        return JSONResponse({"ok": False, "error": "entity muss person|organization sein"}, status_code=400)
    if not id_list:
        return JSONResponse({"ok": False, "error": "ids leer"}, status_code=400)

    if entity == "person":
        rows_by_id = await db_fetch_persons_bulk(id_list)
        base_headers = ["id", "first_name", "last_name", "org_id"]
        fk = (field_key or "").strip()
        headers = base_headers.copy()
        if fk and fk not in headers:
            headers.append(fk)
        data_rows: list[list[Any]] = []
        for pid in id_list:
            r = rows_by_id.get(pid) or {}
            row = [
                pid,
                (r.get("first_name") or ""),
                (r.get("last_name") or ""),
                (r.get("org_id") or ""),
            ]
            if fk and fk not in base_headers:
                row.append((r.get(fk) or ""))
            data_rows.append(row)
        filename = f"bulk_persons_{len(id_list)}.xlsx"
    else:
        rows_by_id = await db_fetch_orgs_bulk(id_list)
        headers = ["id", "name", "address", "website"]
        data_rows = []
        for oid in id_list:
            r = rows_by_id.get(oid) or {}
            data_rows.append([
                oid,
                (r.get("name") or ""),
                (r.get("address") or ""),
                (r.get("website") or ""),
            ])
        filename = f"bulk_orgs_{len(id_list)}.xlsx"

    xlsx_bytes = _make_simple_xlsx_bytes(headers, data_rows)
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



def _cell_value(raw: Any) -> tuple[bool, Optional[str]]:
    """
    Excel Import Semantik:
    - leere Zelle => (False, None) = "nicht anfassen"
    - '__CLEAR__' => (True, None)  = "Feld leeren"
    - sonst => (True, value)
    """
    if raw is None:
        return False, None
    s = str(raw).strip()
    if s == "":
        return False, None
    if s.upper() == "__CLEAR__":
        return True, None
    return True, s


# ---------------------------------------------------------------------
# Minimal XLSX writer/reader (ohne openpyxl)
# ---------------------------------------------------------------------

_XLSX_NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_XLSX_NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

def _col_letter(idx0: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA ..."""
    n = idx0 + 1
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _col_index(col_letters: str) -> int:
    """A -> 0, Z -> 25, AA -> 26"""
    col_letters = (col_letters or "").upper()
    n = 0
    for ch in col_letters:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - 64)
    return max(0, n - 1)

def _xlsx_make_sheet_xml(headers: list[str], rows: list[list[Any]]) -> bytes:
    import xml.etree.ElementTree as _ET

    ns = _XLSX_NS_MAIN
    _ET.register_namespace("", ns)

    ws = _ET.Element(f"{{{ns}}}worksheet")
    sheet_data = _ET.SubElement(ws, f"{{{ns}}}sheetData")

    def add_row(r_idx: int, values: list[Any]):
        row_el = _ET.SubElement(sheet_data, f"{{{ns}}}row", {"r": str(r_idx)})
        for c_idx, v in enumerate(values):
            col = _col_letter(c_idx)
            cell_ref = f"{col}{r_idx}"
            if v is None:
                continue
            sv = str(v)
            if sv == "":
                continue

            # Zahlen als Zahl, sonst inline string (kein sharedStrings nötig)
            if isinstance(v, (int, float)) or (isinstance(v, str) and re.fullmatch(r"-?\d+(\.\d+)?", sv)):
                c_el = _ET.SubElement(row_el, f"{{{ns}}}c", {"r": cell_ref})
                v_el = _ET.SubElement(c_el, f"{{{ns}}}v")
                v_el.text = sv
            else:
                c_el = _ET.SubElement(row_el, f"{{{ns}}}c", {"r": cell_ref, "t": "inlineStr"})
                is_el = _ET.SubElement(c_el, f"{{{ns}}}is")
                t_el = _ET.SubElement(is_el, f"{{{ns}}}t")
                # preserve leading/trailing spaces
                if sv.startswith(" ") or sv.endswith(" "):
                    t_el.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
                t_el.text = sv

    # header row
    add_row(1, headers)
    # data rows
    for i, r in enumerate(rows, start=2):
        add_row(i, r)

    return _ET.tostring(ws, encoding="utf-8", xml_declaration=True)

def _make_simple_xlsx_bytes(headers: list[str], rows: list[list[Any]]) -> bytes:
    """Erzeugt ein simples XLSX mit einem Sheet (sheet1)."""
    from io import BytesIO

    # Core XML parts
    content_types = f'''<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>
'''
    rels = f'''<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
'''
    wb = f'''<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="{_XLSX_NS_MAIN}" xmlns:r="{_XLSX_NS_REL}">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
'''
    wb_rels = f'''<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>
'''
    sheet1 = _xlsx_make_sheet_xml(headers, rows)

    bio = BytesIO()
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("xl/workbook.xml", wb)
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels)
        z.writestr("xl/worksheets/sheet1.xml", sheet1)
    return bio.getvalue()

def _read_xlsx_first_sheet(file_bytes: bytes) -> list[list[str]]:
    """Liest sheet1 aus einem XLSX (Zip+XML). Gibt Zeilen als Listen zurück."""
    from io import BytesIO

    with zipfile.ZipFile(BytesIO(file_bytes), "r") as z:
        # shared strings optional
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            sroot = ET.fromstring(z.read("xl/sharedStrings.xml"))
            ns = {"m": _XLSX_NS_MAIN}
            for si in sroot.findall("m:si", ns):
                t = si.find("m:t", ns)
                if t is not None and t.text is not None:
                    shared.append(t.text)
                else:
                    parts = []
                    for rt in si.findall(".//m:t", ns):
                        if rt.text:
                            parts.append(rt.text)
                    shared.append("".join(parts))

        sheet_path = "xl/worksheets/sheet1.xml"
        if sheet_path not in z.namelist():
            ws = [p for p in z.namelist() if p.startswith("xl/worksheets/") and p.endswith(".xml")]
            if not ws:
                return []
            sheet_path = ws[0]

        root = ET.fromstring(z.read(sheet_path))
        ns = {"m": _XLSX_NS_MAIN}

        rows_map: dict[int, dict[int, str]] = {}

        for row_el in root.findall(".//m:sheetData/m:row", ns):
            r_attr = row_el.get("r")
            try:
                r_idx = int(r_attr) if r_attr else None
            except Exception:
                r_idx = None
            if r_idx is None:
                continue

            cols: dict[int, str] = {}
            for c in row_el.findall("m:c", ns):
                ref = c.get("r") or ""
                mref = re.match(r"^([A-Za-z]+)(\d+)$", ref)
                if not mref:
                    continue
                c_idx = _col_index(mref.group(1))

                t = c.get("t")
                val = ""
                if t == "inlineStr":
                    t_el = c.find("m:is/m:t", ns)
                    if t_el is not None and t_el.text is not None:
                        val = t_el.text
                    else:
                        parts = []
                        for rt in c.findall(".//m:is//m:t", ns):
                            if rt.text:
                                parts.append(rt.text)
                        val = "".join(parts)
                else:
                    v_el = c.find("m:v", ns)
                    if v_el is not None and v_el.text is not None:
                        raw = v_el.text
                        if t == "s":
                            try:
                                val = shared[int(raw)]
                            except Exception:
                                val = raw
                        else:
                            val = raw

                cols[c_idx] = (val or "")

            if cols:
                rows_map[r_idx] = cols

        if not rows_map:
            return []

        max_row = max(rows_map.keys())
        max_col = max((max(cols.keys()) for cols in rows_map.values()), default=-1)

        out: list[list[str]] = []
        for r in range(1, max_row + 1):
            cols = rows_map.get(r) or {}
            row_vals = []
            for c in range(0, max_col + 1):
                row_vals.append((cols.get(c) or "").strip())
            out.append(row_vals)

        while out and all(x == "" for x in out[-1]):
            out.pop()

        return out

def _sniff_delimiter(sample: str) -> str:
    for delim in [";", ",", "\t"]:
        if sample.count(delim) >= 2:
            return delim
    return

@app.get("/dq/bulk/xlsx")
async def dq_bulk_xlsx_export(mode: str = "contacts"):
    """
    Exportiere Bulk-Excel:
      - mode=contacts      -> Kontakte ohne Freelancer
      - mode=freelancers   -> nur Freelancer
      - mode=organizations -> Organisationen
    """
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return JSONResponse({"ok": False, "error": "DB nicht initialisiert"}, status_code=500)

    mode = (mode or "contacts").strip().lower()
    if mode not in ("contacts", "freelancers", "organizations"):
        return JSONResponse({"ok": False, "error": "mode muss contacts|freelancers|organizations sein"}, status_code=400)

    async with db_pool.acquire() as conn:
        if mode == "organizations":
            rows = await conn.fetch(
                "SELECT id, name, address, website FROM orgs_cache ORDER BY id"
            )
            headers = ["id", "name", "address", "website"]
            data_rows = [[r["id"], r["name"], r["address"], r["website"]] for r in rows]
            filename = "bulk_organizations.xlsx"

        else:
            base_sql = """
            SELECT
              p.id, p.first_name, p.last_name, p.gender, p.email, p.du_sie, p.position, p.linkedin_url,
              COALESCE(o.name, '') AS org_name
            FROM persons_cache p
            LEFT JOIN orgs_cache o ON o.id = p.org_id
            WHERE 1=1
            """

            if mode == "contacts":
                base_sql += """ AND (o.name IS NULL OR lower(o.name) <> lower($1)) """
                rows = await conn.fetch(base_sql + " ORDER BY p.id", FREELANCER_ORG_NAME)
                filename = "bulk_contacts.xlsx"
            else:
                base_sql += """ AND (o.name IS NOT NULL AND lower(o.name) = lower($1)) """
                rows = await conn.fetch(base_sql + " ORDER BY p.id", FREELANCER_ORG_NAME)
                filename = "bulk_freelancers.xlsx"

            headers = [
                "id",
                "first_name",
                "last_name",
                "gender",
                "email",
                "du_sie",
                "position",
                "linkedin_url",
                "org_name",
            ]
            data_rows = [
                [
                    r["id"],
                    r["first_name"],
                    r["last_name"],
                    r["gender"],
                    r["email"],
                    r["du_sie"],
                    r["position"],
                    r["linkedin_url"],
                    r["org_name"],
                ]
                for r in rows
            ]

    xlsx_bytes = _make_simple_xlsx_bytes(headers, data_rows)

    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
@app.post("/dq/bulk/csv/import")
async def dq_bulk_csv_import(entity: str = Query(...), file: UploadFile = File(...)):
    """
    CSV-Import: patcht Datensätze in Pipedrive und zieht Cache nach.

    WICHTIG:
    - Leere Zellen bedeuten: Feld NICHT ändern.
    - '__CLEAR__' bedeutet: Feld in Pipedrive löschen/leeren.
    """
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)
    if not db_pool:
        return JSONResponse({"ok": False, "error": "DB nicht initialisiert"}, status_code=500)

    entity = (entity or "").strip().lower()
    if entity not in ("person", "organization"):
        return JSONResponse({"ok": False, "error": "entity muss 'person' oder 'organization' sein"}, status_code=400)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    raw = await file.read()
    text_data: Optional[str] = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text_data = raw.decode(enc)
            break
        except Exception:
            continue
    if text_data is None:
        return JSONResponse({"ok": False, "error": "CSV konnte nicht dekodiert werden"}, status_code=400)

    sample = text_data[:4096]
    delim = _sniff_delimiter(sample)
    rdr = csv.DictReader(io.StringIO(text_data), delimiter=delim)

    sem = asyncio.Semaphore(5)

    results: dict[str, Any] = {"updated": 0, "skipped": 0, "failed": 0, "errors": []}

    async def handle_row(row: dict):
        try:
            if not row:
                return

            rid_raw = str(row.get("id") or "").strip()
            if not rid_raw.isdigit():
                results["skipped"] += 1
                return
            rid = int(rid_raw)

            if entity == "person":
                patch: dict[str, Any] = {}
                cf: dict[str, Any] = {}

                do, v = _cell_value(row.get("first_name"))
                if do:
                    patch["first_name"] = v

                do, v = _cell_value(row.get("last_name"))
                if do:
                    patch["last_name"] = v

                do, v = _cell_value(row.get("email"))
                if do:
                    patch["emails"] = ([{"label": "work", "value": v, "primary": True}] if v else [])

                do, v = _cell_value(row.get("gender_id"))
                if do:
                    cf[PD_PERSON_GENDER_KEY] = (int(v) if (v and str(v).isdigit()) else None)

                do, v = _cell_value(row.get("du_sie_id"))
                if do:
                    cf[PD_PERSON_DU_SIE_KEY] = (int(v) if (v and str(v).isdigit()) else None)

                do, v = _cell_value(row.get("position"))
                if do:
                    cf[PD_PERSON_POSITION_KEY] = v

                do, v = _cell_value(row.get("linkedin_url"))
                if do:
                    cf[PD_PERSON_LINKEDIN_KEY] = v

                if cf:
                    patch["custom_fields"] = cf

                if not patch:
                    results["skipped"] += 1
                    return

                async with sem:
                    await pipedrive_patch_v2("persons", rid, patch, headers)

                await db_upsert_person_cache_partial(
                    rid,
                    first_name=patch.get("first_name") if "first_name" in patch else None,
                    last_name=patch.get("last_name") if "last_name" in patch else None,
                    email=(patch.get("emails")[0].get("value") if patch.get("emails") else "") if "emails" in patch else None,
                    gender=(str(cf.get(PD_PERSON_GENDER_KEY)) if (PD_PERSON_GENDER_KEY in cf and cf.get(PD_PERSON_GENDER_KEY) is not None) else ("") if (PD_PERSON_GENDER_KEY in cf) else None),
                    du_sie=(str(cf.get(PD_PERSON_DU_SIE_KEY)) if (PD_PERSON_DU_SIE_KEY in cf and cf.get(PD_PERSON_DU_SIE_KEY) is not None) else ("") if (PD_PERSON_DU_SIE_KEY in cf) else None),
                    position=(cf.get(PD_PERSON_POSITION_KEY) if PD_PERSON_POSITION_KEY in cf else None),
                    linkedin_url=(cf.get(PD_PERSON_LINKEDIN_KEY) if PD_PERSON_LINKEDIN_KEY in cf else None),
                )

                results["updated"] += 1
                return

            # organization
            patch: dict[str, Any] = {}

            do, v = _cell_value(row.get("name"))
            if do:
                patch["name"] = v
            do, v = _cell_value(row.get("address"))
            if do:
                patch["address"] = v
            do, v = _cell_value(row.get("website"))
            if do:
                patch["website"] = v

            if not patch:
                results["skipped"] += 1
                return

            async with sem:
                await pipedrive_patch_v2("organizations", rid, patch, headers)

            await db_upsert_org_cache_partial(
                rid,
                name=patch.get("name") if "name" in patch else None,
                address=patch.get("address") if "address" in patch else None,
                website=patch.get("website") if "website" in patch else None,
            )
            results["updated"] += 1

        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"id": row.get("id"), "error": str(e)})

    tasks = [handle_row(r) for r in rdr]
    if tasks:
        await asyncio.gather(*tasks)

    return JSONResponse({"ok": True, "result": results})



def _xlsx_cell_to_str(v: Any) -> str:
    if v is None:
        return ""
    # Excel kann Zahlen als float liefern
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v)
    return str(v).strip()


def _parse_xlsx_upload(file_bytes: bytes) -> tuple[list[str], list[dict]]:
    """
    Liest das erste Sheet aus einer .xlsx und gibt (headers, rows) zurück.
    Implementierung ohne openpyxl (XLSX = Zip + XML).

    - Header = erste Zeile
    - Leere Zeilen werden übersprungen
    """
    grid = _read_xlsx_first_sheet(file_bytes)
    if not grid:
        return [], []

    header_row = grid[0]
    headers = [(_xlsx_cell_to_str(h) or "").strip() for h in header_row]
    norm_headers = [h.strip() for h in headers]

    out_rows: list[dict] = []
    for row in grid[1:]:
        vals = [_xlsx_cell_to_str(v) for v in row]
        if all((v.strip() == "" for v in vals)):
            continue
        d: dict[str, str] = {}
        for i, h in enumerate(norm_headers):
            if not h:
                continue
            d[h] = vals[i] if i < len(vals) else ""
        out_rows.append(d)

    return headers, out_rows


async def _bulk_stage_save(token: str, entity: str, rows: list[dict]) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bulk_staging(token, entity_type, created_at, rows)
            VALUES($1, $2, $3, $4::jsonb)
            ON CONFLICT(token) DO UPDATE SET
              entity_type = EXCLUDED.entity_type,
              created_at  = EXCLUDED.created_at,
              rows        = EXCLUDED.rows
            """,
            token,
            entity,
            _utcnow(),
            json.dumps(rows, ensure_ascii=False),
        )


async def _bulk_stage_load(token: str) -> tuple[Optional[str], list[dict]]:
    if not db_pool:
        return None, []
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT entity_type, rows FROM bulk_staging WHERE token=$1", token)
    if not row:
        return None, []
    entity = row["entity_type"]
    rows_json = row["rows"]
    # asyncpg kann jsonb als dict/list liefern oder als str (je nach config)
    if isinstance(rows_json, str):
        rows = json.loads(rows_json) if rows_json else []
    else:
        rows = rows_json or []
    if not isinstance(rows, list):
        rows = []
    return entity, rows


async def _bulk_stage_delete(token: str) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM bulk_staging WHERE token=$1", token)


@app.post("/dq/bulk/xlsx/preview", response_class=HTMLResponse)
async def dq_bulk_xlsx_preview(entity: str = Form(...), xlsx_file: UploadFile = File(...)):
    """
    Upload einer Excel-Datei -> Vorschau (OHNE Update nach Pipedrive).
    Erst im 2. Schritt (Apply) werden Änderungen gesendet.
    """
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    entity = (entity or "").strip().lower()
    if entity not in ("person", "organization"):
        return HTMLResponse("Ungültige entity (person|organization)", status_code=400)

    content = await xlsx_file.read()
    headers, rows = _parse_xlsx_upload(content)

    if not headers or not rows:
        body = """
        <div class="topbar">
          <div>
            <div class="title">Excel Import – Vorschau</div>
            <div class="subtitle">Keine Daten gefunden (Header fehlt oder Datei leer).</div>
          </div>
          <div style="display:flex; gap:10px;">
            <a class="btn btn-outline" href="/dq/bulk">← Zurück</a>
          </div>
        </div>
        """
        return HTMLResponse(page_shell("Excel Import – Vorschau", body))

    # Erwartete Spalten
    if entity == "person":
        expected = {"id", "first_name", "last_name", "email", "gender_id", "du_sie_id", "position", "linkedin_url"}
    else:
        expected = {"id", "name", "address", "website"}

    missing_cols = [c for c in ("id",) if c not in [h.strip() for h in headers]]
    if missing_cols:
        body = f"""
        <div class="topbar">
          <div>
            <div class="title">Excel Import – Vorschau</div>
            <div class="subtitle">Fehlende Pflicht-Spalten: <b>{html_escape(", ".join(missing_cols))}</b></div>
          </div>
          <div style="display:flex; gap:10px;">
            <a class="btn btn-outline" href="/dq/bulk">← Zurück</a>
          </div>
        </div>
        """
        return HTMLResponse(page_shell("Excel Import – Vorschau", body))

    # Rows normalisieren: nur bekannte Keys behalten
    cleaned: list[dict] = []
    preview_rows = []
    for idx, r in enumerate(rows):
        rr = {k.strip(): (r.get(k, "") if isinstance(r, dict) else "") for k in expected}
        # id normalisieren
        pid_raw = (rr.get("id") or "").strip()
        ok = pid_raw.isdigit()
        err = "" if ok else "ID fehlt/ungültig"
        rr["_ok"] = ok
        rr["_err"] = err
        rr["_idx"] = idx
        cleaned.append(rr)
        preview_rows.append(rr)

    token = str(uuid.uuid4())
    await _bulk_stage_save(token, entity, cleaned)

    # Preview Table
    ths = ["✓", "Status"] + [h for h in cleaned[0].keys() if not h.startswith("_")]
    # Der key-order oben kommt aus expected; wir bauen manuell:
    if entity == "person":
        col_order = ["id", "first_name", "last_name", "email", "gender_id", "du_sie_id", "position", "linkedin_url"]
    else:
        col_order = ["id", "name", "address", "website"]

    # Back-Link für Detailseiten (Pagination + Filter erhalten)
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    trs = []
    for r in preview_rows[:500]:
        ok = bool(r.get("_ok"))
        err = r.get("_err") or ""
        idx = int(r.get("_idx"))
        chk = f'<input type="checkbox" class="bulk-check" data-idx="{idx}" {"checked" if ok else ""} {"disabled" if not ok else ""}/>'
        status = "OK" if ok else f"Fehler: {html_escape(err)}"
        tds = [f"<td>{chk}</td>", f"<td>{status}</td>"]
        for c in col_order:
            tds.append(f"<td>{html_escape((r.get(c) or '').strip())}</td>")
        trs.append("<tr>" + "".join(tds) + "</tr>")

    note_clear = """
    <div class="small" style="margin-top:10px;">
      Hinweis: Leere Zellen bedeuten <b>keine Änderung</b>. Wenn du ein Feld in Pipedrive aktiv leeren willst,
      trage in Excel den Wert <code>__CLEAR__</code> ein.
    </div>
    """

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Excel Import – Vorschau</div>
          <div class="subtitle">Es wird <b>nichts</b> automatisch nach Pipedrive geschrieben. Bitte wähle die Zeilen aus und klicke anschließend auf „Änderungen anwenden“.</div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/dq/bulk">← Zurück</a>
          <button class="btn btn-primary" onclick="applyBulkXlsx('{token}')">Änderungen anwenden</button>
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:60px;">✓</th>
              <th style="width:220px;">Status</th>
              {''.join(f'<th>{html_escape(c)}</th>' for c in col_order)}
            </tr>
          </thead>
          <tbody>
            {''.join(trs)}
          </tbody>
        </table>
        {note_clear}
      </div>

      <script>
        async function applyBulkXlsx(token) {{
          if(!confirm("Wirklich ausgewählte Zeilen nach Pipedrive übernehmen?")) return;

          const checks = Array.from(document.querySelectorAll(".bulk-check"));
          const selected = checks.filter(c => c.checked && !c.disabled).map(c => parseInt(c.dataset.idx));

          const res = await fetch("/dq/bulk/xlsx/apply", {{
            method: "POST",
            headers: {{"Content-Type":"application/json"}},
            body: JSON.stringify({{ token, selected }})
          }});

          let data = null;
          try {{ data = await res.json(); }} catch(e) {{}}

          if(res.ok && data && data.ok) {{
            alert("✅ Fertig. Erfolgreich: " + data.applied + " · Fehler: " + data.failed);
            window.location.href = "/dq/bulk";
          }} else {{
            alert("❌ Fehler: " + ((data && data.error) ? data.error : ("HTTP " + res.status)));
          }}
        }}
      </script>
    """
    return HTMLResponse(page_shell("Excel Import – Vorschau", body))


@app.post("/dq/bulk/xlsx/apply")
async def dq_bulk_xlsx_apply(payload: dict = Body(...)):
    """
    Apply staged Excel-Änderungen nach Pipedrive (NUR nach explizitem Klick).
    payload: { token: "...", selected: [rowIndex,...] }
    """
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    token = (payload.get("token") or "").strip()
    selected = payload.get("selected") or []
    if not token:
        return JSONResponse({"ok": False, "error": "token fehlt"}, status_code=400)
    if not isinstance(selected, list) or not all(isinstance(x, int) for x in selected):
        return JSONResponse({"ok": False, "error": "selected muss List[int] sein"}, status_code=400)

    entity, rows = await _bulk_stage_load(token)
    if not entity or not rows:
        return JSONResponse({"ok": False, "error": "Staging nicht gefunden/leer (token abgelaufen?)"}, status_code=404)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    selected_set = set(selected)
    applied = 0
    failed = 0
    errors: list[str] = []

    # Apply in der Reihenfolge der Datei
    for r in rows:
        try:
            idx = int(r.get("_idx", -1))
        except Exception:
            idx = -1
        if idx not in selected_set:
            continue

        pid_raw = (r.get("id") or "").strip()
        if not pid_raw.isdigit():
            failed += 1
            continue

        entity_id = int(pid_raw)

        try:
            if entity == "person":
                patch: dict = {}
                cf: dict = {}

                def _maybe_set_root(key: str, col: str):
                    v = (r.get(col) or "").strip()
                    if v == "":
                        return
                    if v == "__CLEAR__":
                        patch[key] = None
                    else:
                        patch[key] = v

                _maybe_set_root("first_name", "first_name")
                _maybe_set_root("last_name", "last_name")

                # email -> emails array
                email_v = (r.get("email") or "").strip()
                if email_v != "":
                    if email_v == "__CLEAR__":
                        patch["emails"] = []
                    else:
                        patch["emails"] = [{"label": "work", "value": email_v, "primary": True}]

                # custom fields
                gender_v = (r.get("gender_id") or "").strip()
                if gender_v != "":
                    if gender_v == "__CLEAR__":
                        cf[PD_PERSON_GENDER_KEY] = None
                    elif gender_v.isdigit():
                        cf[PD_PERSON_GENDER_KEY] = int(gender_v)

                dusie_v = (r.get("du_sie_id") or "").strip()
                if dusie_v != "":
                    if dusie_v == "__CLEAR__":
                        cf[PD_PERSON_DU_SIE_KEY] = None
                    elif dusie_v.isdigit():
                        cf[PD_PERSON_DU_SIE_KEY] = int(dusie_v)

                pos_v = (r.get("position") or "").strip()
                if pos_v != "":
                    cf[PD_PERSON_POSITION_KEY] = None if pos_v == "__CLEAR__" else pos_v

                li_v = (r.get("linkedin_url") or "").strip()
                if li_v != "":
                    cf[PD_PERSON_LINKEDIN_KEY] = None if li_v == "__CLEAR__" else li_v

                if cf:
                    patch["custom_fields"] = cf

                if patch:
                    await pipedrive_patch_v2("persons", entity_id, patch, headers)
                    await refresh_person_cache_from_api(entity_id, headers)
                    applied += 1

            else:
                patch: dict = {}
                for col in ("name", "address", "website"):
                    v = (r.get(col) or "").strip()
                    if v == "":
                        continue
                    if v == "__CLEAR__":
                        patch[col] = None
                    else:
                        patch[col] = v

                if patch:
                    await pipedrive_patch_v2("organizations", entity_id, patch, headers)
                    await refresh_org_cache_from_api(entity_id, headers)
                    applied += 1

        except Exception as e:
            failed += 1
            errors.append(f"ID {entity_id}: {str(e)}")

    # Staging löschen (damit kein Re-Apply aus Versehen)
    await _bulk_stage_delete(token)

    return JSONResponse({
        "ok": True,
        "entity": entity,
        "applied": applied,
        "failed": failed,
        "errors": errors[:20],
    })

def _freelancer_filter_sql_alias(mode: str, alias: str, param_no: int) -> str:
    """
    SQL-Fragment für Freelancer-Filterung über orgs_cache.
    param_no ist die $-Parameter-Nummer für FREELANCER_ORG_NAME.
    """
    if mode == "all":
        return ""
    if mode == "only":
        return f"""
          AND EXISTS (
            SELECT 1 FROM orgs_cache o
            WHERE o.id = {alias}.org_id
              AND lower(o.name) = lower(${param_no})
          )
        """
    if mode == "exclude":
        return f"""
          AND NOT EXISTS (
            SELECT 1 FROM orgs_cache o
            WHERE o.id = {alias}.org_id
              AND lower(o.name) = lower(${param_no})
          )
        """
    return ""


async def _db_count(sql: str, *params) -> Optional[int]:
    if not db_pool:
        return None
    async with db_pool.acquire() as conn:
        v = await conn.fetchval(sql, *params)
    try:
        return int(v)
    except Exception:
        return None


async def db_count_persons_missing(col: str, freelancer_mode: str = "exclude") -> Optional[int]:
    # col ist trusted (wir rufen das nur intern mit festen Spalten auf)
    base = f"""
      SELECT COUNT(*) FROM persons_cache p
      WHERE ({col} IS NULL OR btrim({col}) = '')
    """
    params: list[Any] = []
    flt = _freelancer_filter_sql_alias(freelancer_mode, "p", 1)
    if flt.strip():
        params.append(FREELANCER_ORG_NAME)
    sql = base + flt
    return await _db_count(sql, *params)


async def db_count_persons_invalid(col: str, freelancer_mode: str = "exclude") -> Optional[int]:
    # Schnellzählung per Postgres-RegEx (für Overview). Detail-Listen filtern Python-seitig.
    params: list[Any] = [PG_NAME_ALLOWED_PATTERN]
    flt = _freelancer_filter_sql_alias(freelancer_mode, "p", 2)
    if flt.strip():
        params.append(FREELANCER_ORG_NAME)

    sql = f"""
      SELECT COUNT(*) FROM persons_cache p
      WHERE {col} IS NOT NULL
        AND btrim({col}) <> ''
        AND {col} !~ $1
      {flt}
    """
    return await _db_count(sql, *params)


async def db_count_persons_title_in_firstname(freelancer_mode: str = "exclude") -> Optional[int]:
    # Titel am Anfang (dr, prof, herr, frau ...)
    pattern = r"^\s*(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|herr|frau)(\s|\.|$)"
    params: list[Any] = [pattern]
    flt = _freelancer_filter_sql_alias(freelancer_mode, "p", 2)
    if flt.strip():
        params.append(FREELANCER_ORG_NAME)

    sql = f"""
      SELECT COUNT(*) FROM persons_cache p
      WHERE p.first_name IS NOT NULL
        AND btrim(p.first_name) <> ''
        AND p.first_name ~* $1
      {flt}
    """
    return await _db_count(sql, *params)


async def db_count_persons_without_org(freelancer_mode: str = "exclude") -> Optional[int]:
    flt = _freelancer_filter_sql_alias(freelancer_mode, "p", 1)
    params = []
    if flt.strip():
        params.append(FREELANCER_ORG_NAME)
    sql = f"""
      SELECT COUNT(*) FROM persons_cache p
      WHERE p.org_id IS NULL
      {flt}
    """
    return await _db_count(sql, *params)


async def db_count_orgs_without_contacts() -> Optional[int]:
    sql = """
      SELECT COUNT(*)
      FROM orgs_cache o
      WHERE NOT EXISTS (SELECT 1 FROM persons_cache p WHERE p.org_id = o.id)
    """
    return await _db_count(sql)


async def db_count_orgs_missing(field: str) -> Optional[int]:
    if field not in ("name", "address", "website"):
        return None
    sql = f"""
      SELECT COUNT(*) FROM orgs_cache
      WHERE ({field} IS NULL OR btrim({field}) = '')
    """
    return await _db_count(sql)


async def db_count_orgs_invalid_name() -> Optional[int]:
    # Organisationsnamen: etwas großzügiger (zusätzlich &, /, (), +, ,)
    pattern = r"^[[:alnum:][:space:]\.\,\&\+\/\-\(\)\'’‘´`ʼ]+$"
    sql = """
      SELECT COUNT(*) FROM orgs_cache
      WHERE name IS NOT NULL
        AND btrim(name) <> ''
        AND name !~ $1
    """
    return await _db_count(sql, pattern)


async def compute_overview_counts() -> dict[str, Optional[int]]:
    """
    Liefert eine Map: href -> count (oder None, falls DB nicht verfügbar).
    """
    counts: dict[str, Optional[int]] = {}

    # Kontakte (ohne Freelancer)
    counts["/dq/contacts/first_name/missing"] = await db_count_persons_missing("first_name", "exclude")
    counts["/dq/contacts/first_name/invalidchars"] = await db_count_persons_invalid("first_name", "exclude")
    counts["/dq/contacts/first_name/title"] = await db_count_persons_title_in_firstname("exclude")

    counts["/dq/contacts/last_name/missing"] = await db_count_persons_missing("last_name", "exclude")
    counts["/dq/contacts/last_name/invalidchars"] = await db_count_persons_invalid("last_name", "exclude")

    counts["/dq/contacts/gender/missing"] = await db_count_persons_missing("gender", "exclude")
    counts["/dq/contacts/email/missing"] = await db_count_persons_missing("email", "exclude")
    counts["/dq/contacts/du_sie/missing"] = await db_count_persons_missing("du_sie", "exclude")
    counts["/dq/contacts/position/missing"] = await db_count_persons_missing("position", "exclude")
    counts["/dq/contacts/linkedin/missing"] = await db_count_persons_missing("linkedin_url", "exclude")
    counts["/dq/contacts/org/missing"] = await db_count_persons_without_org("exclude")
    counts["/dq/contacts/email/mismatch"] = await db_count_email_mismatch()

    # Freelancer (nur Organisation = Freelancer)
    counts["/dq/freelancers/first_name/missing"] = await db_count_persons_missing("first_name", "only")
    counts["/dq/freelancers/last_name/missing"] = await db_count_persons_missing("last_name", "only")
    counts["/dq/freelancers/gender/missing"] = await db_count_persons_missing("gender", "only")
    counts["/dq/freelancers/email/missing"] = await db_count_persons_missing("email", "only")
    counts["/dq/freelancers/du_sie/missing"] = await db_count_persons_missing("du_sie", "only")
    counts["/dq/freelancers/position/missing"] = await db_count_persons_missing("position", "only")
    counts["/dq/freelancers/linkedin/missing"] = await db_count_persons_missing("linkedin_url", "only")

    # Organisationen
    counts["/dq/orgs/missing?field=name"] = await db_count_orgs_missing("name")
    counts["/dq/orgs/missing?field=address"] = await db_count_orgs_missing("address")
    counts["/dq/orgs/missing?field=website"] = await db_count_orgs_missing("website")
    counts["/dq/orgs/no_contacts"] = await db_count_orgs_without_contacts()
    counts["/dq/orgs/invalidchars?field=name"] = await db_count_orgs_invalid_name()

    return counts
def _normalize_name(s: str) -> str:
    # NFKC räumt z.B. Fullwidth-Varianten auf; trim.
    return unicodedata.normalize("NFKC", (s or "")).strip()


def _is_allowed_name_char(ch: str) -> bool:
    if ch in NAME_ALLOWED_PUNCT:
        return True
    if ch.isspace():
        return True
    cat = unicodedata.category(ch)  # e.g. 'Lu', 'Ll', 'Mn', 'So'
    if cat and cat[0] in ("L", "M"):  # Letter or Mark (combining accents)
        return True
    return False


def _has_invalid_name_chars(text: str) -> bool:
    t = _normalize_name(text)
    if not t:
        return False
    for ch in t:
        if ch.isdigit():
            return True
        # Disallow controls/format chars explicitly (zero-width etc.)
        cat = unicodedata.category(ch)
        if cat and cat[0] == "C":
            return True
        if not _is_allowed_name_char(ch):
            return True
    return False

########################################################################
#
# Pipedrive Helpers (v2)
#
########################################################################

def extract_address(address_value):
    """API v2 liefert 'address' als Objekt; wir wollen für die UI einen String."""
    if isinstance(address_value, dict):
        return address_value.get("value") or ""
    return address_value or ""


def _scalarize(v: Any) -> str:
    """Versucht, den Wert UI-tauglich als String zu machen (inkl. v2 list/dict)."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        if "value" in v and isinstance(v.get("value"), str):
            return v.get("value") or ""
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        vals = []
        for item in v:
            if isinstance(item, dict) and "value" in item:
                vals.append(str(item.get("value") or "").strip())
            else:
                vals.append(str(item).strip())
        vals = [x for x in vals if x]
        return ", ".join(vals)
    return str(v)


def _primary_from_list(items: Any) -> str:
    if not isinstance(items, list) or not items:
        return ""
    for it in items:
        if isinstance(it, dict) and it.get("primary") and it.get("value"):
            return str(it.get("value") or "").strip()
    for it in items:
        if isinstance(it, dict) and it.get("value"):
            return str(it.get("value") or "").strip()
    return ""


def _email_primary_from_person(p: dict) -> str:
    items = p.get("emails")
    if not items:
        items = p.get("email")
    primary = _primary_from_list(items)
    if primary:
        return primary
    s = _scalarize(items).strip()
    if not s:
        return ""
    return s.split(",")[0].strip()


def _label_ids_list(p: dict) -> list[int]:
    v = p.get("label_ids")
    if not v:
        return []
    if isinstance(v, list):
        out = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out
    return []


def _get_org_id_from_person(p: dict) -> Optional[int]:
    org_id = p.get("org_id") or p.get("organization_id")
    if isinstance(org_id, dict):
        org_id = org_id.get("value") or org_id.get("id")
    try:
        return int(org_id) if org_id is not None else None
    except Exception:
        return None


def _get_custom_field_value(entity: dict, key: str) -> Any:
    if not entity or not key:
        return None
    cf = entity.get("custom_fields")
    if isinstance(cf, dict) and key in cf:
        return cf.get(key)
    return entity.get(key)


def _as_option_id_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        if v.get("id") is not None:
            return str(v.get("id")).strip()
        if v.get("value") is not None:
            return str(v.get("value")).strip()
    return str(v).strip()


async def pipedrive_patch_v2(entity: str, entity_id: int, payload: dict, headers: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.patch(
            f"{PIPEDRIVE_API_V2_URL}/{entity}/{entity_id}",
            headers=headers,
            json=payload,
        )
        if r.status_code in (200, 201):
            return r.json()
        raise RuntimeError(f"Update fehlgeschlagen ({r.status_code}): {r.text}")


async def pipedrive_delete_v2(entity: str, entity_id: int, headers: dict) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.delete(
            f"{PIPEDRIVE_API_V2_URL}/{entity}/{entity_id}",
            headers=headers,
        )
    if r.status_code in (200, 204):
        return
    raise RuntimeError(f"Delete fehlgeschlagen ({r.status_code}): {r.text}")


def normalize_update_payload_v2(entity_type: str, field_key: str, value: str) -> dict:
    """
    Baut ein v2-konformes PATCH-Payload-Fragment für persons & organizations.
    """
    v = (value or "").strip()
    et = (entity_type or "").strip().lower()

    if et == "organization":
        if field_key in ("name", "website"):
            return {field_key: (v if v else None)}
        if field_key == "address":
            return {"address": (v if v else None)}
        return {field_key: (v if v else None)}

    # persons
    if field_key == "emails":
        return {"emails": ([{"value": v, "primary": True}] if v else [])}

    if field_key == "label_ids":
        if not v:
            return {"label_ids": []}
        ids: list[int] = []
        for part in v.split(","):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))
        return {"label_ids": ids}

    SINGLE_OPTION_CUSTOM_FIELDS = {
        PD_PERSON_GENDER_KEY,
        PD_PERSON_DU_SIE_KEY,
    }
    if field_key in SINGLE_OPTION_CUSTOM_FIELDS:
        if not v:
            cf_val = None
        elif v.isdigit():
            cf_val = int(v)
        else:
            cf_val = None
        return {"custom_fields": {field_key: cf_val}}

    if field_key in {
        PD_PERSON_POSITION_KEY,
        PD_PERSON_LINKEDIN_KEY,
    }:
        return {"custom_fields": {field_key: (v if v else None)}}

    return {field_key: (v if v else None)}

########################################################################
#
# Cache: Upserts
#
########################################################################

async def upsert_persons(batch: list[dict]) -> Optional[datetime]:
    if not batch:
        return None

    rows: list[tuple] = []
    max_ts: Optional[datetime] = None

    for p in batch:
        pid = p.get("id")
        if pid is None:
            continue

        ts = _parse_ts(p.get("update_time") or p.get("updated_at") or p.get("updateTime"))
        if ts and (max_ts is None or ts > max_ts):
            max_ts = ts

        email_primary = _email_primary_from_person(p)

        gender_raw = _get_custom_field_value(p, PD_PERSON_GENDER_KEY)
        dusie_raw = _get_custom_field_value(p, PD_PERSON_DU_SIE_KEY)
        position_raw = _get_custom_field_value(p, PD_PERSON_POSITION_KEY)
        linkedin_raw = _get_custom_field_value(p, PD_PERSON_LINKEDIN_KEY)

        rows.append((
            int(pid),
            _scalarize(p.get("first_name")).strip(),
            _scalarize(p.get("last_name")).strip(),
            _as_option_id_str(gender_raw),
            email_primary,
            _as_option_id_str(dusie_raw),
            _scalarize(position_raw).strip(),
            _scalarize(linkedin_raw).strip(),
            _get_org_id_from_person(p),
            ts,
            _label_ids_list(p),
        ))

    if not rows:
        return max_ts

    sql = """
    INSERT INTO persons_cache
      (id, first_name, last_name, gender, email, du_sie, position, linkedin_url, org_id, update_time, label_ids)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (id) DO UPDATE SET
      first_name   = EXCLUDED.first_name,
      last_name    = EXCLUDED.last_name,
      gender       = EXCLUDED.gender,
      email        = EXCLUDED.email,
      du_sie       = EXCLUDED.du_sie,
      position     = EXCLUDED.position,
      linkedin_url = EXCLUDED.linkedin_url,
      org_id       = EXCLUDED.org_id,
      update_time  = COALESCE(EXCLUDED.update_time, persons_cache.update_time),
      label_ids    = EXCLUDED.label_ids
    """
    async with db_pool.acquire() as conn:
        await conn.executemany(sql, rows)

    return max_ts


async def upsert_orgs(batch: list[dict]) -> Optional[datetime]:
    if not batch:
        return None

    rows: list[tuple] = []
    max_ts: Optional[datetime] = None

    for o in batch:
        oid = o.get("id")
        if oid is None:
            continue

        ts = _parse_ts(o.get("update_time") or o.get("updated_at") or o.get("updateTime"))
        if ts and (max_ts is None or ts > max_ts):
            max_ts = ts

        addr = extract_address(o.get("address"))
        web = _scalarize(o.get("website")).strip()

        rows.append((
            int(oid),
            _scalarize(o.get("name")).strip(),
            addr.strip(),
            web,
            ts
        ))

    if not rows:
        return max_ts

    sql = """
    INSERT INTO orgs_cache (id, name, address, website, update_time)
    VALUES ($1,$2,$3,$4,$5)
    ON CONFLICT (id) DO UPDATE SET
      name        = EXCLUDED.name,
      address     = EXCLUDED.address,
      website     = EXCLUDED.website,
      update_time = COALESCE(EXCLUDED.update_time, orgs_cache.update_time)
    """
    async with db_pool.acquire() as conn:
        await conn.executemany(sql, rows)

    return max_ts


async def db_upsert_org_cache_partial(
    org_id: int,
    *,
    name: Optional[str] = None,
    address: Optional[str] = None,
    website: Optional[str] = None,
):
    if not db_pool:
        return

    now = _utcnow()

    sql = """
    INSERT INTO orgs_cache (id, name, address, website, update_time)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (id) DO UPDATE SET
      name        = COALESCE(EXCLUDED.name, orgs_cache.name),
      address     = COALESCE(EXCLUDED.address, orgs_cache.address),
      website     = COALESCE(EXCLUDED.website, orgs_cache.website),
      update_time = EXCLUDED.update_time
    """
    async with db_pool.acquire() as conn:
        await conn.execute(sql, org_id, name, address, website, now)

########################################################################
#
# Sync Jobs (v2)
#
########################################################################

async def sync_persons_incremental(full: bool = False, max_pages: int = 20) -> dict:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    custom_keys = [
        PD_PERSON_GENDER_KEY,
        PD_PERSON_DU_SIE_KEY,
        PD_PERSON_POSITION_KEY,
        PD_PERSON_LINKEDIN_KEY,
    ]

    params: dict[str, Any] = {
        "limit": 500,
        "custom_fields": ",".join(custom_keys),
    }

    cursor: Optional[str] = None
    if full:
        cursor, in_progress = await get_sync_cursor("persons")
        if not in_progress:
            await set_sync_cursor("persons", None, True)
            cursor = None
    else:
        since = await get_sync_time("persons") - timedelta(minutes=2)
        params["updated_since"] = since.isoformat()

    max_seen: Optional[datetime] = None
    total = 0
    pages = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor

            r = await client.get(f"{PIPEDRIVE_API_V2_URL}/persons", headers=headers, params=p)
            if r.status_code != 200:
                raise RuntimeError(f"Pipedrive API Fehler ({r.status_code}): {r.text}")

            payload = r.json() or {}
            items = payload.get("data") or []
            add = payload.get("additional_data") or {}
            next_cursor = add.get("next_cursor")

            if items:
                total += len(items)
                ts = await upsert_persons(items)
                if ts and (max_seen is None or ts > max_seen):
                    max_seen = ts

            cursor = next_cursor
            pages += 1

            if full:
                await set_sync_cursor("persons", cursor, True)

            if max_pages and pages >= max_pages:
                break
            if not cursor:
                break

    if max_seen:
        await set_sync_time("persons", max_seen)

    if full and not cursor:
        await set_sync_cursor("persons", None, False)

    return {
        "entity": "persons",
        "full": full,
        "max_pages": max_pages,
        "processed": total,
        "pages": pages,
        "cursor_remaining": bool(cursor),
        "new_sync_time": max_seen.isoformat() if max_seen else None,
    }


async def sync_orgs_incremental(full: bool = False, max_pages: int = 20) -> dict:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    params: dict[str, Any] = {"limit": 500}

    cursor: Optional[str] = None
    if full:
        cursor, in_progress = await get_sync_cursor("organizations")
        if not in_progress:
            await set_sync_cursor("organizations", None, True)
            cursor = None
    else:
        since = await get_sync_time("organizations") - timedelta(minutes=2)
        params["updated_since"] = since.isoformat()

    max_seen: Optional[datetime] = None
    total = 0
    pages = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor

            r = await client.get(f"{PIPEDRIVE_API_V2_URL}/organizations", headers=headers, params=p)
            if r.status_code != 200:
                raise RuntimeError(f"Pipedrive API Fehler ({r.status_code}): {r.text}")

            payload = r.json() or {}
            items = payload.get("data") or []
            add = payload.get("additional_data") or {}
            next_cursor = add.get("next_cursor")

            if items:
                total += len(items)
                ts = await upsert_orgs(items)
                if ts and (max_seen is None or ts > max_seen):
                    max_seen = ts

            cursor = next_cursor
            pages += 1

            if full:
                await set_sync_cursor("organizations", cursor, True)

            if max_pages and pages >= max_pages:
                break
            if not cursor:
                break

    if max_seen:
        await set_sync_time("organizations", max_seen)

    if full and not cursor:
        await set_sync_cursor("organizations", None, False)

    return {
        "entity": "organizations",
        "full": full,
        "max_pages": max_pages,
        "processed": total,
        "pages": pages,
        "cursor_remaining": bool(cursor),
        "new_sync_time": max_seen.isoformat() if max_seen else None,
    }

########################################################################
#
# Detail Refresh (Cache nachziehen)
#
########################################################################

async def pipedrive_get_person_v2(person_id: int, headers: dict) -> dict:
    custom_keys = ",".join([
        PD_PERSON_GENDER_KEY,
        PD_PERSON_DU_SIE_KEY,
        PD_PERSON_POSITION_KEY,
        PD_PERSON_LINKEDIN_KEY,
    ])
    params = {"custom_fields": custom_keys}

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(
            f"{PIPEDRIVE_API_V2_URL}/persons/{person_id}",
            headers=headers,
            params=params,
        )
    if r.status_code != 200:
        raise RuntimeError(f"Person GET fehlgeschlagen ({r.status_code}): {r.text}")
    return (r.json() or {}).get("data") or {}


async def refresh_person_cache_from_api(person_id: int, headers: dict) -> None:
    if not db_pool:
        return
    try:
        p = await pipedrive_get_person_v2(person_id, headers)
        if p:
            await upsert_persons([p])
    except Exception:
        return


async def pipedrive_get_org_v2(org_id: int, headers: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{PIPEDRIVE_API_V2_URL}/organizations/{org_id}", headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Organization GET fehlgeschlagen ({r.status_code}): {r.text}")
    return (r.json() or {}).get("data") or {}


async def refresh_org_cache_from_api(org_id: int, headers: dict) -> None:
    if not db_pool:
        return
    try:
        o = await pipedrive_get_org_v2(org_id, headers)
        if o:
            await upsert_orgs([o])
    except Exception:
        return

########################################################################
#
# Person Field Options (Dropdown) – via v1 /personFields (robust)
#
########################################################################

_PERSON_FIELDS_OPTIONS_BY_KEY: Optional[dict[str, list[tuple[str, str]]]] = None


async def _load_person_fields_options_v1(headers: dict) -> dict[str, list[tuple[str, str]]]:
    global _PERSON_FIELDS_OPTIONS_BY_KEY
    if _PERSON_FIELDS_OPTIONS_BY_KEY is not None:
        return _PERSON_FIELDS_OPTIONS_BY_KEY

    url = "https://api.pipedrive.com/v1/personFields"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers=headers)

    if r.status_code != 200:
        _PERSON_FIELDS_OPTIONS_BY_KEY = {}
        return _PERSON_FIELDS_OPTIONS_BY_KEY

    data = (r.json() or {}).get("data") or []
    mapping: dict[str, list[tuple[str, str]]] = {}

    for f in data:
        key = f.get("key")
        if not key:
            continue
        opts = f.get("options") or []
        out: list[tuple[str, str]] = []
        for o in opts:
            if not isinstance(o, dict):
                continue
            oid = o.get("id")
            lab = o.get("label") or o.get("name") or ""
            if oid is None:
                continue
            out.append((str(oid), str(lab)))
        mapping[str(key)] = out

    _PERSON_FIELDS_OPTIONS_BY_KEY = mapping
    return mapping


async def get_person_field_options(headers: dict, field_key: str) -> list[tuple[str, str]]:
    mapping = await _load_person_fields_options_v1(headers)
    return mapping.get(field_key, [])

########################################################################
#
# HTML Helper
#
########################################################################

def page_shell(title: str, body_html: str, back_href: str = "/overview") -> str:
    logo_html = ""
    if os.path.isfile("static/bizforward-Logo-Clean-2024.svg"):
        logo_html = '<header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>'
    else:
        logo_html = '<header><div style="font-weight:900;letter-spacing:.2px">bizforward · Datenqualität</div></header>'

    
    backbar_html = ""
    if back_href:
        backbar_html = f'''
        <div class="backbar">
          <a class="btn btn-outline btn-inline" href="{html_escape(back_href)}">← Zurück</a>
          <a class="btn btn-outline btn-inline" href="/overview">Übersicht</a>
        </div>
        '''
    html = """
    <html>
    <head>
      <meta charset="utf-8"/>
      <meta name="viewport" content="width=device-width, initial-scale=1"/>
      <title>__TITLE__</title>
      <link rel="stylesheet" href="/static/app.css?v=__CSS_VERSION__">
      <style>
        .backbar{display:flex; justify-content:flex-end; margin:10px 0 18px; gap:10px; flex-wrap:wrap;}
        .chip{display:inline-flex; align-items:center; gap:6px; padding:6px 10px; border-radius:999px;
              text-decoration:none; border:1px solid rgba(15,23,42,.14); background:#fff; font-size:13px;}
        .chip:hover{border-color: rgba(2,132,199,.35);}
        .chip-link{border-color: rgba(2,132,199,.35); color:#075985;}
        .chip-primary{border-color: rgba(14,165,233,.35); color:#075985; background: rgba(14,165,233,.06);}
        .chip-danger{border-color: rgba(239,68,68,.35); color:#b91c1c; background: rgba(239,68,68,.06); cursor:pointer;}
        .chip-danger:hover{border-color: rgba(239,68,68,.6);}
        .btn-inline{padding:6px 12px; border-radius:12px;}
        /* Modern tables */
        table{width:100%; border-collapse:separate; border-spacing:0;}
        thead th{position:sticky; top:0; z-index:5; background:rgba(255,255,255,.92); backdrop-filter:saturate(180%) blur(8px);
                 border-bottom:1px solid rgba(15,23,42,.10);}
        tbody tr:nth-child(even){background:rgba(2,132,199,.03);}
        tbody tr:hover{background:rgba(14,165,233,.08);}
        td, th{padding:10px 12px; vertical-align:top; position:relative;}
        /* Sticky action column (last column) */
        th:last-child{position:sticky; right:0; z-index:6; background:rgba(255,255,255,.96);}
        td:last-child{position:sticky; right:0; z-index:2; background:rgba(255,255,255,.98); overflow:visible;}
        /* When an action menu is open, lift this cell above other sticky cells (prevents overlap) */
        td:last-child:has(details[open]){z-index:999;}

        /* Action menu (native details/summary) */
        .action-menu{position:relative; display:inline-block; z-index:1;}
        .action-menu[open]{z-index:999;}
        .action-menu[open] .menu{filter:none;}

        .action-menu > summary{list-style:none;}
        .action-menu > summary::-webkit-details-marker{display:none;}
        .action-menu .menu{
          display:none;
          position:static; /* no overlay -> better readability */
          margin-top:10px;
          min-width:220px;
          background:#fff;
          color:#0f172a;
          border:1px solid rgba(15,23,42,.14);
          border-radius:16px;
          box-shadow:0 18px 44px rgba(2,6,23,.20);
          padding:8px;
        }
        .action-menu[open] .menu{display:block;}
        .menu-item{
          display:flex;
          align-items:center;
          justify-content:flex-start;
          gap:10px;
          width:100%;
          padding:10px 12px;
          border-radius:12px;
          text-decoration:none;
          border:none;
          background:transparent;
          cursor:pointer;
          color:#0f172a;
          font-size:14px;
          font-weight:600;
        }
        .menu-item:hover{background:rgba(14,165,233,.12);}
        .menu-item:focus{outline:2px solid rgba(14,165,233,.35); outline-offset:2px;}
        .menu-danger{color:#b91c1c;}
        .menu-danger:hover{background:rgba(239,68,68,.12);}
        .action-btn{min-width:132px; justify-content:center;}

        
        /* Landing tiles */
        .tiles{display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:18px; margin-top:18px;}
        .tile{display:block; padding:22px 20px; border-radius:18px; background:#fff; border:1px solid rgba(15,23,42,.10);
              box-shadow:0 8px 26px rgba(2,6,23,.06); text-decoration:none; color:inherit; transition:transform .12s ease, border-color .12s ease;}
        .tile:hover{border-color: rgba(2,132,199,.35); transform: translateY(-1px);}
        .tile-title{font-weight:900; letter-spacing:.02em; font-size:16px; margin-bottom:10px;}
        .tile-count{font-size:36px; font-weight:900; line-height:1; margin-bottom:10px;}
        .tile-sub{font-size:13px; color: rgba(15,23,42,.65);}
        @media (max-width: 980px){ .tiles{grid-template-columns:1fr;} }

        .mono{font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono','Courier New', monospace; font-size:13px;}
      </style>
    </head>
    <body>
      __LOGO_HTML__
      <div class="container">
        __BACKBAR_HTML__
        __BODY_HTML__
      </div>
      <script>

        function _selectedIds(){
          const xs = Array.from(document.querySelectorAll("input.rowchk:checked"));
          return xs.map(x => x.value).join(",");
        }

        function bulkExport(entity, fieldKey){
          const ids = _selectedIds();
          if(!ids){
            alert("Bitte mindestens einen Datensatz auswählen.");
            return;
          }
          const fk = fieldKey ? ("&field_key=" + encodeURIComponent(fieldKey)) : "";
          window.location.href = "/dq/bulk/xlsx/selected?entity=" + encodeURIComponent(entity) +
                                 "&ids=" + encodeURIComponent(ids) + fk;
        }

        function toggleAllRows(masterId){
          const m = document.getElementById(masterId);
          const checked = m ? m.checked : false;
          document.querySelectorAll("input.rowchk").forEach(x => { x.checked = checked; });
        }

        async function deletePerson(id, redirectUrl){
          if(!confirm("Kontakt wirklich in Pipedrive löschen?\n\nHinweis: Das kann nicht rückgängig gemacht werden.")) return;
          const res = await fetch(`/dq/contacts/person/${id}/delete`, {method:"POST"});
          const data = await res.json().catch(()=>null);
          if(res.ok && data && data.ok){
            alert("✅ Kontakt gelöscht");
            if(redirectUrl){ window.location.href = redirectUrl; }
            else { location.reload(); }
          } else {
            alert("❌ Fehler: " + ((data && data.error) ? data.error : ("HTTP " + res.status)));
          }
        }

        
        function _closest(el, sel){
          while(el && el !== document){ if(el.matches && el.matches(sel)) return el; el = el.parentNode; }
          return null;
        }

        function closeAllActionMenus(){
          document.querySelectorAll(".action-menu.open").forEach(m => m.classList.remove("open"));
        }

        function toggleActionMenu(btn){
          const m = _closest(btn, ".action-menu");
          if(!m) return;
          const isOpen = m.classList.contains("open");
          closeAllActionMenus();
          if(!isOpen) m.classList.add("open");
        }

        document.addEventListener("click", function(ev){
          const inside = _closest(ev.target, ".action-menu");
          if(!inside) closeAllActionMenus();
        });


        async function updateField(entityType, id, fieldKey){
          const inp = document.getElementById(`inp_${entityType}_${id}_${fieldKey}`);
          const val = inp ? inp.value : "";
          if(!confirm("Wirklich in Pipedrive aktualisieren?")) return;

          const res = await fetch("/dq/update", {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body: JSON.stringify({
              entity_type: entityType,
              entity_id: parseInt(id),
              field_key: fieldKey,
              value: val
            })
          });
          const data = await res.json().catch(()=>null);
          if(data && data.ok){
            alert("✅ Aktualisiert.");
          } else {
            alert("❌ Fehler: " + ((data && data.error) ? data.error : ("HTTP " + res.status)));
          }
        }
      </script>
    </body>
    </html>
    """

    return (html
            .replace("__TITLE__", html_escape(title))
            .replace("__CSS_VERSION__", str(CSS_VERSION))
            .replace("__LOGO_HTML__", logo_html)
            .replace("__BODY_HTML__", body_html)
            .replace("__BACKBAR_HTML__", backbar_html))


DQ_CARDS = [
    {
        "group": "Kontakte",
        "title": "Vorname",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/first_name/missing"},
            {"label": "Ungültige Zeichen", "href": "/dq/contacts/first_name/invalidchars"},
            {"label": "Titel im Vornamen", "href": "/dq/contacts/first_name/title"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Nachname",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/last_name/missing"},
            {"label": "Ungültige Zeichen", "href": "/dq/contacts/last_name/invalidchars"},
        ],
    },
    {"group": "Kontakte", "title": "Geschlecht", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/contacts/gender/missing"}]},
    {"group": "Kontakte", "title": "E-Mail-Adresse", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/contacts/email/missing"}]},
    {"group": "Kontakte", "title": "Du oder Sie", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/contacts/du_sie/missing"}]},
    {"group": "Kontakte", "title": "Position", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/contacts/position/missing"}]},
    {"group": "Kontakte", "title": "LinkedIn-URL", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/contacts/linkedin/missing"}]},

    {"group": "Kontakte", "title": "Zuordnung", "description": "", "actions": [
        {"label": "Keine Organisation", "href": "/dq/contacts/org/missing"},
        {"label": "E-Mail passt nicht zur Organisation", "href": "/dq/contacts/email/mismatch"},
    ]},

    # Freelancer (Organisation = "Freelancer")
    {"group": "Freelancer", "title": "Vorname", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/first_name/missing"}]},
    {"group": "Freelancer", "title": "Nachname", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/last_name/missing"}]},
    {"group": "Freelancer", "title": "Geschlecht", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/gender/missing"}]},
    {"group": "Freelancer", "title": "E-Mail-Adresse", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/email/missing"}]},
    {"group": "Freelancer", "title": "Du oder Sie", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/du_sie/missing"}]},
    {"group": "Freelancer", "title": "Position", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/position/missing"}]},
    {"group": "Freelancer", "title": "LinkedIn-URL", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/linkedin/missing"}]},

    # Orgs
    {"group": "Organisationen", "title": "Name / Rechtsform", "description": "", "actions": [
        {"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=name"},
        {"label": "Ungültige Zeichen", "href": "/dq/orgs/invalidchars?field=name"},
    ]},
    {"group": "Organisationen", "title": "Adresse", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=address"}]},
    {"group": "Organisationen", "title": "Website", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=website"}]},
    {"group": "Organisationen", "title": "Kontakte", "description": "", "actions": [{"label": "Keine Kontakte", "href": "/dq/orgs/no_contacts"}]},
]



def _render_cards(group: str, counts: dict[str, Optional[int]]) -> str:
    cards = [c for c in DQ_CARDS if c["group"] == group]

    if group == "Kontakte":
        group_class = "contacts"
        group_sub = "Personenbezogene Prüfungen (ohne Freelancer)"
    elif group == "Freelancer":
        group_class = "contacts"
        group_sub = f"Personenbezogene Prüfungen (Organisation = {FREELANCER_ORG_NAME})"
    else:
        group_class = "orgs"
        group_sub = "Firmendaten / Stammdaten"

    def _count_badge(n: Optional[int]) -> str:
        if n is None:
            return ""
        return f'<span style="margin-left:8px; padding:2px 8px; border-radius:999px; font-size:12px; background:rgba(2,132,199,.12); color:#075985;">{int(n)}</span>'

    card_html = []
    for c in cards:
        actions_html = []
        seen_hrefs = set()
        total = 0
        has_any = False

        for a in c.get("actions", []):
            href = a["href"]
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            n = counts.get(href)
            if isinstance(n, int):
                total += n
                has_any = True

            actions_html.append(
                f'<a class="chip" href="{href}">{a["label"]}{_count_badge(n)}</a>'
            )

        total_badge = _count_badge(total) if has_any else ""
        desc = c.get("description", "")

        card_html.append(f"""
          <div class="card">
            <div class="card-top">
              <h3>{c["title"]}{total_badge}</h3>
              <div class="card-desc">{desc}</div>
            </div>
            <div class="actions-row">
              {''.join(actions_html)}
            </div>
          </div>
        """)

    return f"""
      <div class="section-header {group_class}">
        <div class="section-title">{group}</div>
        <div class="section-sub">{group_sub}</div>
      </div>
      <div class="grid">
        {''.join(card_html)}
      </div>
    """

@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    counts = await compute_overview_counts() if db_pool else {}

    def _group_total(group_name: str) -> int:
        total = 0
        for c in DQ_CARDS:
            if c.get("group") != group_name:
                continue
            for a in c.get("actions", []):
                n = counts.get(a.get("href"))
                if isinstance(n, int):
                    total += n
        return int(total)

    total_contacts = _group_total("Kontakte")
    total_freelancers = _group_total("Freelancer")
    total_orgs = _group_total("Organisationen")

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Datenqualität</div>
          <div class="subtitle">Wähle einen Bereich</div>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
          <a class="btn btn-outline" href="/admin">Admin</a>
          <a class="btn btn-outline" href="/logout">Logout</a>
        </div>
      </div>

      <div class="tiles">
        <a class="tile" href="/overview/contacts">
          <div class="tile-title">Kontakte</div>
          <div class="tile-count">{total_contacts}</div>
          <div class="tile-sub">Personen ohne Organisation "{html_escape(FREELANCER_ORG_NAME)}"</div>
        </a>

        <a class="tile" href="/overview/freelancer">
          <div class="tile-title">Freelancer</div>
          <div class="tile-count">{total_freelancers}</div>
          <div class="tile-sub">Personen mit Organisation "{html_escape(FREELANCER_ORG_NAME)}"</div>
        </a>

        <a class="tile" href="/overview/orgs">
          <div class="tile-title">Organisationen</div>
          <div class="tile-count">{total_orgs}</div>
          <div class="tile-sub">Organisation-Prüfungen</div>
        </a>
      </div>
    """
    # Landing page: no back button
    return HTMLResponse(page_shell("Datenqualität – Übersicht", body, back_href=""))




@app.get("/overview/contacts", response_class=HTMLResponse)
async def overview_contacts(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    counts = await compute_overview_counts() if db_pool else {}
    body = f'''
      <div class="topbar">
        <div>
          <div class="title">Kontakte</div>
          <div class="subtitle"><a class="chip chip-link" href="/overview">‹ Übersicht</a></div>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
          <a class="btn btn-outline" href="/admin">Admin</a>
          <a class="btn btn-outline" href="/logout">Logout</a>
        </div>
      </div>
      {_render_cards("Kontakte", counts)}
    '''
    return HTMLResponse(page_shell("Kontakte – Übersicht", body, back_href="/overview"))


@app.get("/overview/freelancer", response_class=HTMLResponse)
async def overview_freelancer(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    counts = await compute_overview_counts() if db_pool else {}
    body = f'''
      <div class="topbar">
        <div>
          <div class="title">Freelancer</div>
          <div class="subtitle"><a class="chip chip-link" href="/overview">‹ Übersicht</a></div>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
          <a class="btn btn-outline" href="/admin">Admin</a>
          <a class="btn btn-outline" href="/logout">Logout</a>
        </div>
      </div>
      {_render_cards("Freelancer", counts)}
    '''
    return HTMLResponse(page_shell("Freelancer – Übersicht", body, back_href="/overview"))


@app.get("/overview/orgs", response_class=HTMLResponse)
async def overview_orgs(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    counts = await compute_overview_counts() if db_pool else {}
    body = f'''
      <div class="topbar">
        <div>
          <div class="title">Organisationen</div>
          <div class="subtitle"><a class="chip chip-link" href="/overview">‹ Übersicht</a></div>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
          <a class="btn btn-outline" href="/admin">Admin</a>
          <a class="btn btn-outline" href="/logout">Logout</a>
        </div>
      </div>
      {_render_cards("Organisationen", counts)}
    '''
    return HTMLResponse(page_shell("Organisationen – Übersicht", body, back_href="/overview"))


########################################################################
#
# Admin: Sync
#
########################################################################

@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    body = """
    <div class="topbar">
      <div>
        <div class="title">Admin – Sync</div>
        <div class="subtitle">Starte Sync-Jobs in Batches (perfekt für große Datenmengen).</div>
      </div>
      <div style="display:flex; gap:10px;">      </div>
    </div>

    <div class="panel">
      <div style="display:flex; flex-wrap:wrap; gap:10px;">
        <a class="btn btn-primary" href="/admin/sync?entity=organizations&full=1&max_pages=50">Initial: Orgs (50 Seiten)</a>
        <a class="btn btn-primary" href="/admin/sync?entity=persons&full=1&max_pages=50">Initial: Persons (50 Seiten)</a>
        <a class="btn btn-outline" href="/admin/sync?entity=organizations&full=0&max_pages=20">Inkrementell: Orgs</a>
        <a class="btn btn-outline" href="/admin/sync?entity=persons&full=0&max_pages=20">Inkrementell: Persons</a>
        <a class="btn btn-outline" href="/admin/sync/status">Status</a>
        <a class="btn btn-outline" href="/admin/cache/counts">Cache-Counts</a>
      </div>

      <div style="margin-top:12px;" class="small">
        Tipp: Für Initial-Sync mehrfach klicken, bis bei <b>processed</b> keine neuen Datensätze mehr kommen.<br/>
        Wichtig für Freelancer-Listen: zuerst <b>Initial: Orgs</b> laufen lassen, damit orgs_cache die Org-Namen kennt.
      </div>
    </div>
    """
    return HTMLResponse(page_shell("Admin – Sync", body))


@app.get("/admin/sync")
async def admin_sync(entity: str = "persons", full: int = 0, max_pages: int = 20):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return JSONResponse({"ok": False, "error": "DATABASE_URL fehlt / DB nicht initialisiert"}, status_code=500)

    try:
        if entity == "persons":
            res = await sync_persons_incremental(full=bool(full), max_pages=max_pages)
            return {"ok": True, "result": res}

        if entity in ("orgs", "organizations"):
            res = await sync_orgs_incremental(full=bool(full), max_pages=max_pages)
            return {"ok": True, "result": res}

        return JSONResponse({"ok": False, "error": "entity muss 'persons' oder 'organizations' sein"}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/admin/sync/status")
async def admin_sync_status():
    if not db_pool:
        return {"ok": False, "error": "DB nicht initialisiert"}
    p = await get_sync_time("persons")
    o = await get_sync_time("organizations")
    return {"ok": True, "persons_last": p.isoformat(), "orgs_last": o.isoformat()}


@app.get("/admin/cache/counts")
async def admin_cache_counts():
    if not db_pool:
        return {"ok": False, "error": "DB nicht initialisiert"}
    async with db_pool.acquire() as conn:
        persons = await conn.fetchval("SELECT COUNT(*) FROM persons_cache")
        orgs = await conn.fetchval("SELECT COUNT(*) FROM orgs_cache")
        missing_first = await conn.fetchval("SELECT COUNT(*) FROM persons_cache WHERE first_name IS NULL OR btrim(first_name)=''")
    return {
        "ok": True,
        "persons_cache": int(persons),
        "orgs_cache": int(orgs),
        "missing_first_name": int(missing_first),
    }

########################################################################
#
# DB Queries: Person Detail + DQ Lists
#
########################################################################

async def db_fetch_person_detail(person_id: int) -> dict:
    sql = """
    SELECT
      p.id, p.first_name, p.last_name, p.gender, p.email, p.du_sie, p.position, p.linkedin_url,
      p.org_id, p.label_ids,
      COALESCE(o.name, '-') AS org_name
    FROM persons_cache p
    LEFT JOIN orgs_cache o ON o.id = p.org_id
    WHERE p.id = $1
    """
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(sql, person_id)
    return dict(row) if row else {}



async def db_delete_person_cache(person_id: int) -> None:
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM persons_cache WHERE id=$1", int(person_id))




async def db_upsert_person_cache_partial(
    person_id: int,
    *,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    gender: Optional[str] = None,
    email: Optional[str] = None,
    du_sie: Optional[str] = None,
    position: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    org_id: Optional[int] = None,
    label_ids: Optional[list[int]] = None,
):
    """
    Upsert in persons_cache, aber nur Felder überschreiben, die übergeben wurden.

    Konvention:
    - None bedeutet: Feld nicht anfassen
    - "" (leerer String) bedeutet: Feld bewusst leeren

    Hinweis: gender/du_sie werden im Cache als TEXT (Option-ID) gespeichert.
    """
    if not db_pool:
        return

    now = _utcnow()

    sql = """
    INSERT INTO persons_cache
      (id, first_name, last_name, gender, email, du_sie, position, linkedin_url, org_id, update_time, label_ids)
    VALUES
      ($1, COALESCE($2,''), COALESCE($3,''), COALESCE($4,''), COALESCE($5,''), COALESCE($6,''), COALESCE($7,''), COALESCE($8,''), $9, $10, $11)
    ON CONFLICT (id) DO UPDATE SET
      first_name   = COALESCE($2, persons_cache.first_name),
      last_name    = COALESCE($3, persons_cache.last_name),
      gender       = COALESCE($4, persons_cache.gender),
      email        = COALESCE($5, persons_cache.email),
      du_sie       = COALESCE($6, persons_cache.du_sie),
      position     = COALESCE($7, persons_cache.position),
      linkedin_url = COALESCE($8, persons_cache.linkedin_url),
      org_id       = COALESCE($9, persons_cache.org_id),
      update_time  = $10,
      label_ids    = COALESCE($11, persons_cache.label_ids)
    """

    async with db_pool.acquire() as conn:
        await conn.execute(
            sql,
            int(person_id),
            first_name,
            last_name,
            gender,
            email,
            du_sie,
            position,
            linkedin_url,
            org_id,
            now,
            label_ids,
        )

def _freelancer_filter_sql(mode: str) -> str:
    """
    mode:
      - "only": nur Freelancer (org.name = FREELANCER_ORG_NAME)
      - "exclude": alles außer Freelancer
      - "all": keine Filterung
    """
    if mode == "all":
        return ""

    if mode == "only":
        return """
          AND EXISTS (
            SELECT 1 FROM orgs_cache o
            WHERE o.id = persons_cache.org_id
              AND lower(o.name) = lower($3)
          )
        """
    if mode == "exclude":
        return """
          AND NOT EXISTS (
            SELECT 1 FROM orgs_cache o
            WHERE o.id = persons_cache.org_id
              AND lower(o.name) = lower($3)
          )
        """
    return ""


def _dq_missing_sql_for_column(col: str, freelancer_mode: str = "exclude") -> str:
    flt = _freelancer_filter_sql(freelancer_mode)
    if flt.strip():
        return f"""
        SELECT id, first_name, last_name
        FROM persons_cache
        WHERE ({col} IS NULL OR btrim({col}) = '')
          AND id > $1
        {flt}
        ORDER BY id
        LIMIT $2
        """
    return f"""
        SELECT id, first_name, last_name
        FROM persons_cache
        WHERE ({col} IS NULL OR btrim({col}) = '')
          AND id > $1
        ORDER BY id
        LIMIT $2
    """


async def _render_missing_list(
    title: str,
    base_path: str,
    after_id: int,
    limit: int,
    sql: str,
    field_key: str,
    freelancer_mode: str = "exclude",
) -> HTMLResponse:
    """
    Standard-Listen-Renderer für "fehlende Daten" (Kontakte/Freelancer).

    Neu:
    - Checkbox-Auswahl
    - CSV Export der Auswahl
    - Excel Import (Bulk Update)
    """
    back_url = f"{base_path}?after_id={after_id}&limit={limit}"
    back_q = urllib.parse.quote(back_url, safe="")

    async with db_pool.acquire() as conn:
        if freelancer_mode in ("only", "exclude"):
            rows = await conn.fetch(sql, after_id, limit, FREELANCER_ORG_NAME)
        else:
            rows = await conn.fetch(sql, after_id, limit)

    trs = []
    last_id = after_id
    for r in rows:
        pid = int(r["id"])
        last_id = pid
        fn = (r["first_name"] or "").strip() or "-"
        ln = (r["last_name"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td style="width:48px; text-align:center;">
              <input class="rowchk" type="checkbox" value="{pid}">
            </td>
            <td style="width:120px;"><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:340px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/contacts/person/{pid}?back={back_q}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_person_url(pid)}">Pipedrive ↗</a>
                  <a class="menu-item menu-danger" href="/dq/contacts/person/{pid}/delete_confirm?back={back_q}">🗑 Löschen</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="{base_path}?after_id={last_id}&limit={limit}">Weiter →</a>'

    subtitle = (
        "Kontakte (ohne Freelancer)"
        if freelancer_mode == "exclude"
        else ("Nur Freelancer" if freelancer_mode == "only" else "Alle Kontakte")
    )

    bulk_panel = """
      <div class="panel" style="margin-bottom:12px;">
        <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center; justify-content:space-between;">
          <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
            <label class="small" style="display:flex; align-items:center; gap:8px;">
              <input id="chk_all_rows" type="checkbox" onchange="toggleAllRows('chk_all_rows')">
              Alle auswählen
            </label>
            <button class="btn btn-outline" onclick="bulkExport('person', '{field_key}')">Excel-Export</button>
          </div>
        </div>
        <div class="small" style="margin-top:8px; opacity:.9;">
          Hinweise: Leere Zellen werden <b>nicht</b> geändert. Wert <code>__CLEAR__</code> leert ein Feld.
        </div>
      </div>
    """

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">{html_escape(title)}</div>
          <div class="subtitle">{html_escape(subtitle)}</div>
        </div>
        <div style="display:flex; gap:10px;">          {next_link}
        </div>
      </div>

      {bulk_panel}

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:48px; text-align:center;"><input id="chk_all_rows_header" type="checkbox" onchange="toggleAllRows('chk_all_rows_header')"></th>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:340px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="5">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell(title, body))

async def _db_collect_invalid_person_name_rows(
    col: str,
    after_id: int,
    limit: int,
    freelancer_mode: str,
    scan_batch: int = 2000,
    max_batches: int = 20,
) -> tuple[list[dict], int]:
    """
    Holt invalid name rows aus der DB, aber die Prüfung der Sonderzeichen passiert in Python,
    damit Akzente, Bindestrich, Punkt, Apostroph etc. sauber erlaubt bleiben.

    Rückgabe:
      (rows, next_after_id)  -> next_after_id ist die letzte gescannte ID (für Pagination)
    """
    out: list[dict] = []
    last_scanned = after_id

    flt = _freelancer_filter_sql(freelancer_mode)
    base_sql = f"""
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE {col} IS NOT NULL
      AND btrim({col}) <> ''
      AND id > $1
    {flt}
    ORDER BY id
    LIMIT $2
    """

    async with db_pool.acquire() as conn:
        for _ in range(max_batches):
            if freelancer_mode in ("only", "exclude"):
                rows = await conn.fetch(base_sql, last_scanned, scan_batch, FREELANCER_ORG_NAME)
            else:
                rows = await conn.fetch(base_sql, last_scanned, scan_batch)

            if not rows:
                break

            for r in rows:
                rid = int(r["id"])
                last_scanned = rid
                val = (r[col] or "").strip()
                if _has_invalid_name_chars(val):
                    out.append(dict(r))
                    if len(out) >= limit:
                        return out, last_scanned

    return out, last_scanned

########################################################################
#
# DQ: Missing (Kontakte ohne Freelancer)
#
########################################################################

@app.get("/dq/contacts/first_name/missing", response_class=HTMLResponse)
async def dq_first_name_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("first_name", freelancer_mode="exclude")
    return await _render_missing_list("Vorname – Fehlende Daten", "/dq/contacts/first_name/missing", after_id, limit, sql, field_key="first_name", freelancer_mode="exclude")


@app.get("/dq/contacts/last_name/missing", response_class=HTMLResponse)
async def dq_last_name_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("last_name", freelancer_mode="exclude")
    return await _render_missing_list("Nachname – Fehlende Daten", "/dq/contacts/last_name/missing", after_id, limit, sql, field_key="last_name", freelancer_mode="exclude")


@app.get("/dq/contacts/gender/missing", response_class=HTMLResponse)
async def dq_gender_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("gender", freelancer_mode="exclude")
    return await _render_missing_list("Geschlecht – Fehlende Daten", "/dq/contacts/gender/missing", after_id, limit, sql, field_key="gender", freelancer_mode="exclude")


@app.get("/dq/contacts/email/missing", response_class=HTMLResponse)
async def dq_email_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("email", freelancer_mode="exclude")
    return await _render_missing_list("E-Mail – Fehlende Daten", "/dq/contacts/email/missing", after_id, limit, sql, field_key="email", freelancer_mode="exclude")


@app.get("/dq/contacts/du_sie/missing", response_class=HTMLResponse)
async def dq_du_sie_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("du_sie", freelancer_mode="exclude")
    return await _render_missing_list("Du oder Sie – Fehlende Daten", "/dq/contacts/du_sie/missing", after_id, limit, sql, field_key="du_sie", freelancer_mode="exclude")


@app.get("/dq/contacts/position/missing", response_class=HTMLResponse)
async def dq_position_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("position", freelancer_mode="exclude")
    return await _render_missing_list("Position – Fehlende Daten", "/dq/contacts/position/missing", after_id, limit, sql, field_key="position", freelancer_mode="exclude")


@app.get("/dq/contacts/linkedin/missing", response_class=HTMLResponse)
async def dq_linkedin_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("linkedin_url", freelancer_mode="exclude")
    return await _render_missing_list("LinkedIn-URL – Fehlende Daten", "/dq/contacts/linkedin/missing", after_id, limit, sql, field_key="linkedin_url", freelancer_mode="exclude")

########################################################################
#
# DQ: Missing (Freelancer only)
#
########################################################################

@app.get("/dq/freelancers/first_name/missing", response_class=HTMLResponse)
async def dq_freelancers_first_name_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("first_name", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Vorname (fehlend)", "/dq/freelancers/first_name/missing", after_id, limit, sql, field_key="first_name", freelancer_mode="only")


@app.get("/dq/freelancers/last_name/missing", response_class=HTMLResponse)
async def dq_freelancers_last_name_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("last_name", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Nachname (fehlend)", "/dq/freelancers/last_name/missing", after_id, limit, sql, field_key="last_name", freelancer_mode="only")


@app.get("/dq/freelancers/gender/missing", response_class=HTMLResponse)
async def dq_freelancers_gender_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("gender", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Geschlecht (fehlend)", "/dq/freelancers/gender/missing", after_id, limit, sql, field_key="gender", freelancer_mode="only")


@app.get("/dq/freelancers/email/missing", response_class=HTMLResponse)
async def dq_freelancers_email_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("email", freelancer_mode="only")
    return await _render_missing_list("Freelancer – E-Mail (fehlend)", "/dq/freelancers/email/missing", after_id, limit, sql, field_key="email", freelancer_mode="only")


@app.get("/dq/freelancers/du_sie/missing", response_class=HTMLResponse)
async def dq_freelancers_du_sie_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("du_sie", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Du/Sie (fehlend)", "/dq/freelancers/du_sie/missing", after_id, limit, sql, field_key="du_sie", freelancer_mode="only")


@app.get("/dq/freelancers/position/missing", response_class=HTMLResponse)
async def dq_freelancers_position_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("position", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Position (fehlend)", "/dq/freelancers/position/missing", after_id, limit, sql, field_key="position", freelancer_mode="only")


@app.get("/dq/freelancers/linkedin/missing", response_class=HTMLResponse)
async def dq_freelancers_linkedin_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("linkedin_url", freelancer_mode="only")
    return await _render_missing_list("Freelancer – LinkedIn (fehlend)", "/dq/freelancers/linkedin/missing", after_id, limit, sql, field_key="linkedin_url", freelancer_mode="only")

########################################################################
#
# DQ: Invalid Chars (Kontakte ohne Freelancer) – DB scan + Python validation
#
########################################################################

@app.get("/dq/contacts/first_name/invalidchars", response_class=HTMLResponse)
async def dq_first_name_invalidchars(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/contacts/first_name/invalidchars"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    rows, next_after = await _db_collect_invalid_person_name_rows("first_name", after_id, limit, freelancer_mode="exclude")

    trs = []
    for r in rows:
        pid = int(r["id"])
        fn = (r["first_name"] or "").strip() or "-"
        ln = (r["last_name"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:340px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/contacts/person/{pid}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_person_url(pid)}">Pipedrive ↗</a>
                  <a class="menu-item menu-danger" href="/dq/contacts/person/{pid}/delete_confirm?back={back_q}">🗑 Löschen</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = ""
    if next_after and next_after > after_id:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/first_name/invalidchars?after_id={next_after}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Vorname – Ungültige Zeichen</div>
          <div class="subtitle">Kontakte (ohne Freelancer) </div>
          <div class="subtitle"><span class="small">Erlaubt: Buchstaben inkl. Akzente, Leerzeichen, Bindestrich, Punkt, Apostroph. Nicht erlaubt: Emojis, Zahlen, Steuerzeichen.</span></div>
        </div>
        <div style="display:flex; gap:10px;">          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:340px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Vorname – Ungültige Zeichen", body))


@app.get("/dq/contacts/last_name/invalidchars", response_class=HTMLResponse)
async def dq_last_name_invalidchars(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/contacts/last_name/invalidchars"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    rows, next_after = await _db_collect_invalid_person_name_rows("last_name", after_id, limit, freelancer_mode="exclude")

    trs = []
    for r in rows:
        pid = int(r["id"])
        fn = (r["first_name"] or "").strip() or "-"
        ln = (r["last_name"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:340px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/contacts/person/{pid}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_person_url(pid)}">Pipedrive ↗</a>
                  <a class="menu-item menu-danger" href="/dq/contacts/person/{pid}/delete_confirm?back={back_q}">🗑 Löschen</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = ""
    if next_after and next_after > after_id:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/last_name/invalidchars?after_id={next_after}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Nachname – Ungültige Zeichen</div>
          <div class="subtitle">Kontakte (ohne Freelancer) </div>
        </div>
        <div style="display:flex; gap:10px;">          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:340px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Nachname – Ungültige Zeichen", body))

########################################################################
#
# DQ: Titel im Vornamen
#
########################################################################

@app.get("/dq/contacts/first_name/title", response_class=HTMLResponse)
async def dq_first_name_title(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/contacts/first_name/title"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    pattern = r"^\s*(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|herr|frau)(\s|\.|$)"

    sql = """
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE first_name IS NOT NULL
      AND btrim(first_name) <> ''
      AND first_name ~* $1
      AND id > $2
    ORDER BY id
    LIMIT $3
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, pattern, after_id, limit)

    trs = []
    last_id = after_id
    for r in rows:
        pid = int(r["id"])
        last_id = pid
        fn = (r["first_name"] or "").strip() or "-"
        ln = (r["last_name"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:340px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/contacts/person/{pid}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_person_url(pid)}">Pipedrive ↗</a>
                  <a class="menu-item menu-danger" href="/dq/contacts/person/{pid}/delete_confirm?back={back_q}">🗑 Löschen</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/first_name/title?after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Vorname – Titel im Vornamen</div>
          <div class="subtitle">Liste aus Cache-DB · Page size: {limit}</div>
        </div>
        <div style="display:flex; gap:10px;">          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:340px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Vorname – Titel im Vornamen", body))

########################################################################
#
# Kontakt Detail + Update
#
########################################################################

@app.get("/dq/contacts/person/{person_id}", response_class=HTMLResponse)
async def dq_person_detail(person_id: int, saved: int = 0, back: str = ""):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)

    headers = get_headers()

    # Cache nachziehen (Person)
    await refresh_person_cache_from_api(person_id, headers)

    p = await db_fetch_person_detail(person_id)
    if not p:
        return HTMLResponse("Kontakt nicht im Cache gefunden. Bitte Sync laufen lassen.", status_code=404)

    # Falls Org-Name fehlt, Org cache nachziehen
    org_id = p.get("org_id")
    if org_id and (p.get("org_name") in (None, "-", "")):
        await refresh_org_cache_from_api(int(org_id), headers)
        p = await db_fetch_person_detail(person_id)

    gender_opts = await get_person_field_options(headers, PD_PERSON_GENDER_KEY)
    du_opts = await get_person_field_options(headers, PD_PERSON_DU_SIE_KEY)

    label_ids = p.get("label_ids") or []
    if not isinstance(label_ids, list):
        label_ids = []

    # Labels: IDs -> Namen (über /v1/personFields -> field key "label")
    label_opts = await get_person_field_options(headers, "label")
    if not label_opts:
        # Fallback, falls ein Workspace abweichende Keys nutzt
        label_opts = await get_person_field_options(headers, "label_ids")
    label_map = {k: v for (k, v) in (label_opts or [])}

    label_names: list[str] = []
    for lid in label_ids:
        k = str(lid)
        label_names.append(label_map.get(k, k))
    labels_text = ", ".join(label_names) if label_names else "-"

    notice = ""
    if saved == 1:
        notice = '<div class="panel" style="margin-bottom:12px; border-color: rgba(14,165,233,.35);">✅ Gespeichert.</div>'

    def val(k: str) -> str:
        return html_escape((p.get(k) or "").strip())

    def select_html(select_id: str, current: str, options: list[tuple[str, str]]) -> str:
        cur = (current or "").strip()
        opts_html = ['<option value="">– bitte wählen –</option>']
        for v, lab in options:
            sel = " selected" if v == cur else ""
            opts_html.append(f'<option value="{html_escape(v)}"{sel}>{html_escape(lab)}</option>')
        return f'<select class="field-input" id="{select_id}">{"".join(opts_html)}</select>'

    body = f"""
      <div class="topbar">
        <div>
          <h1 class="title" style="margin:0;">Kontakt bearbeiten</h1>
          <div class="subtitle"><code class="badge">{person_id}</code> · Organisation: <b>{html_escape(p.get("org_name") or "-")}</b></div>
        </div>
        <div style="display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-end;">
          <a class="chip chip-link" target="_blank" rel="noopener" href="{pipedrive_person_url(person_id)}">Pipedrive ↗</a>
          <a class="chip chip-link" target="_blank" rel="noopener" href="{pipedrive_org_url(int(org_id)) if org_id else "#"}">Organisation ↗</a>
          <button class="chip chip-danger" onclick="deletePerson({person_id}, '/overview')">🗑 Löschen</button>
          <a class="btn btn-outline" href="/overview">Übersicht</a>
        </div>
      </div>

      {notice}

      <div class="panel">
        <table>
          <tbody>
            <tr><th style="width:240px;">Vorname</th><td><input class="field-input" id="first_name" value="{val("first_name")}" /></td></tr>
            <tr><th>Nachname</th><td><input class="field-input" id="last_name" value="{val("last_name")}" /></td></tr>

            <tr><th>Geschlecht</th><td>{select_html("gender", p.get("gender") or "", gender_opts)}</td></tr>
            <tr><th>E-Mail-Adresse</th><td><input class="field-input" id="email" value="{val("email")}" /></td></tr>
            <tr><th>Du oder Sie</th><td>{select_html("du_sie", p.get("du_sie") or "", du_opts)}</td></tr>

            <tr><th>Position</th><td><input class="field-input" id="position" value="{val("position")}" /></td></tr>
            <tr><th>LinkedIn-URL</th><td><input class="field-input" id="linkedin_url" value="{val("linkedin_url")}" /></td></tr>

            <tr><th>Labels</th><td><input class="field-input" value="{html_escape(labels_text)}" disabled /></td></tr>
            <tr><th>Organisation</th><td><input class="field-input" value="{html_escape(p.get("org_name") or "-")}" disabled /></td></tr>
          </tbody>
        </table>

        <div style="margin-top:12px; display:flex; gap:10px;">
          <button class="btn btn-primary" onclick="savePerson({person_id})">Speichern</button>
        </div>
      </div>

      <script>
        async function savePerson(personId) {{
          const payload = {{
            first_name: document.getElementById("first_name").value || "",
            last_name: document.getElementById("last_name").value || "",
            gender: document.getElementById("gender").value || "",
            email: document.getElementById("email").value || "",
            du_sie: document.getElementById("du_sie").value || "",
            position: document.getElementById("position").value || "",
            linkedin_url: document.getElementById("linkedin_url").value || ""
          }};

          const res = await fetch(`/dq/contacts/person/${{personId}}/update`, {{
            method:"POST",
            headers:{{"Content-Type":"application/json"}},
            body: JSON.stringify(payload)
          }});

          let data = null;
          try {{ data = await res.json(); }} catch(e) {{}}

          if(res.ok && data && data.ok) {{
            window.location.href = `/dq/contacts/person/${{personId}}?saved=1`;
          }} else {{
            alert("❌ Fehler: " + ((data && data.error) ? data.error : ("HTTP " + res.status)));
          }}
        }}
      </script>
    """
    back_href = back if (back and isinstance(back,str) and back.startswith("/")) else "/overview"
    return HTMLResponse(page_shell("Kontakt bearbeiten", body, back_href=back_href))




@app.post("/dq/contacts/person/{person_id}/delete")
async def dq_person_delete(person_id: int):
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    try:
        await pipedrive_delete_v2("persons", int(person_id), headers)
        await db_delete_person_cache(int(person_id))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/dq/contacts/person/{person_id}/delete_confirm", response_class=HTMLResponse)
async def dq_person_delete_confirm(person_id: int, back: str = ""):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    back_href = back if (back and isinstance(back, str) and back.startswith("/")) else "/overview"
    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Kontakt löschen</div>
          <div class="subtitle">ID: <code class="badge">{person_id}</code> – diese Aktion kann nicht rückgängig gemacht werden.</div>
        </div>
      </div>

      <div class="panel" style="border-color: rgba(239,68,68,.35);">
        <p style="margin:0 0 14px 0;">
          Soll der Kontakt wirklich in <b>Pipedrive</b> gelöscht werden?
        </p>
        <div style="display:flex; gap:10px; flex-wrap:wrap;">
          <a class="btn btn-outline" href="{html_escape(back_href)}">Abbrechen</a>
          <form method="post" action="/dq/contacts/person/{person_id}/delete_form" style="margin:0;">
            <input type="hidden" name="back" value="{html_escape(back_href)}"/>
            <button class="btn btn-primary" style="background:#ef4444; border-color:#ef4444;">🗑 Endgültig löschen</button>
          </form>
        </div>
      </div>
    """
    return HTMLResponse(page_shell("Kontakt löschen", body, back_href=back_href))


@app.post("/dq/contacts/person/{person_id}/delete_form")
async def dq_person_delete_form(person_id: int, back: str = Form(default="/overview")):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    headers = get_headers()
    if not headers:
        return RedirectResponse("/login")
    back_href = back if (back and isinstance(back, str) and back.startswith("/")) else "/overview"
    try:
        await pipedrive_delete_v2("persons", int(person_id), headers)
        await db_delete_person_cache(int(person_id))
    except Exception:
        pass
    return RedirectResponse(back_href, status_code=303)


@app.post("/dq/contacts/person/{person_id}/update")
async def dq_person_update(person_id: int, payload: dict = Body(...)):
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    first_name = (payload.get("first_name") or "").strip()
    last_name = (payload.get("last_name") or "").strip()
    email = (payload.get("email") or "").strip()

    gender_id = (payload.get("gender") or "").strip()
    du_sie_id = (payload.get("du_sie") or "").strip()

    position = (payload.get("position") or "").strip()
    linkedin = (payload.get("linkedin_url") or "").strip()

    patch: dict[str, Any] = {
        "first_name": (first_name if first_name != "" else None),
        "last_name": (last_name if last_name != "" else None),
        "emails": ([{"label": "work", "value": email, "primary": True}] if email else []),
        "custom_fields": {
            PD_PERSON_GENDER_KEY: (int(gender_id) if gender_id.isdigit() else None),
            PD_PERSON_DU_SIE_KEY: (int(du_sie_id) if du_sie_id.isdigit() else None),
            PD_PERSON_POSITION_KEY: (position if position != "" else None),
            PD_PERSON_LINKEDIN_KEY: (linkedin if linkedin != "" else None),
        },
    }

    patch["custom_fields"] = {k: v for k, v in patch["custom_fields"].items() if v is not None}

    try:
        await pipedrive_patch_v2("persons", person_id, patch, headers)
        await refresh_person_cache_from_api(person_id, headers)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

########################################################################
#
# Neue Bereiche
#
########################################################################



########################################################################
#
# Organisation Detail + Update
#
########################################################################

@app.get("/dq/orgs/org/{org_id}", response_class=HTMLResponse)
async def dq_org_detail(org_id: int, saved: int = 0, back: str = ""):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)

    org_map = await db_fetch_orgs_bulk([int(org_id)])
    o = org_map.get(int(org_id)) or {}

    def val(k: str) -> str:
        return html_escape((o.get(k) or "").strip())

    notice = ""
    if saved:
        notice = '<div class="panel" style="border-color: rgba(34,197,94,.35); background: rgba(34,197,94,.06);">✅ Gespeichert.</div>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Organisation bearbeiten</div>
          <div class="subtitle"><code class="badge">{org_id}</code></div>
        </div>
        <div style="display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-end;">
          <a class="chip chip-link" target="_blank" rel="noopener" href="{pipedrive_org_url(int(org_id))}">Pipedrive ↗</a>
          <a class="btn btn-outline" href="/overview">Übersicht</a>
        </div>
      </div>

      {notice}

      <div class="panel">
        <table>
          <tbody>
            <tr><th style="width:240px;">Name</th><td><input class="field-input" id="name" value="{val("name")}" /></td></tr>
            <tr><th>Adresse</th><td><input class="field-input" id="address" value="{val("address")}" /></td></tr>
            <tr><th>Website</th><td><input class="field-input" id="website" value="{val("website")}" /></td></tr>
          </tbody>
        </table>
        <div style="display:flex; gap:10px; margin-top:14px;">
          <button type="button" class="btn btn-primary" onclick="saveOrg({org_id})">Speichern</button>
        </div>
      </div>

      <script>
        async function saveOrg(orgId) {{
          const payload = {{
            name: document.getElementById("name").value || "",
            address: document.getElementById("address").value || "",
            website: document.getElementById("website").value || ""
          }};

          const res = await fetch(`/dq/orgs/org/${{orgId}}/update`, {{
            method:"POST",
            headers:{{"Content-Type":"application/json"}},
            body: JSON.stringify(payload)
          }});
          const data = await res.json().catch(()=>null);
          if(res.ok && data && data.ok) {{
            window.location.href = `/dq/orgs/org/${{orgId}}?saved=1`;
          }} else {{
            alert("❌ Fehler: " + ((data && data.error) ? data.error : ("HTTP " + res.status)));
          }}
        }}
      </script>
    """
    back_href = back if (back and isinstance(back,str) and back.startswith("/")) else "/overview"
    return HTMLResponse(page_shell("Organisation bearbeiten", body, back_href=back_href))


@app.post("/dq/orgs/org/{org_id}/update")
async def dq_org_update(org_id: int, payload: dict = Body(...)):
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    name = (payload.get("name") or "").strip()
    address = (payload.get("address") or "").strip()
    website = (payload.get("website") or "").strip()

    data = {
        "name": name,
        "address": address,
        "website": website,
    }

    try:
        await pipedrive_patch_v2("organizations", int(org_id), data, headers)
        await db_upsert_org_cache_partial(int(org_id), name=name, address=address, website=website)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    return JSONResponse({"ok": True})


def _extract_host_from_website(url: str) -> str:
    s = (url or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"^https?://", "", s)
    s = re.sub(r"^www\.", "", s)
    s = s.split("/")[0]
    s = s.split("?")[0]
    s = s.split("#")[0]
    s = s.split(":")[0]
    return s.strip()


def _email_domain(email: str) -> str:
    s = (email or "").strip().lower()
    if "@" not in s:
        return ""
    return s.split("@", 1)[1].strip()


def _org_name_tokens_for_domain(name: str) -> list[str]:
    """Tokens aus Organisationsname, die typischerweise in Domains vorkommen.

    - entfernt gängige Rechtsformen
    - extrahiert normale Tokens (>=4 Zeichen)
    - nimmt zusätzlich Akronyme (2–5 Buchstaben) mit (z.B. AXA, VWFS, IT, NRW)
    """
    raw = (name or "").strip()
    if not raw:
        return []
    s = raw.lower()

    # typische Rechtsformen entfernen (DE/EU/US grob)
    s = re.sub(r"(gmbh|ag|kg|ohg|ug|se|ltd|limited|inc\.?|corp\.?|llc|plc|bv|sarl|sas|sa|oy|ab|aps|as)", " ", s, flags=re.IGNORECASE)

    # Akronyme aus dem Original (vor lowercasing) ziehen
    acr = re.findall(r"[A-ZÄÖÜ]{2,5}", raw)
    acr = [a.lower() for a in acr]

    # Normalisieren zu Domain-ähnlichen Tokens
    s = re.sub(r"[^a-z0-9]+", " ", s)
    toks = [t for t in s.split() if len(t) >= 4]

    # Akronyme hinzufügen
    for a in acr:
        if 2 <= len(a) <= 5 and a not in toks:
            toks.append(a)

    # dedupe, keep order
    out: list[str] = []
    seen: set[str] = set()
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out
def _domain_root(host: str) -> str:
    """Heuristik fuer 'root domain' ohne Public Suffix List.

    Beispiele:
      - www.foo.bar -> foo.bar
      - foo.co.uk   -> foo.co.uk
      - vtours.com.br -> vtours.com.br
    """
    h = (host or '').strip().lower()
    if not h:
        return ''
    h = re.sub(r'^www\.', '', h)
    parts = [p for p in h.split('.') if p]
    if len(parts) < 2:
        return h

    cc = parts[-1]
    sld = parts[-2]
    # ccTLD-Heuristik (co.uk, com.br, ...)
    if len(cc) == 2 and sld in {'co','com','org','net','gov','ac'} and len(parts) >= 3:
        return '.'.join(parts[-3:])
    return '.'.join(parts[-2:])


def _domain_sld(root: str) -> str:
    """Registrable Label (Stamm) zur Root-Domain.

    Beispiele:
      - vertbaudet.de -> vertbaudet
      - vtours.com.br -> vtours
      - foo.co.uk -> foo
      - it.nrw -> it
    """
    r = (root or '').strip().lower()
    if not r or '.' not in r:
        return r
    parts = [p for p in r.split('.') if p]
    if len(parts) < 2:
        return r
    cc = parts[-1]
    sld = parts[-2]
    if len(cc) == 2 and sld in {'co','com','org','net','gov','ac'} and len(parts) >= 3:
        return parts[-3]
    return parts[-2]


def _edit_distance_leq1(a: str, b: str) -> bool:
    """Sehr schnelle 'Levenshtein <= 1' Prüfung (für Domain-Stämme).
    Erlaubt 0 oder 1 Edit (Insertion/Deletion/Substitution).
    """
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False

    i = j = 0
    edits = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
            continue
        edits += 1
        if edits > 1:
            return False
        if la == lb:
            i += 1
            j += 1
        elif la > lb:
            i += 1
        else:
            j += 1

    if i < la or j < lb:
        edits += 1
    return edits <= 1



def _matches_domain(email_dom: str, website_host: str) -> bool:
    """True, wenn Email-Domain zur Website-Domain passt.

    - exakte / Subdomain Matches (inkl. www.-Variante)
    - Root-Domain Match
    - TLD-Varianten Match (z.B. vtours.de vs vtours.com.br)
    """
    if not email_dom or not website_host:
        return False

    ed = (email_dom or '').strip().lower()
    wh = (website_host or '').strip().lower()

    if not ed or not wh:
        return False

    # exakte / Subdomain Matches
    if ed == wh:
        return True
    if ed.endswith('.' + wh) or wh.endswith('.' + ed):
        return True

    # Root-Domain vergleichen (z.B. a.b.com -> b.com)
    ed_root = _domain_root(ed)
    wh_root = _domain_root(wh)
    if ed_root and wh_root and ed_root == wh_root:
        return True

    # TLD-Varianten tolerieren: gleicher Stamm (SLD) reicht
    ed_sld = _domain_sld(ed_root)
    wh_sld = _domain_sld(wh_root)
    if ed_sld and wh_sld:
        if ed_sld == wh_sld and len(ed_sld) >= 2:
            return True
        # sehr kleine Tippfehler tolerieren (z.B. verbaudet vs vertbaudet)
        if min(len(ed_sld), len(wh_sld)) >= 6 and _edit_distance_leq1(ed_sld, wh_sld):
            return True

    return False



def _matches_name(email_dom: str, org_name: str) -> bool:
    if not email_dom or not org_name:
        return False
    toks = _org_name_tokens_for_domain(org_name)
    if not toks:
        return False
    ed = (email_dom or "").strip().lower()
    return any(t in ed for t in toks)



_email_mismatch_count_cache = {"ts": 0.0, "value": None}
async def db_count_email_mismatch(ttl_seconds: int = 300) -> Optional[int]:
    """Zählt E-Mail/Org-Mismatch über Heuristik (batch scan). Ergebnis wird kurz gecached."""
    if not db_pool:
        return None
    now = time.time()
    try:
        ts = float(_email_mismatch_count_cache.get("ts", 0.0))
    except Exception:
        ts = 0.0
    if _email_mismatch_count_cache.get("value") is not None and (now - ts) < ttl_seconds:
        return int(_email_mismatch_count_cache["value"])

    sql = """
    SELECT p.id, p.email, COALESCE(o.name,'') AS org_name, COALESCE(o.website,'') AS org_website
    FROM persons_cache p
    LEFT JOIN orgs_cache o ON o.id = p.org_id
    WHERE p.org_id IS NOT NULL
      AND p.email IS NOT NULL
      AND btrim(p.email) <> ''
      AND p.id > $1
    ORDER BY p.id
    LIMIT $2
    """

    batch = 5000
    last_id = 0
    total = 0
    async with db_pool.acquire() as conn:
        while True:
            rows = await conn.fetch(sql, last_id, batch)
            if not rows:
                break
            for r in rows:
                last_id = int(r["id"])
                email_dom = _email_domain((r.get("email") or "").strip())
                if not email_dom:
                    continue
                org_name = (r.get("org_name") or "").strip()
                org_website = (r.get("org_website") or "").strip()
                host = _extract_host_from_website(org_website)

                domain_ok = True
                name_ok = True
                if host:
                    domain_ok = _matches_domain(email_dom, host)
                if org_name:
                    name_ok = _matches_name(email_dom, org_name)

                if (host and (not domain_ok) and (not name_ok)) or ((not host) and org_name and (not name_ok)):
                    total += 1
    _email_mismatch_count_cache["ts"] = now
    _email_mismatch_count_cache["value"] = total
    return total

async def _db_collect_email_mismatch_rows(after_id: int, limit: int, scan_batch: int = 2000, max_batches: int = 20) -> tuple[list[dict], int]:
    """Heuristik-Scan: E-Mail-Domain passt nicht zur Website-Domain und/oder nicht zum Org-Namen."""
    out: list[dict] = []
    last_scanned = after_id

    sql = """
    SELECT p.id, p.first_name, p.last_name, p.email, p.org_id, COALESCE(o.name,'') AS org_name, COALESCE(o.website,'') AS org_website
    FROM persons_cache p
    LEFT JOIN orgs_cache o ON o.id = p.org_id
    WHERE p.org_id IS NOT NULL
      AND p.email IS NOT NULL
      AND btrim(p.email) <> ''
      AND p.id > $1
    ORDER BY p.id
    LIMIT $2
    """

    async with db_pool.acquire() as conn:
        for _ in range(max_batches):
            rows = await conn.fetch(sql, last_scanned, scan_batch)
            if not rows:
                break

            for r in rows:
                rid = int(r["id"])
                last_scanned = rid

                email = (r.get("email") or "").strip()
                email_dom = _email_domain(email)
                if not email_dom:
                    continue

                org_name = (r.get("org_name") or "").strip()
                org_website = (r.get("org_website") or "").strip()
                host = _extract_host_from_website(org_website)

                domain_ok = True
                name_ok = True

                # Domain-Check nur, wenn Website vorhanden
                if host:
                    domain_ok = _matches_domain(email_dom, host)

                # Name-Check nur, wenn Name vorhanden
                if org_name:
                    name_ok = _matches_name(email_dom, org_name)

                if (host and (not domain_ok) and (not name_ok)) or ((not host) and org_name and (not name_ok)):
                    reason = []
                    if host and not domain_ok:
                        reason.append("domain")
                    if org_name and not name_ok:
                        reason.append("name")
                    d = dict(r)
                    d["reason"] = ",".join(reason) if reason else "mismatch"
                    out.append(d)
                    if len(out) >= limit:
                        return out, last_scanned

    return out, last_scanned


@app.get("/dq/contacts/org/missing", response_class=HTMLResponse)
async def dq_contacts_missing_org(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/contacts/org/missing"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    sql = """
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE org_id IS NULL
      AND id > $1
    ORDER BY id
    LIMIT $2
    """

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, after_id, limit)

    trs = []
    last_id = after_id
    for r in rows:
        pid = int(r["id"])
        last_id = pid
        fn = (r["first_name"] or "").strip() or "-"
        ln = (r["last_name"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td style="width:48px; text-align:center;"><input class="rowchk" type="checkbox" value="{pid}"></td>
            <td style="width:120px;"><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:340px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/contacts/person/{pid}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_person_url(pid)}">Pipedrive ↗</a>
                  <a class="menu-item menu-danger" href="/dq/contacts/person/{pid}/delete_confirm?back={back_q}">🗑 Löschen</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = f'<a class="btn btn-outline" href="/dq/contacts/org/missing?after_id={last_id}&limit={limit}">Weiter →</a>' if rows else ""

    bulk_panel = f"""
      <div class="panel" style="margin-bottom:12px;">
        <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center; justify-content:space-between;">
          <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
            <label class="small" style="display:flex; align-items:center; gap:8px;">
              <input id="chk_all_rows" type="checkbox" onchange="toggleAllRows('chk_all_rows')">
              Alle auswählen
            </label>
            <button class="btn btn-outline" onclick="bulkExport('person', 'org_id')">Excel-Export</button>
          </div>
        </div>
      </div>
    """

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Kontakte – Keine Organisation</div>
          <div class="subtitle">Kontakte ohne zugeordnete Organisation </div>
        </div>
        <div style="display:flex; gap:10px;">{next_link}</div>
      </div>

      {bulk_panel}

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:48px; text-align:center;"><input id="chk_all_rows_header" type="checkbox" onchange="toggleAllRows('chk_all_rows_header')"></th>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:340px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="5">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Kontakte – Keine Organisation", body))


@app.get("/dq/orgs/no_contacts", response_class=HTMLResponse)
async def dq_orgs_no_contacts(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/orgs/no_contacts"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")


    sql = """
    SELECT o.id, o.name, o.website
    FROM orgs_cache o
    WHERE o.id > $1
      AND NOT EXISTS (SELECT 1 FROM persons_cache p WHERE p.org_id = o.id)
    ORDER BY o.id
    LIMIT $2
    """

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, after_id, limit)

    trs = []
    last_id = after_id
    for r in rows:
        oid = int(r["id"])
        last_id = oid
        name = (r["name"] or "").strip() or "-"
        website = (r["website"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td style="width:120px;"><code class="badge">{oid}</code></td>
            <td>{html_escape(name)}</td>
            <td>{html_escape(website)}</td>
            <td style="width:180px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/orgs/org/{oid}?back={back_q}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_org_url(oid)}">Pipedrive ↗</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = f'<a class="btn btn-outline" href="/dq/orgs/no_contacts?after_id={last_id}&limit={limit}">Weiter →</a>' if rows else ""

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Organisationen – Keine Kontakte</div>
          <div class="subtitle">Organisationen ohne zugeordnete Kontakte </div>
        </div>
        <div style="display:flex; gap:10px;">{next_link}</div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Name</th>
              <th>Website</th>
              <th style="width:180px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Organisationen – Keine Kontakte", body))


@app.get("/dq/contacts/email/mismatch", response_class=HTMLResponse)
async def dq_contacts_email_mismatch(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/contacts/email/mismatch"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    rows, next_after = await _db_collect_email_mismatch_rows(after_id, limit)

    trs = []
    for r in rows:
        pid = int(r["id"])
        fn = (r.get("first_name") or "").strip() or "-"
        ln = (r.get("last_name") or "").strip() or "-"
        email = (r.get("email") or "").strip() or "-"
        org_name = (r.get("org_name") or "").strip() or "-"
        org_website = (r.get("org_website") or "").strip() or "-"
        reason = (r.get("reason") or "").strip() or "mismatch"

        trs.append(f"""
          <tr>
            <td style="width:48px; text-align:center;"><input class="rowchk" type="checkbox" value="{pid}"></td>
            <td style="width:120px;"><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td>{html_escape(email)}</td>
            <td>{html_escape(org_name)}<div class="small" style="opacity:.85">{html_escape(org_website)}</div></td>
            <td style="width:120px;"><code class="badge">{html_escape(reason)}</code></td>
            <td style="width:340px;">
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/contacts/person/{pid}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_person_url(pid)}">Pipedrive ↗</a>
                  <a class="menu-item menu-danger" href="/dq/contacts/person/{pid}/delete_confirm?back={back_q}">🗑 Löschen</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = ""
    if next_after and next_after > after_id:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/email/mismatch?after_id={next_after}&limit={limit}">Weiter →</a>'

    bulk_panel = f"""
      <div class="panel" style="margin-bottom:12px;">
        <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center; justify-content:space-between;">
          <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
            <label class="small" style="display:flex; align-items:center; gap:8px;">
              <input id="chk_all_rows" type="checkbox" onchange="toggleAllRows('chk_all_rows')">
              Alle auswählen
            </label>
            <button class="btn btn-outline" onclick="bulkExport('person', 'email')">Excel-Export</button>
          </div>
        </div>
      </div>
    """

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Kontakte – E-Mail passt nicht zur Organisation</div>
          <div class="subtitle">Heuristik: Domain passt nicht zur Website-Domain und/oder nicht zum Organisationsnamen </div>
        </div>
        <div style="display:flex; gap:10px;">{next_link}</div>
      </div>

      {bulk_panel}

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:48px; text-align:center;"><input id="chk_all_rows_header" type="checkbox" onchange="toggleAllRows('chk_all_rows_header')"></th>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th>E-Mail</th>
              <th>Organisation</th>
              <th style="width:120px;">Grund</th>
              <th style="width:340px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="8">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Kontakte – E-Mail passt nicht", body))


########################################################################
#
# Organisationen: Missing + Invalidchars
#
########################################################################

@app.get("/dq/orgs/missing", response_class=HTMLResponse)
async def dq_orgs_missing(field: str, after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = f"/dq/orgs/missing?field={urllib.parse.quote(field, safe='')}"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("&" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    if field not in ("name", "address", "website"):
        return HTMLResponse("Ungültiges Feld", status_code=400)

    sql = f"""
    SELECT id, name, address, website
    FROM orgs_cache
    WHERE ({field} IS NULL OR btrim({field}) = '')
      AND id > $1
    ORDER BY id
    LIMIT $2
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, after_id, limit)

    trs = []
    last_id = after_id
    for r in rows:
        oid = int(r["id"])
        last_id = oid
        name = (r["name"] or "").strip() or "-"
        val = (r[field] or "").strip() if r.get(field) else ""
        trs.append(f"""
          <tr>
            <td><code class="badge">{oid}</code></td>
            <td>{html_escape(name)}</td>
            <td>
              <div class="mono">{html_escape(val) or "-"}</div>
              
            </td>
            <td>
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/orgs/org/{oid}?back={back_q}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_org_url(oid)}">Pipedrive ↗</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="/dq/orgs/missing?field={html_escape(field)}&after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Organisationen – Fehlende Daten</div>
          <div class="subtitle">Feld: {html_escape(field)} </div>
        </div>
        <div style="display:flex; gap:10px;">          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Name</th>
              <th>Wert</th>
              <th style="width:180px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Organisationen – Fehlende Daten", body))


@app.get("/dq/orgs/invalidchars", response_class=HTMLResponse)
async def dq_orgs_invalidchars(field: str, after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    base_path = "/dq/orgs/invalidchars"
    qs = []
    if after_id:
        qs.append(f"after_id={after_id}")
    if limit:
        qs.append(f"limit={limit}")
    current_url = base_path + (("?" + "&".join(qs)) if qs else "")
    back_q = urllib.parse.quote(current_url, safe="")

    if field != "name":
        return HTMLResponse("Invalidchars ist nur für name sinnvoll", status_code=400)

    # Für Org-Namen lassen wir hier bewusst mehr zu (z.B. Zahlen, & etc.) – wir flaggen nur Emojis/Steuerzeichen.
    sql = """
    SELECT id, name
    FROM orgs_cache
    WHERE name IS NOT NULL
      AND btrim(name) <> ''
      AND id > $1
    ORDER BY id
    LIMIT $2
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, after_id, limit)

    def org_name_invalid(s: str) -> bool:
        t = unicodedata.normalize("NFKC", (s or "")).strip()
        if not t:
            return False
        for ch in t:
            cat = unicodedata.category(ch)
            if cat and cat[0] == "C":
                return True
            # Emoji/Symbole grob: 'So' etc.
            if cat and cat[0] == "S":
                return True
        return False

    bad = []
    last_id = after_id
    for r in rows:
        oid = int(r["id"])
        last_id = oid
        if org_name_invalid(r["name"] or ""):
            bad.append(r)

    trs = []
    for r in bad:
        oid = int(r["id"])
        name = (r["name"] or "").strip() or "-"
        trs.append(f"""
          <tr>
            <td><code class="badge">{oid}</code></td>
            <td>{html_escape(name)}</td>
            <td>
              <div class="mono">{html_escape(name) or "-"}</div>
              
            </td>
            <td>
              <details class="action-menu">
                 <summary class="chip chip-primary action-btn">⋯ Aktionen</summary>
                 <div class="menu" role="menu">
                  <a class="menu-item" href="/dq/orgs/org/{oid}?back={back_q}">Bearbeiten</a>
                  <a class="menu-item" target="_blank" rel="noopener" href="{pipedrive_org_url(oid)}">Pipedrive ↗</a>
                </div>
               </details>
             </td>
          </tr>
        """)

    next_link = f'<a class="btn btn-outline" href="/dq/orgs/invalidchars?field=name&after_id={last_id}&limit={limit}">Weiter →</a>' if rows else ""

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Organisationen – Ungültige Zeichen</div>
          <div class="subtitle">Feld: name </div>
        </div>
        <div style="display:flex; gap:10px;">          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Name</th>
              <th>Wert</th>
              <th style="width:180px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer.</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Organisationen – Ungültige Zeichen", body))

########################################################################
#
# Update Endpoint (Inline Edit)
#
########################################################################

@app.post("/dq/update")
async def dq_update(payload: dict = Body(...)):
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    entity_type = (payload.get("entity_type") or "").strip().lower()
    entity_id = payload.get("entity_id")
    field_key = payload.get("field_key")
    value = payload.get("value")

    if entity_type not in ("person", "organization"):
        return JSONResponse({"ok": False, "error": "entity_type muss 'person' oder 'organization' sein"}, status_code=400)
    if not isinstance(entity_id, int):
        return JSONResponse({"ok": False, "error": "entity_id muss int sein"}, status_code=400)
    if not field_key or not isinstance(field_key, str):
        return JSONResponse({"ok": False, "error": "field_key fehlt"}, status_code=400)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    entity_endpoint = "persons" if entity_type == "person" else "organizations"
    patch_fragment = normalize_update_payload_v2(entity_type, field_key, value)

    try:
        result = await pipedrive_patch_v2(entity_endpoint, entity_id, patch_fragment, headers)
    except Exception as e:
        # Fallback für org.address als Objekt
        if entity_type == "organization" and field_key == "address":
            v = (value or "").strip()
            alt_payload = {"address": ({"value": v} if v else None)}
            try:
                result = await pipedrive_patch_v2(entity_endpoint, entity_id, alt_payload, headers)
            except Exception:
                return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        else:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    # Cache nachziehen
    if db_pool and entity_type == "organization":
        v = (value or "").strip()
        if field_key == "name":
            await db_upsert_org_cache_partial(entity_id, name=v)
        elif field_key == "address":
            await db_upsert_org_cache_partial(entity_id, address=v)
        elif field_key == "website":
            await db_upsert_org_cache_partial(entity_id, website=v)
        else:
            await db_upsert_org_cache_partial(entity_id)

    if db_pool and entity_type == "person":
        # Für Personen refreshen wir on-demand beim Öffnen; hier optional:
        pass

    return JSONResponse({"ok": True, "result": result.get("data") or result})

########################################################################
#
# Lokaler Start
#
########################################################################
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
