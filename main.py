import os
import re
import json
import httpx
import asyncio
import asyncpg
import unicodedata
from typing import Any, Optional

from fastapi import FastAPI, Request, Body
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime, timezone, timedelta

app = FastAPI()

########################################################################
#
# Konfiguration
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

def page_shell(title: str, body_html: str) -> str:
    logo_html = ""
    if os.path.isfile("static/bizforward-Logo-Clean-2024.svg"):
        logo_html = '<header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>'
    else:
        logo_html = '<header><div style="font-weight:900;letter-spacing:.2px">bizforward · Datenqualität</div></header>'

    return f"""
    <html>
    <head>
      <meta charset="utf-8"/>
      <meta name="viewport" content="width=device-width, initial-scale=1"/>
      <title>{html_escape(title)}</title>
      <link rel="stylesheet" href="/static/app.css?v={CSS_VERSION}">
    </head>
    <body>
      {logo_html}
      <div class="container">
        {body_html}
      </div>
      <script>
        async function updateField(entityType, id, fieldKey){{
          const inp = document.getElementById(`inp_${{entityType}}_${{id}}_${{fieldKey}}`);
          const val = inp ? inp.value : "";
          if(!confirm("Wirklich in Pipedrive aktualisieren?")) return;

          const res = await fetch("/dq/update", {{
            method:"POST",
            headers:{{"Content-Type":"application/json"}},
            body: JSON.stringify({{
              entity_type: entityType,
              entity_id: parseInt(id),
              field_key: fieldKey,
              value: val
            }})
          }});
          const data = await res.json().catch(()=>null);
          if(data && data.ok){{
            alert("✅ Aktualisiert.");
          }} else {{
            alert("❌ Fehler: " + ((data && data.error) ? data.error : ("HTTP " + res.status)));
          }}
        }}
      </script>
    </body>
    </html>
    """


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

    # Freelancer (Organisation = "Freelancer")
    {"group": "Freelancer", "title": "Vorname", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/first_name/missing"}]},
    {"group": "Freelancer", "title": "Nachname", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/last_name/missing"}]},
    {"group": "Freelancer", "title": "Geschlecht", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/gender/missing"}]},
    {"group": "Freelancer", "title": "E-Mail-Adresse", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/email/missing"}]},
    {"group": "Freelancer", "title": "Du oder Sie", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/du_sie/missing"}]},
    {"group": "Freelancer", "title": "Position", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/position/missing"}]},
    {"group": "Freelancer", "title": "LinkedIn-URL", "description": "Organisation = Freelancer", "actions": [{"label": "Fehlende Daten", "href": "/dq/freelancers/linkedin/missing"}]},

    # Orgs
    {"group": "Organisationen", "title": "Name / Rechtsform", "description": "", "actions": [
        {"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=name"},
        {"label": "Ungültige Zeichen", "href": "/dq/orgs/invalidchars?field=name"},
    ]},
    {"group": "Organisationen", "title": "Adresse", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=address"}]},
    {"group": "Organisationen", "title": "Website", "description": "", "actions": [{"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=website"}]},
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
        total = 0
        has_any = False

        for a in c.get("actions", []):
            href = a["href"]
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

    def _group_total(group_name: str) -> Optional[int]:
        total = 0
        seen_any = False
        for c in DQ_CARDS:
            if c.get("group") != group_name:
                continue
            for a in c.get("actions", []):
                n = counts.get(a.get("href"))
                if isinstance(n, int):
                    total += n
                    seen_any = True
        return total if seen_any else None

    def _tot_badge(n: Optional[int]) -> str:
        if n is None:
            return ""
        return f'<span style="margin-left:10px; padding:4px 10px; border-radius:999px; font-size:12px; background:rgba(15,23,42,.08); color:#0f172a;">Summe: <b>{int(n)}</b></span>'

    total_contacts = _group_total("Kontakte")
    total_freelancers = _group_total("Freelancer")
    total_orgs = _group_total("Organisationen")

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Datenqualität – Übersicht</div>
          <div class="subtitle">
            Kontakte = Personen <b>ohne</b> Organisation „{html_escape(FREELANCER_ORG_NAME)}“ ·
            Freelancer = Personen mit Organisation „{html_escape(FREELANCER_ORG_NAME)}“
          </div>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
          <a class="btn btn-outline" href="/admin">Admin</a>
          <a class="btn btn-outline" href="/logout">Logout</a>
        </div>
      </div>

      <div class="panel" style="margin-bottom:14px;">
        <div class="small" style="display:flex; flex-wrap:wrap; gap:12px; align-items:center;">
          <div><b>Gesamt:</b></div>
          <div>Kontakte{_tot_badge(total_contacts)}</div>
          <div>Freelancer{_tot_badge(total_freelancers)}</div>
          <div>Organisationen{_tot_badge(total_orgs)}</div>
        </div>
      </div>

      {_render_cards("Kontakte", counts)}
      {_render_cards("Freelancer", counts)}
      {_render_cards("Organisationen", counts)}
    """
    return HTMLResponse(page_shell("Datenqualität – Übersicht", body))

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
      <div style="display:flex; gap:10px;">
        <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
      </div>
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
    freelancer_mode: str = "exclude",
) -> HTMLResponse:
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
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="{base_path}?after_id={last_id}&limit={limit}">Weiter →</a>'

    subtitle = "Kontakte (ohne Freelancer)" if freelancer_mode == "exclude" else ("Nur Freelancer" if freelancer_mode == "only" else "Alle Kontakte")

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">{html_escape(title)}</div>
          <div class="subtitle">{html_escape(subtitle)} · Liste aus Cache-DB · Page size: {limit}</div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:160px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer (oder Cache noch nicht vollständig).</td></tr>'}
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
    return await _render_missing_list("Vorname – Fehlende Daten", "/dq/contacts/first_name/missing", after_id, limit, sql, freelancer_mode="exclude")


@app.get("/dq/contacts/last_name/missing", response_class=HTMLResponse)
async def dq_last_name_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("last_name", freelancer_mode="exclude")
    return await _render_missing_list("Nachname – Fehlende Daten", "/dq/contacts/last_name/missing", after_id, limit, sql, freelancer_mode="exclude")


@app.get("/dq/contacts/gender/missing", response_class=HTMLResponse)
async def dq_gender_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("gender", freelancer_mode="exclude")
    return await _render_missing_list("Geschlecht – Fehlende Daten", "/dq/contacts/gender/missing", after_id, limit, sql, freelancer_mode="exclude")


@app.get("/dq/contacts/email/missing", response_class=HTMLResponse)
async def dq_email_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("email", freelancer_mode="exclude")
    return await _render_missing_list("E-Mail – Fehlende Daten", "/dq/contacts/email/missing", after_id, limit, sql, freelancer_mode="exclude")


@app.get("/dq/contacts/du_sie/missing", response_class=HTMLResponse)
async def dq_du_sie_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("du_sie", freelancer_mode="exclude")
    return await _render_missing_list("Du oder Sie – Fehlende Daten", "/dq/contacts/du_sie/missing", after_id, limit, sql, freelancer_mode="exclude")


@app.get("/dq/contacts/position/missing", response_class=HTMLResponse)
async def dq_position_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("position", freelancer_mode="exclude")
    return await _render_missing_list("Position – Fehlende Daten", "/dq/contacts/position/missing", after_id, limit, sql, freelancer_mode="exclude")


@app.get("/dq/contacts/linkedin/missing", response_class=HTMLResponse)
async def dq_linkedin_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("linkedin_url", freelancer_mode="exclude")
    return await _render_missing_list("LinkedIn-URL – Fehlende Daten", "/dq/contacts/linkedin/missing", after_id, limit, sql, freelancer_mode="exclude")

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
    return await _render_missing_list("Freelancer – Vorname (fehlend)", "/dq/freelancers/first_name/missing", after_id, limit, sql, freelancer_mode="only")


@app.get("/dq/freelancers/last_name/missing", response_class=HTMLResponse)
async def dq_freelancers_last_name_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("last_name", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Nachname (fehlend)", "/dq/freelancers/last_name/missing", after_id, limit, sql, freelancer_mode="only")


@app.get("/dq/freelancers/gender/missing", response_class=HTMLResponse)
async def dq_freelancers_gender_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("gender", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Geschlecht (fehlend)", "/dq/freelancers/gender/missing", after_id, limit, sql, freelancer_mode="only")


@app.get("/dq/freelancers/email/missing", response_class=HTMLResponse)
async def dq_freelancers_email_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("email", freelancer_mode="only")
    return await _render_missing_list("Freelancer – E-Mail (fehlend)", "/dq/freelancers/email/missing", after_id, limit, sql, freelancer_mode="only")


@app.get("/dq/freelancers/du_sie/missing", response_class=HTMLResponse)
async def dq_freelancers_du_sie_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("du_sie", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Du/Sie (fehlend)", "/dq/freelancers/du_sie/missing", after_id, limit, sql, freelancer_mode="only")


@app.get("/dq/freelancers/position/missing", response_class=HTMLResponse)
async def dq_freelancers_position_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("position", freelancer_mode="only")
    return await _render_missing_list("Freelancer – Position (fehlend)", "/dq/freelancers/position/missing", after_id, limit, sql, freelancer_mode="only")


@app.get("/dq/freelancers/linkedin/missing", response_class=HTMLResponse)
async def dq_freelancers_linkedin_missing(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    sql = _dq_missing_sql_for_column("linkedin_url", freelancer_mode="only")
    return await _render_missing_list("Freelancer – LinkedIn (fehlend)", "/dq/freelancers/linkedin/missing", after_id, limit, sql, freelancer_mode="only")

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
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
          </tr>
        """)

    next_link = ""
    if next_after and next_after > after_id:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/first_name/invalidchars?after_id={next_after}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Vorname – Ungültige Zeichen</div>
          <div class="subtitle">Kontakte (ohne Freelancer) · Liste aus Cache-DB · Page size: {limit}</div>
          <div class="subtitle"><span class="small">Erlaubt: Buchstaben inkl. Akzente, Leerzeichen, Bindestrich, Punkt, Apostroph. Nicht erlaubt: Emojis, Zahlen, Steuerzeichen.</span></div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:160px;">Aktion</th>
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
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
          </tr>
        """)

    next_link = ""
    if next_after and next_after > after_id:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/last_name/invalidchars?after_id={next_after}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Nachname – Ungültige Zeichen</div>
          <div class="subtitle">Kontakte (ohne Freelancer) · Liste aus Cache-DB · Page size: {limit}</div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:160px;">Aktion</th>
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
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
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
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
          {next_link}
        </div>
      </div>

      <div class="panel">
        <table>
          <thead>
            <tr>
              <th style="width:120px;">ID</th>
              <th>Vorname</th>
              <th>Nachname</th>
              <th style="width:160px;">Aktion</th>
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
async def dq_person_detail(person_id: int, saved: int = 0):
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
    labels_text = ", ".join(str(x) for x in label_ids) if label_ids else "-"

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
          <div class="title">Kontakt bearbeiten</div>
          <div class="subtitle"><code class="badge">{person_id}</code> · Organisation: <b>{html_escape(p.get("org_name") or "-")}</b></div>
        </div>
        <div style="display:flex; gap:10px;">
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

            <tr><th>Labels (IDs)</th><td><input class="field-input" value="{html_escape(labels_text)}" disabled /></td></tr>
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
    return HTMLResponse(page_shell("Kontakt bearbeiten", body))


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
              <input class="field-input" id="inp_organization_{oid}_{field}" value="{html_escape(val)}" />
              <div class="small">Aktueller Wert (editierbar)</div>
            </td>
            <td>
              <button class="btn btn-primary" onclick="updateField('organization','{oid}','{field}')">Aktualisieren</button>
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
          <div class="subtitle">Feld: {html_escape(field)} · Liste aus Cache-DB · Page size: {limit}</div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
          {next_link}
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
              <input class="field-input" id="inp_organization_{oid}_name" value="{html_escape(name)}" />
              <div class="small">Aktueller Wert (editierbar)</div>
            </td>
            <td>
              <button class="btn btn-primary" onclick="updateField('organization','{oid}','name')">Aktualisieren</button>
            </td>
          </tr>
        """)

    next_link = f'<a class="btn btn-outline" href="/dq/orgs/invalidchars?field=name&after_id={last_id}&limit={limit}">Weiter →</a>' if rows else ""

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Organisationen – Ungültige Zeichen</div>
          <div class="subtitle">Feld: name · Liste aus Cache-DB · Page size: {limit}</div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
          {next_link}
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
