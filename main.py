import os
import re
import json
import httpx
import asyncio
import asyncpg
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

def _utcnow():
    return datetime.now(timezone.utc)

def _parse_ts(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        # Pipedrive liefert oft ISO mit Z
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

        # Personen-Indizes (schnell für DQ-Listen)
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

        # Org-Indizes
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_orgs_name
        ON orgs_cache (name);
        """)


async def get_sync_time(entity: str) -> datetime:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT last_update_time FROM sync_state WHERE entity=$1", entity)
        if row and row["last_update_time"]:
            return row["last_update_time"]
        # Default: sehr alt (Initial-Sync)
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
            UPDATE sync_state
            SET last_cursor=$2, full_in_progress=$3
            WHERE entity=$1
        """, entity, cursor, in_progress)


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
# LogIn
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
        return HTMLResponse(f"<h3>❌ Fehler beim Login: {token_data}</h ALIGN=left 3>")
    user_tokens["default"] = access_token
    return RedirectResponse("/overview")


def get_headers() -> dict:
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}


def extract_address(address_value):
    """API v2 liefert 'address' als Objekt; wir wollen für die UI einen String."""
    if isinstance(address_value, dict):
        return address_value.get("value") or "-"
    return address_value or "-"


########################################################################
#
# KONSTANTEN / VARIABLEN
#
########################################################################

CSS_VERSION = "1"  # hochzählen, wenn du CSS änderst (Cache-Busting)

# Kontakt-Feldkeys
PD_PERSON_GENDER_KEY = "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72"
PD_PERSON_DU_SIE_KEY = "1fde2275ff2973c9062d64f1612122384b5902cf"
PD_PERSON_POSITION_KEY = "4585e5de11068a3bccf02d8b93c126bcf5c257ff"
PD_PERSON_LINKEDIN_KEY = "25563b12f847a280346bba40deaf527af82038cc"

# Org-Feldkeys (v2 Standard)
PD_ORG_NAME_KEY = "name"
PD_ORG_ADDRESS_KEY = "address"
PD_ORG_WEBSITE_KEY = "website"

# Allowed chars (A–Z + Umlaute + Leerzeichen + Bindestrich + Apostroph)
NAME_ALLOWED_REGEX = re.compile(r"^[A-Za-zÄÖÜäöüß\s\-']+$")

# Titel-Erkennung im Vornamen
TITLE_PREFIX_REGEX = re.compile(
    r"^\s*(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?|herr|frau)\b",
    re.IGNORECASE,
)


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
    {
        "group": "Kontakte",
        "title": "Geschlecht",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/gender/missing"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "E-Mail-Adresse",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/email/missing"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Du oder Sie",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/du_sie/missing"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Position",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/position/missing"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "LinkedIn-URL",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/linkedin/missing"},
        ],
    },

    # Orgs bleiben vorerst, aber siehe Punkt 3 (Org-Cache erweitern!)
    {
        "group": "Organisationen",
        "title": "Name / Rechtsform",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=name"},
            {"label": "Ungültige Zeichen", "href": "/dq/orgs/invalidchars?field=name"},
        ],
    },
    {
        "group": "Organisationen",
        "title": "Adresse",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=address"},
        ],
    },
    {
        "group": "Organisationen",
        "title": "Website",
        "description": "",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/orgs/missing?field=website"},
        ],
    },
]

########################################################################
#
#  Pipedrive Fetch Helpers
#
########################################################################

def pd_get_value_v2(entity: dict, field_key: str):
    """
    v2: Standard-Felder liegen auf Root-Level (z.B. first_name, last_name, emails, label_ids, org_id ...)
    v2: Custom Fields liegen unter entity["custom_fields"][<key>]
    """
    if not entity:
        return None

    # Root fields we use in UI/logic
    ROOT_FIELDS = {
        "id", "name", "first_name", "last_name", "org_id",
        "emails", "phones", "label_ids", "visible_to",
        "add_time", "update_time"
    }
    if field_key in ROOT_FIELDS:
        return entity.get(field_key)

    # Custom fields (v2)
    cf = entity.get("custom_fields") or {}
    return cf.get(field_key)


def normalize_person_update_payload_v2(field_key: str, value: str) -> dict:
    """
    Baut ein v2-konformes PATCH-Payload-Fragment.
    - emails: array of objects
    - custom fields: unter custom_fields
    - single option custom fields: int (wenn möglich)
    """
    v = (value or "").strip()

    # 1) emails (v2)
    if field_key == "emails":
        if not v:
            return {"emails": []}
        # Single primary mail setzen
        return {"emails": [{"value": v, "primary": True}]}

    # 2) label_ids (v2 root field, array)
    if field_key == "label_ids":
        # Erwartung: "1,2,3" oder "" (UI kannst du später schöner machen)
        if not v:
            return {"label_ids": []}
        ids = []
        for part in v.split(","):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))
        return {"label_ids": ids}

    # 3) Custom fields (v2)
    SINGLE_OPTION_CUSTOM_FIELDS = {
        PD_PERSON_GENDER_KEY,
        PD_PERSON_DU_SIE_KEY,
    }

    if field_key in SINGLE_OPTION_CUSTOM_FIELDS:
        # v2: option id als Zahl (int) :contentReference[oaicite:6]{index=6}
        if not v:
            cf_val = None
        elif v.isdigit():
            cf_val = int(v)
        else:
            # Falls UI mal Labels schickt: hier NICHT mappen, sondern sauber im UI IDs verwenden
            cf_val = None
        return {"custom_fields": {field_key: cf_val}}

    # Text/sonstige Custom Fields
    # (Position, Linkedin etc.)
    if field_key in {
        PD_PERSON_POSITION_KEY,
        PD_PERSON_LINKEDIN_KEY,
        PD_PERSON_GENDER_KEY,
        PD_PERSON_DU_SIE_KEY,
    }:
        return {"custom_fields": {field_key: (v if v else None)}}

    # Default: Root-Feld direkt patchen
    return {field_key: (v if v else None)}

async def fetch_all_v2(endpoint: str, headers: dict, params: Optional[dict] = None) -> list[dict]:
    """
    Cursor-basierte Pagination (v2) – gibt alle items als Liste zurück.
    Tipp Performance:
    - für persons/organizations NUR benötigte custom_fields via params["custom_fields"] (max 15 keys) :contentReference[oaicite:7]{index=7}
    """
    if params is None:
        params = {}

    out: list[dict] = []
    limit = int(params.get("limit") or 500)
    cursor = None

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            p = dict(params)
            p["limit"] = limit
            if cursor:
                p["cursor"] = cursor

            resp = await client.get(f"{PIPEDRIVE_API_V2_URL}/{endpoint}", headers=headers, params=p)
            if resp.status_code != 200:
                raise RuntimeError(f"Pipedrive API Fehler ({resp.status_code}): {resp.text}")

            data = resp.json() or {}
            items = data.get("data") or []
            if not items:
                break

            out.extend(items)
            cursor = (data.get("additional_data") or {}).get("next_cursor")
            if not cursor:
                break

    return out



def _scalarize(v: Any) -> str:
    """Versucht, den Wert UI-tauglich als String zu machen (inkl. v2 email/list/dict)."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        # häufig {value: "..."} bei address o.ä.
        if "value" in v and isinstance(v.get("value"), str):
            return v.get("value") or ""
        # fallback
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        # Pipedrive: email kann Liste von Objekten sein
        vals = []
        for item in v:
            if isinstance(item, dict) and "value" in item:
                vals.append(str(item.get("value") or "").strip())
            else:
                vals.append(str(item).strip())
        vals = [x for x in vals if x]
        return ", ".join(vals)
    return str(v)


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    if isinstance(v, dict):
        # z.B. address={value: "..."} -> missing, wenn value leer
        return _scalarize(v).strip() == ""
    if isinstance(v, list):
        if len(v) == 0:
            return True
        return all(_is_missing(x) for x in v)
    return False


def _has_invalid_name_chars(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    return NAME_ALLOWED_REGEX.match(t) is None


async def pipedrive_update_v2(entity: str, entity_id: int, payload: dict, headers: dict) -> dict:
    """
    Update helper: v2 ist je nach Endpoint PATCH oder PUT.
    Wir versuchen PATCH und fallen bei Bedarf auf PUT zurück.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.patch(f"{PIPEDRIVE_API_V2_URL}/{entity}/{entity_id}", headers=headers, json=payload)
        if r.status_code in (200, 201):
            return r.json()
        # fallback
        r2 = await client.put(f"{PIPEDRIVE_API_V2_URL}/{entity}/{entity_id}", headers=headers, json=payload)
        if r2.status_code in (200, 201):
            return r2.json()
        raise RuntimeError(f"Update fehlgeschlagen ({r2.status_code}): {r2.text}")


async def iter_v2_pages(endpoint: str, headers: dict, params: Optional[dict] = None, max_pages: int = 20):
    """
    Iteriert cursor-basiert über v2 endpoints.
    - max_pages: Begrenze Seiten pro Run (wichtig für Render-Timeouts).
      max_pages=0 => unbegrenzt
    """
    params = params or {}
    cursor = None
    pages = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            p = dict(params)
            p["limit"] = int(p.get("limit") or 500)
            if cursor:
                p["cursor"] = cursor

            resp = await client.get(f"{PIPEDRIVE_API_V2_URL}/{endpoint}", headers=headers, params=p)
            if resp.status_code != 200:
                raise RuntimeError(f"Pipedrive API Fehler ({resp.status_code}): {resp.text}")

            payload = resp.json() or {}
            items = payload.get("data") or []
            add = payload.get("additional_data") or {}
            cursor = add.get("next_cursor")

            if items:
                yield items

            pages += 1
            if max_pages and pages >= max_pages:
                break

            if not cursor:
                break


def _get_org_id_from_person(p: dict) -> Optional[int]:
    org_id = p.get("org_id") or p.get("organization_id")
    if isinstance(org_id, dict):
        org_id = org_id.get("value") or org_id.get("id")
    try:
        return int(org_id) if org_id is not None else None
    except Exception:
        return None


def _email_first(p: dict) -> str:
    # p['email'] kann str, list[dict], list[str] sein
    val = _scalarize(p.get("email")).strip()
    if not val:
        return ""
    return val.split(",")[0].strip()


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



async def upsert_persons(batch: list[dict]) -> Optional[datetime]:
    if not batch:
        return None

    rows = []
    max_ts: Optional[datetime] = None

    for p in batch:
        pid = p.get("id")
        if pid is None:
            continue

        ts = _parse_ts(p.get("update_time") or p.get("updated_at") or p.get("updateTime"))
        if ts and (max_ts is None or ts > max_ts):
            max_ts = ts

        custom = p.get("custom_fields") or {}

        email_primary = _primary_from_list(p.get("emails"))  # v2: emails (list of objects)

        gender_id = custom.get(PD_PERSON_GENDER_KEY)
        du_sie_id = custom.get(PD_PERSON_DU_SIE_KEY)

        rows.append((
            int(pid),
            _scalarize(p.get("first_name")).strip(),
            _scalarize(p.get("last_name")).strip(),
            (str(gender_id) if gender_id is not None else ""),   # store option id as string
            email_primary,
            (str(du_sie_id) if du_sie_id is not None else ""),   # store option id as string
            _scalarize(custom.get(PD_PERSON_POSITION_KEY)).strip(),
            _scalarize(custom.get(PD_PERSON_LINKEDIN_KEY)).strip(),
            _get_org_id_from_person(p),
            ts,
            _label_ids_list(p),  # v2: label_ids is root field (list[int])
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

    rows = []
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
            (addr if addr != "-" else ""),
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


async def sync_persons_incremental(full: bool = False, max_pages: int = 20) -> dict:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    # Wir wollen diese Custom Fields im Cache haben:
    custom_keys = [
        PD_PERSON_GENDER_KEY,
        PD_PERSON_DU_SIE_KEY,
        PD_PERSON_POSITION_KEY,
        PD_PERSON_LINKEDIN_KEY,
    ]

    params = {
        "limit": 500,
        # v2: custom_fields als comma-separated string
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

    params = {"limit": 500}

    cursor = None
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

            resp = await client.get(f"{PIPEDRIVE_API_V2_URL}/organizations", headers=headers, params=p)
            if resp.status_code != 200:
                raise RuntimeError(f"Pipedrive API Fehler ({resp.status_code}): {resp.text}")

            payload = resp.json() or {}
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
        "new_sync_time": max_seen.isoformat() if max_seen else None
    }

@app.get("/admin/sync/reset")
async def admin_sync_reset(entity: str = "persons"):
    if not db_pool:
        return {"ok": False, "error": "DB nicht initialisiert"}
    if entity not in ("persons", "organizations"):
        return {"ok": False, "error": "entity muss 'persons' oder 'organizations' sein"}

    async with db_pool.acquire() as conn:
        await conn.execute("""
          UPDATE sync_state
          SET last_update_time=$2, last_cursor=NULL, full_in_progress=FALSE
          WHERE entity=$1
        """, entity, datetime(1970,1,1,tzinfo=timezone.utc))

    return {"ok": True, "entity": entity, "reset": True}

def _primary_from_list(items: Any) -> str:
    """
    v2 emails/phones sind Listen von Objekten:
    [{"label":"work","value":"x","primary":true}, ...]
    """
    if not isinstance(items, list) or not items:
        return ""
    # primary zuerst
    for it in items:
        if isinstance(it, dict) and it.get("primary") and it.get("value"):
            return str(it.get("value") or "").strip()
    # sonst erstes mit value
    for it in items:
        if isinstance(it, dict) and it.get("value"):
            return str(it.get("value") or "").strip()
    return ""


async def pipedrive_get_person_v2(person_id: int, headers: dict) -> dict:
    """
    Holt eine Person aus API v2 inkl. ausgewählter custom fields (max 15 keys).
    """
    custom_keys = ",".join(
        [
            PD_PERSON_GENDER_KEY,
            PD_PERSON_DU_SIE_KEY,
            PD_PERSON_POSITION_KEY,
            PD_PERSON_LINKEDIN_KEY,
        ]
    )

    params = {"custom_fields": custom_keys}

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(
            f"{PIPEDRIVE_API_V2_URL}/persons/{person_id}",
            headers=headers,
            params=params,
        )
    if r.status_code != 200:
        raise RuntimeError(f"Person GET fehlgeschlagen ({r.status_code}): {r.text}")

    data = r.json() or {}
    return data.get("data") or {}

_PERSON_FIELD_CACHE: dict[str, dict] = {}


async def pipedrive_get_person_field_v2(field_code: str, headers: dict) -> dict:
    """
    Holt Definition eines Person-Feldes (inkl. options) aus Fields API v2.
    field_code ist bei Custom Fields i.d.R. dein Hash-Key.
    """
    if field_code in _PERSON_FIELD_CACHE:
        return _PERSON_FIELD_CACHE[field_code]

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{PIPEDRIVE_API_V2_URL}/personFields/{field_code}", headers=headers)

    if r.status_code != 200:
        raise RuntimeError(f"personField GET fehlgeschlagen ({r.status_code}): {r.text}")

    payload = r.json() or {}
    field = payload.get("data") or {}
    _PERSON_FIELD_CACHE[field_code] = field
    return field


async def get_enum_options(field_code: str, headers: dict) -> list[dict]:
    """
    Gibt Optionen zurück als Liste [{id:..., label:...}, ...]
    """
    field = await pipedrive_get_person_field_v2(field_code, headers)
    opts = field.get("options") or []
    # robust: nur id/label extrahieren
    out = []
    for o in opts:
        if isinstance(o, dict) and "id" in o:
            out.append(
                {
                    "id": o.get("id"),
                    "label": o.get("label") or o.get("name") or str(o.get("id")),
                }
            )
    return out


def _render_select(name: str, options: list[dict], selected_value: Any) -> str:
    sel = "" if selected_value is None else str(selected_value)
    rows = ['<option value="">— bitte wählen —</option>']
    for o in options:
        oid = "" if o.get("id") is None else str(o.get("id"))
        lab = html_escape(str(o.get("label") or ""))
        selected_attr = " selected" if oid == sel else ""
        rows.append(f'<option value="{html_escape(oid)}"{selected_attr}>{lab}</option>')
    return f'<select class="field-input" name="{html_escape(name)}">{"".join(rows)}</select>'

async def fetch_persons_page_v2(headers: dict, cursor: str | None, limit: int, custom_keys: list[str]) -> tuple[list[dict], str | None]:
    params = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    if custom_keys:
        params["custom_fields"] = ",".join(custom_keys)

    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get(f"{PIPEDRIVE_API_V2_URL}/persons", headers=headers, params=params)

    if r.status_code != 200:
        raise RuntimeError(f"Persons page Fehler ({r.status_code}): {r.text}")

    payload = r.json() or {}
    items = payload.get("data") or []
    next_cursor = (payload.get("additional_data") or {}).get("next_cursor")
    return items, next_cursor


async def collect_bad_persons(
    headers: dict,
    page_size: int,
    start_cursor: str | None,
    predicate,
    need_custom_keys: list[str],
    scan_limit_per_call: int = 500,
    max_pages_scan: int = 30,
) -> tuple[list[dict], str | None]:
    """
    Scannt pages, bis page_size Treffer gesammelt oder keine Daten mehr.
    Damit bleibt UI schnell, auch bei 300k Datensätzen.
    """
    bad: list[dict] = []
    cursor = start_cursor
    pages = 0

    while len(bad) < page_size and pages < max_pages_scan:
        pages += 1
        persons, next_cursor = await fetch_persons_page_v2(
            headers=headers,
            cursor=cursor,
            limit=scan_limit_per_call,
            custom_keys=need_custom_keys,
        )
        if not persons:
            return bad, None

        for p in persons:
            if predicate(p):
                bad.append(p)
                if len(bad) >= page_size:
                    break

        cursor = next_cursor
        if not cursor:
            break

    return bad, cursor

@app.get("/dq/contacts/first_name/missing", response_class=HTMLResponse)
async def dq_first_name_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)

    limit = max(50, min(int(limit), 500))

    sql = """
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE (first_name IS NULL OR btrim(first_name) = '')
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
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/first_name/missing?after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Vorname – Fehlende Daten</div>
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
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer (oder Cache noch nicht vollständig).</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Vorname – Fehlende Daten", body))


########################################################################
#
#  HTML HELPER
#
########################################################################

def page_shell(title: str, body_html: str) -> str:
    # Logo optional
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
      <title>{title}</title>
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
          const data = await res.json();
          if(data.ok){{
            alert("✅ Aktualisiert.");
          }} else {{
            alert("❌ Fehler: " + (data.error || "Unbekannt"));
          }}
        }}
      </script>
    </body>
    </html>
    """

def _render_cards(group: str) -> str:
    cards = [c for c in DQ_CARDS if c["group"] == group]

    group_class = "contacts" if group == "Kontakte" else "orgs"
    group_sub = "Personenbezogene Prüfungen" if group == "Kontakte" else "Firmendaten / Stammdaten"

    card_html = []
    for c in cards:
        actions_html = []
        for a in c.get("actions", []):
            actions_html.append(f'<a class="chip" href="{a["href"]}">{a["label"]}</a>')


        card_html.append(f"""
          <div class="card">
            <div class="card-top">
              <h3>{c["title"]}</h3>
              <div class="card-desc">{c.get("description","")}</div>
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



@app.post("/dq/contacts/person/save")
async def dq_contact_save(payload: dict = Body(...)):
    """
    Speichert ausgewählte Felder per v2 PATCH.
    - Standardfelder: first_name, last_name, emails
    - Custom fields: gender, du/sie, position, linkedin via custom_fields
    """
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    headers = get_headers()
    person_id = int(payload.get("person_id"))

    first_name = (payload.get("first_name") or "").strip()
    last_name = (payload.get("last_name") or "").strip()
    email_primary = (payload.get("email_primary") or "").strip()

    gender_id = (payload.get("gender_id") or "").strip()
    dusie_id = (payload.get("dusie_id") or "").strip()

    position = (payload.get("position") or "").strip()
    linkedin = (payload.get("linkedin") or "").strip()

    patch = {
        "first_name": first_name,
        "last_name": last_name,
        # v2: emails ist Liste von Objekten
        "emails": ([{"label": "work", "value": email_primary, "primary": True}] if email_primary else []),
        # v2: custom fields liegen unter custom_fields
        "custom_fields": {
            PD_PERSON_GENDER_KEY: (int(gender_id) if gender_id else None),
            PD_PERSON_DU_SIE_KEY: (int(dusie_id) if dusie_id else None),
            PD_PERSON_POSITION_KEY: (position if position else None),
            PD_PERSON_LINKEDIN_KEY: (linkedin if linkedin else None),
        },
    }

    # None-Werte entfernen, damit PATCH "sauber" bleibt
    patch["custom_fields"] = {k: v for k, v in patch["custom_fields"].items() if v is not None}

    try:
        result = await pipedrive_patch_v2("persons", person_id, patch, headers)
        return {"ok": True, "result": result.get("data") or result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


########################################################################
#
#  OVERVIEW
#
########################################################################
@app.get("/overview", response_class=HTMLResponse)
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Datenqualität – Übersicht</div>
          <div class="subtitle">Wähle eine Prüfung aus (Kontakte & Organisationen).</div>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
          <a class="btn btn-outline" href="/logout">Logout</a>
        </div>
      </div>

      {_render_cards("Kontakte")}
      {_render_cards("Organisationen")}
    """
    return HTMLResponse(page_shell("Datenqualität – Übersicht", body))


@app.get("/logout")
def logout():
    user_tokens.pop("default", None)
    return RedirectResponse("/overview")


########################################################################
#
#  ENDPUNKTE KONTAKTE
#
########################################################################


async def pipedrive_patch_v2(entity: str, entity_id: int, payload: dict, headers: dict) -> dict:
    """
    v2 Update: PATCH /api/v2/persons/{id} bzw. /api/v2/organizations/{id} :contentReference[oaicite:9]{index=9}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.patch(
            f"{PIPEDRIVE_API_V2_URL}/{entity}/{entity_id}",
            headers=headers,
            json=payload,
        )
        if r.status_code in (200, 201):
            return r.json()

        raise RuntimeError(f"Update fehlgeschlagen ({r.status_code}): {r.text}")



def _render_results_table(
    title: str,
    subtitle: str,
    entity_type: str,
    field_key: str,
    rows: list[dict],
) -> str:
    # show up to 500 rows (UI)
    max_rows = 500
    shown = rows[:max_rows]
    more = len(rows) - len(shown)

    trs = []
    for r in shown:
        rid = r.get("id")
        name = r.get("display_name") or "-"
        val = r.get("current_value") or ""
        trs.append(f"""
          <tr>
            <td><code class="badge">{rid}</code></td>
            <td>{name}</td>
            <td>
              <input class="field-input" id="inp_{entity_type}_{rid}_{field_key}" value="{html_escape(val)}" />
              <div class="small">Aktueller Wert (editierbar)</div>
            </td>
            <td>
              <div class="row-actions">
                <button class="btn btn-primary" onclick="updateField('{entity_type}', '{rid}', '{field_key}')">Aktualisieren</button>
              </div>
            </td>
          </tr>
        """)

    table = f"""
      <div class="topbar">
        <div>
          <div class="title">{title}</div>
          <div class="subtitle">{subtitle}</div>
          <div class="subtitle"><span class="small">Treffer: <b>{len(rows)}</b>{(' · weitere ' + str(more) + ' nicht angezeigt') if more>0 else ''}</span></div>
        </div>
        <div style="display:flex; gap:10px;">
          <a class="btn btn-outline" href="/overview">← Zur Übersicht</a>
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
    return table


def html_escape(s: str) -> str:
    s = s or ""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


async def db_fetch_missing_first_name(after_id: int = 0, limit: int = 200) -> list[dict]:
    sql = """
    SELECT id, first_name, last_name, email, org_id
    FROM persons_cache
    WHERE (first_name IS NULL OR btrim(first_name) = '')
      AND id > $1
    ORDER BY id
    LIMIT $2
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(sql, after_id, limit)
    return [dict(r) for r in rows]


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


async def db_update_person_cache(person_id: int, data: dict):
    sql = """
    UPDATE persons_cache SET
      first_name=$2,
      last_name=$3,
      gender=$4,
      email=$5,
      du_sie=$6,
      position=$7,
      linkedin_url=$8,
      update_time=$9
    WHERE id=$1
    """
    async with db_pool.acquire() as conn:
        await conn.execute(
            sql,
            person_id,
            data.get("first_name") or "",
            data.get("last_name") or "",
            data.get("gender") or "",
            data.get("email") or "",
            data.get("du_sie") or "",
            data.get("position") or "",
            data.get("linkedin_url") or "",
            _utcnow()
        )




@app.get("/dq/contacts/first_name/invalidchars", response_class=HTMLResponse)
async def dq_first_name_invalidchars_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    # Postgres regex: erlaubt A-Z, Umlaute, Leerzeichen, - und '
    pattern = r"^[A-Za-zÄÖÜäöüß\s\-']+$"

    sql = """
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE first_name IS NOT NULL
      AND btrim(first_name) <> ''
      AND first_name !~ $1
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
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/first_name/invalidchars?after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Vorname – Ungültige Zeichen</div>
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
    return HTMLResponse(page_shell("Vorname – Ungültige Zeichen", body))


_person_labels_cache: Optional[dict[int, str]] = None

async def get_person_labels(headers: dict) -> dict[int, str]:
    global _person_labels_cache
    if _person_labels_cache is not None:
        return _person_labels_cache

    url = "https://api.pipedrive.com/v1/personLabels"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers=headers)
        if r.status_code != 200:
            _person_labels_cache = {}
            return _person_labels_cache

        data = (r.json() or {}).get("data") or []
        _person_labels_cache = {int(x["id"]): (x.get("name") or "") for x in data if x.get("id") is not None}
        return _person_labels_cache


_field_options_cache: dict[str, list[tuple[str, str]]] = {}

_field_options_cache_v2: dict[str, list[tuple[str, str]]] = {}

async def get_person_field_options(headers: dict, field_key: str) -> list[tuple[str, str]]:
    """
    v2: GET /api/v2/personFields/{field_code} -> data.options[]
    Wir cachen lokal im Prozess.
    """
    if field_key in _field_options_cache_v2:
        return _field_options_cache_v2[field_key]

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{PIPEDRIVE_API_V2_URL}/personFields/{field_key}", headers=headers)

    if r.status_code != 200:
        _field_options_cache_v2[field_key] = []
        return []

    data = (r.json() or {}).get("data") or {}
    options = data.get("options") or []

    out: list[tuple[str, str]] = []
    for o in options:
        oid = o.get("id")
        label = o.get("label") or o.get("name") or ""
        if oid is not None:
            out.append((str(oid), str(label)))

    _field_options_cache_v2[field_key] = out
    return out


@app.get("/dq/contacts/person/{person_id}", response_class=HTMLResponse)
async def dq_person_detail_db(person_id: int, saved: int = 0):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)

    p = await db_fetch_person_detail(person_id)
    if not p:
        return HTMLResponse("Kontakt nicht im Cache gefunden. Bitte Sync laufen lassen.", status_code=404)

    headers = get_headers()
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
          <a class="btn btn-outline" href="/dq/contacts/first_name/missing">← Zur Liste</a>
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

@app.get("/dq/contacts/last_name/missing", response_class=HTMLResponse)
async def dq_last_name_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    sql = """
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE (last_name IS NULL OR btrim(last_name) = '')
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
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/last_name/missing?after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Nachname – Fehlende Daten</div>
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
    return HTMLResponse(page_shell("Nachname – Fehlende Daten", body))

@app.get("/dq/contacts/last_name/invalidchars", response_class=HTMLResponse)
async def dq_last_name_invalidchars_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))
    pattern = r"^[A-Za-zÄÖÜäöüß\s\-']+$"

    sql = """
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE last_name IS NOT NULL
      AND btrim(last_name) <> ''
      AND last_name !~ $1
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
        next_link = f'<a class="btn btn-outline" href="/dq/contacts/last_name/invalidchars?after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">Nachname – Ungültige Zeichen</div>
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
    return HTMLResponse(page_shell("Nachname – Ungültige Zeichen", body))

def _dq_missing_sql_for_column(col: str) -> str:
    return f"""
    SELECT id, first_name, last_name
    FROM persons_cache
    WHERE ({col} IS NULL OR btrim({col}) = '')
      AND id > $1
    ORDER BY id
    LIMIT $2
    """

async def _render_missing_list(title: str, base_path: str, after_id: int, limit: int, sql: str) -> HTMLResponse:
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
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(fn)}</td>
            <td>{html_escape(ln)}</td>
            <td style="width:160px;"><a class="chip" href="/dq/contacts/person/{pid}">Öffnen</a></td>
          </tr>
        """)

    next_link = ""
    if rows:
        next_link = f'<a class="btn btn-outline" href="{base_path}?after_id={last_id}&limit={limit}">Weiter →</a>'

    body = f"""
      <div class="topbar">
        <div>
          <div class="title">{title}</div>
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
    return HTMLResponse(page_shell(title, body))

@app.get("/dq/contacts/gender/missing", response_class=HTMLResponse)
async def dq_gender_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    return await _render_missing_list("Geschlecht – Fehlende Daten", "/dq/contacts/gender/missing", after_id, limit, _dq_missing_sql_for_column("gender"))

@app.get("/dq/contacts/email/missing", response_class=HTMLResponse)
async def dq_email_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    return await _render_missing_list("E-Mail – Fehlende Daten", "/dq/contacts/email/missing", after_id, limit, _dq_missing_sql_for_column("email"))

@app.get("/dq/contacts/du_sie/missing", response_class=HTMLResponse)
async def dq_du_sie_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    return await _render_missing_list("Du oder Sie – Fehlende Daten", "/dq/contacts/du_sie/missing", after_id, limit, _dq_missing_sql_for_column("du_sie"))

@app.get("/dq/contacts/position/missing", response_class=HTMLResponse)
async def dq_position_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    return await _render_missing_list("Position – Fehlende Daten", "/dq/contacts/position/missing", after_id, limit, _dq_missing_sql_for_column("position"))

@app.get("/dq/contacts/linkedin/missing", response_class=HTMLResponse)
async def dq_linkedin_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)
    limit = max(50, min(int(limit), 500))
    return await _render_missing_list("LinkedIn-URL – Fehlende Daten", "/dq/contacts/linkedin/missing", after_id, limit, _dq_missing_sql_for_column("linkedin_url"))


@app.get("/dq/contacts/first_name/title", response_class=HTMLResponse)
async def dq_first_name_title_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert", status_code=500)

    limit = max(50, min(int(limit), 500))

    # Postgres-RegEx: "dr", "dr.", "prof", "herr", "frau" etc am Anfang
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



@app.post("/dq/contacts/person/{person_id}/update")
async def dq_person_update_db(person_id: int, payload: dict = Body(...)):
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    first_name = (payload.get("first_name") or "").strip()
    last_name = (payload.get("last_name") or "").strip()
    email = (payload.get("email") or "").strip()

    gender_id = (payload.get("gender") or "").strip()     # option id als string
    du_sie_id = (payload.get("du_sie") or "").strip()     # option id als string

    position = (payload.get("position") or "").strip()
    linkedin = (payload.get("linkedin_url") or "").strip()

    patch = {
        "first_name": first_name if first_name != "" else None,
        "last_name": last_name if last_name != "" else None,
        "emails": ([{"label": "work", "value": email, "primary": True}] if email else []),
        "custom_fields": {
            PD_PERSON_GENDER_KEY: (int(gender_id) if gender_id.isdigit() else None),
            PD_PERSON_DU_SIE_KEY: (int(du_sie_id) if du_sie_id.isdigit() else None),
            PD_PERSON_POSITION_KEY: (position if position != "" else None),
            PD_PERSON_LINKEDIN_KEY: (linkedin if linkedin != "" else None),
        },
    }

    # Optional: None Felder rauswerfen (sauberer PATCH)
    patch["custom_fields"] = {k: v for k, v in patch["custom_fields"].items() if v is not None}

    try:
        await pipedrive_patch_v2("persons", person_id, patch, headers)

        # Cache nachziehen (DB)
        if db_pool:
            await db_update_person_cache(person_id, {
                "first_name": first_name,
                "last_name": last_name,
                "gender": (gender_id if gender_id else ""),
                "email": email,
                "du_sie": (du_sie_id if du_sie_id else ""),
                "position": position,
                "linkedin_url": linkedin,
            })

        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)



########################################################################
#
#  ENDPUNKTE ORGANISATIONEN
########################################################################
async def _dq_scan_orgs_missing(field_key: str) -> list[dict]:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    orgs = await fetch_all_v2("organizations", headers=headers)
    bad = []
    for o in orgs:
        v = o.get(field_key)
        # address ist oft dict → missing, wenn value leer
        if field_key == "address":
            v = extract_address(v)
        if _is_missing(v) or (isinstance(v, str) and v.strip() == "-"):
            bad.append(
                {
                    "id": o.get("id"),
                    "display_name": o.get("name") or "-",
                    "current_value": _scalarize(o.get(field_key)) if field_key != "address" else extract_address(o.get(field_key)),
                }
            )
    return bad


async def _dq_scan_orgs_invalidchars(field_key: str) -> list[dict]:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    orgs = await fetch_all_v2("organizations", headers=headers)
    bad = []
    for o in orgs:
        v = _scalarize(o.get(field_key)).strip()
        if not v:
            continue
        if _has_invalid_name_chars(v):
            bad.append(
                {
                    "id": o.get("id"),
                    "display_name": o.get("name") or "-",
                    "current_value": v,
                }
            )
    return bad


@app.get("/dq/orgs/missing", response_class=HTMLResponse)
async def dq_orgs_missing(field: str):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    rows = await _dq_scan_orgs_missing(field)
    title = "Organisationen – Fehlende Daten"
    subtitle = f"Feld: {field}"
    body = _render_results_table(title, subtitle, "organization", field, rows)
    return HTMLResponse(page_shell(title, body))


@app.get("/dq/orgs/invalidchars", response_class=HTMLResponse)
async def dq_orgs_invalidchars(field: str):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    rows = await _dq_scan_orgs_invalidchars(field)
    title = "Organisationen – Sonderzeichen / ungültige Zeichen"
    subtitle = f"Feld: {field} (erlaubt: A–Z, Umlaute, Leerzeichen, - ')"
    body = _render_results_table(title, subtitle, "organization", field, rows)
    return HTMLResponse(page_shell(title, body))


########################################################################
#
# ENDPUNKTE UPDATE
#
########################################################################
@app.post("/dq/update")
async def dq_update(payload: dict = Body(...)):
    """
    Body:
    {
      "entity_type": "person" | "organization",
      "entity_id": 123,
      "field_key": "...",
      "value": "..."
    }
    """
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    entity_type = (payload.get("entity_type") or "").strip().lower()
    entity_id = payload.get("entity_id")
    field_key = payload.get("field_key")
    value = payload.get("value")

    if entity_type not in ("person", "organization"):
        return {"ok": False, "error": "entity_type muss 'person' oder 'organization' sein"}
    if not isinstance(entity_id, int):
        return {"ok": False, "error": "entity_id muss int sein"}
    if not field_key or not isinstance(field_key, str):
        return {"ok": False, "error": "field_key fehlt"}

    headers = get_headers()
    if not headers:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    entity_endpoint = "persons" if entity_type == "person" else "organizations"

    # v2 payload bauen
    patch_fragment = normalize_person_update_payload_v2(field_key, value)

    try:
        result = await pipedrive_patch_v2(entity_endpoint, entity_id, patch_fragment, headers)
        return {"ok": True, "result": result.get("data") or result}
    except Exception as e:
        return {"ok": False, "error": str(e)}

########################################################################
#
# ENDPUNKTE DB
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
      </div>

      <div style="margin-top:12px;" class="small">
        Tipp: Für Initial-Sync mehrfach klicken, bis bei <b>processed</b> keine neuen Datensätze mehr kommen.
      </div>
    </div>
    """
    return HTMLResponse(page_shell("Admin – Sync", body))

@app.get("/admin/sync")
async def admin_sync(entity: str = "persons", full: int = 0, max_pages: int = 20):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    if not db_pool:
        return {"ok": False, "error": "DATABASE_URL fehlt / DB nicht initialisiert"}

    if entity == "persons":
        res = await sync_persons_incremental(full=bool(full), max_pages=max_pages)
        return {"ok": True, "result": res}
    if entity in ("orgs", "organizations"):
        res = await sync_orgs_incremental(full=bool(full), max_pages=max_pages)
        return {"ok": True, "result": res}

    return {"ok": False, "error": "entity muss 'persons' oder 'organizations' sein"}


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
        missing_first = await conn.fetchval(
            "SELECT COUNT(*) FROM persons_cache WHERE first_name IS NULL OR btrim(first_name)=''"
        )
    return {
        "ok": True,
        "persons_cache": int(persons),
        "orgs_cache": int(orgs),
        "missing_first_name": int(missing_first),
    }


########################################################################
#
#  LOKALER START
#
########################################################################
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
