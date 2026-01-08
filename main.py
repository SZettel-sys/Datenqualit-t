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
          update_time TIMESTAMPTZ
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS orgs_cache (
          id BIGINT PRIMARY KEY,
          name TEXT,
          update_time TIMESTAMPTZ
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
          entity TEXT PRIMARY KEY,              -- 'persons' | 'organizations'
          last_update_time TIMESTAMPTZ NOT NULL
        );
        """)

        # Indizes für schnelle "Vorname fehlt" Queries + Join
        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_persons_missing_first_name
        ON persons_cache (id)
        WHERE (first_name IS NULL OR btrim(first_name) = '');
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
    # ================= Kontakte =================
    {
        "group": "Kontakte",
        "title": "Vorname",
        "description": "Prüfungen für das Feld „first_name“.",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/first_name/missing"},
            {"label": "Ungültige Zeichen", "href": "/dq/contacts/invalidchars?field=first_name"},
            {"label": "Titel im Vornamen", "href": "/dq/contacts/title_in_first_name"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Nachname",
        "description": "Prüfungen für das Feld „last_name“.",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/missing?field=last_name"},
            {"label": "Ungültige Zeichen", "href": "/dq/contacts/invalidchars?field=last_name"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Geschlecht",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/contacts/missing?field={PD_PERSON_GENDER_KEY}"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "E-Mail-Adresse",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": "/dq/contacts/missing?field=email"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Du oder Sie",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/contacts/missing?field={PD_PERSON_DU_SIE_KEY}"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "Position",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/contacts/missing?field={PD_PERSON_POSITION_KEY}"},
        ],
    },
    {
        "group": "Kontakte",
        "title": "LinkedIn-URL",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/contacts/missing?field={PD_PERSON_LINKEDIN_KEY}"},
        ],
    },

    # ================= Organisationen =================
    {
        "group": "Organisationen",
        "title": "Name / Rechtsform",
        "description": "Prüfung auf Lücken & ungültige Zeichen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/orgs/missing?field={PD_ORG_NAME_KEY}"},
            {"label": "Ungültige Zeichen", "href": f"/dq/orgs/invalidchars?field={PD_ORG_NAME_KEY}"},
        ],
    },
    {
        "group": "Organisationen",
        "title": "Adresse",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/orgs/missing?field={PD_ORG_ADDRESS_KEY}"},
        ],
    },
    {
        "group": "Organisationen",
        "title": "Website",
        "description": "Fehlende Werte prüfen.",
        "actions": [
            {"label": "Fehlende Daten", "href": f"/dq/orgs/missing?field={PD_ORG_WEBSITE_KEY}"},
        ],
    },
]



########################################################################
#
#  Pipedrive Fetch Helpers
#
########################################################################

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


async def fetch_all_v2(endpoint: str, headers: dict, params: Optional[dict] = None) -> list[dict]:
    """Cursor-basierte Pagination (v2) – gibt alle items als Liste zurück."""
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

        rows.append((
            int(pid),
            _scalarize(p.get("first_name")).strip(),
            _scalarize(p.get("last_name")).strip(),
            _scalarize(p.get(PD_PERSON_GENDER_KEY)).strip(),
            _email_first(p),
            _scalarize(p.get(PD_PERSON_DU_SIE_KEY)).strip(),
            _scalarize(p.get(PD_PERSON_POSITION_KEY)).strip(),
            _scalarize(p.get(PD_PERSON_LINKEDIN_KEY)).strip(),
            _get_org_id_from_person(p),
            ts
        ))

    if not rows:
        return max_ts

    sql = """
    INSERT INTO persons_cache
      (id, first_name, last_name, gender, email, du_sie, position, linkedin_url, org_id, update_time)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (id) DO UPDATE SET
      first_name   = EXCLUDED.first_name,
      last_name    = EXCLUDED.last_name,
      gender       = EXCLUDED.gender,
      email        = EXCLUDED.email,
      du_sie       = EXCLUDED.du_sie,
      position     = EXCLUDED.position,
      linkedin_url = EXCLUDED.linkedin_url,
      org_id       = EXCLUDED.org_id,
      update_time  = COALESCE(EXCLUDED.update_time, persons_cache.update_time)
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

        rows.append((
            int(oid),
            _scalarize(o.get("name")).strip(),
            ts
        ))

    if not rows:
        return max_ts

    sql = """
    INSERT INTO orgs_cache (id, name, update_time)
    VALUES ($1,$2,$3)
    ON CONFLICT (id) DO UPDATE SET
      name        = EXCLUDED.name,
      update_time = COALESCE(EXCLUDED.update_time, orgs_cache.update_time)
    """

    async with db_pool.acquire() as conn:
        await conn.executemany(sql, rows)

    return max_ts


async def sync_persons_incremental(full: bool = False, max_pages: int = 20) -> dict:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    since = await get_sync_time("persons")
    # Overlap (2 Minuten) gegen Race Conditions / Uhrzeit-Granularität
    if not full:
        since = since - timedelta(minutes=2)

    params = {"limit": 500}
    # Wenn Pipedrive updated_since unterstützt:
    if not full:
        params["updated_since"] = since.isoformat()

    # optional: Felder reduzieren (falls API das unterstützt – wenn nicht, rausnehmen)
    # params["include_fields"] = "id,first_name,last_name,email,org_id,update_time"
    # params["custom_fields"] = f"{PD_PERSON_GENDER_KEY},{PD_PERSON_DU_SIE_KEY},{PD_PERSON_POSITION_KEY},{PD_PERSON_LINKEDIN_KEY}"

    max_seen: Optional[datetime] = None
    total = 0

    async for items in iter_v2_pages("persons", headers=headers, params=params, max_pages=max_pages):
        total += len(items)
        ts = await upsert_persons(items)
        if ts and (max_seen is None or ts > max_seen):
            max_seen = ts

    if max_seen:
        await set_sync_time("persons", max_seen)

    return {"entity": "persons", "full": full, "max_pages": max_pages, "processed": total, "new_sync_time": max_seen.isoformat() if max_seen else None}


async def sync_orgs_incremental(full: bool = False, max_pages: int = 20) -> dict:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    since = await get_sync_time("organizations")
    if not full:
        since = since - timedelta(minutes=2)

    params = {"limit": 500}
    if not full:
        params["updated_since"] = since.isoformat()

    max_seen: Optional[datetime] = None
    total = 0

    async for items in iter_v2_pages("organizations", headers=headers, params=params, max_pages=max_pages):
        total += len(items)
        ts = await upsert_orgs(items)
        if ts and (max_seen is None or ts > max_seen):
            max_seen = ts

    if max_seen:
        await set_sync_time("organizations", max_seen)

    return {"entity": "organizations", "full": full, "max_pages": max_pages, "processed": total, "new_sync_time": max_seen.isoformat() if max_seen else None}

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
            actions_html.append(f'<a class="btn btn-sm btn-primary" href="{a["href"]}">{a["label"]}</a>')

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
async def _dq_scan_contacts_missing(field_key: str) -> list[dict]:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    # persons endpoint
    persons = await fetch_all_v2("persons", headers=headers)
    bad = []
    for p in persons:
        v = p.get(field_key)
        if _is_missing(v):
            bad.append(
                {
                    "id": p.get("id"),
                    "display_name": p.get("name") or f"{_scalarize(p.get('first_name'))} {_scalarize(p.get('last_name'))}".strip(),
                    "current_value": _scalarize(v),
                }
            )
    return bad


async def _dq_scan_contacts_invalidchars(field_key: str) -> list[dict]:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    persons = await fetch_all_v2("persons", headers=headers)
    bad = []
    for p in persons:
        v = _scalarize(p.get(field_key)).strip()
        if not v:
            continue
        if _has_invalid_name_chars(v):
            bad.append(
                {
                    "id": p.get("id"),
                    "display_name": p.get("name") or f"{_scalarize(p.get('first_name'))} {_scalarize(p.get('last_name'))}".strip(),
                    "current_value": v,
                }
            )
    return bad


async def _dq_scan_contacts_title_in_first_name() -> list[dict]:
    headers = get_headers()
    if not headers:
        raise RuntimeError("Nicht eingeloggt")

    persons = await fetch_all_v2("persons", headers=headers)
    bad = []
    for p in persons:
        v = _scalarize(p.get("first_name")).strip()
        if not v:
            continue
        if TITLE_PREFIX_REGEX.search(v):
            bad.append(
                {
                    "id": p.get("id"),
                    "display_name": p.get("name") or f"{_scalarize(p.get('first_name'))} {_scalarize(p.get('last_name'))}".strip(),
                    "current_value": v,
                }
            )
    return bad


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


@app.get("/dq/contacts/missing", response_class=HTMLResponse)
async def dq_contacts_missing(field: str):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    rows = await _dq_scan_contacts_missing(field)
    title = "Kontakte – Fehlende Daten"
    subtitle = f"Feld: {field}"
    body = _render_results_table(title, subtitle, "person", field, rows)
    return HTMLResponse(page_shell(title, body))


@app.get("/dq/contacts/invalidchars", response_class=HTMLResponse)
async def dq_contacts_invalidchars(field: str):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    rows = await _dq_scan_contacts_invalidchars(field)
    title = "Kontakte – Sonderzeichen / ungültige Zeichen"
    subtitle = f"Feld: {field} (erlaubt: A–Z, Umlaute, Leerzeichen, - ')"
    body = _render_results_table(title, subtitle, "person", field, rows)
    return HTMLResponse(page_shell(title, body))


@app.get("/dq/contacts/title_in_first_name", response_class=HTMLResponse)
async def dq_contacts_title_in_first_name():
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    rows = await _dq_scan_contacts_title_in_first_name()
    title = "Kontakte – Titel im Vornamen"
    subtitle = "Prüfung: Dr./Prof./Herr/Frau etc. im Feld first_name"
    body = _render_results_table(title, subtitle, "person", "first_name", rows)
    return HTMLResponse(page_shell(title, body))

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
      p.org_id,
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
      first_name=$2, last_name=$3, gender=$4, email=$5, du_sie=$6, position=$7, linkedin_url=$8,
      update_time=$9
    WHERE id=$1
    """
    async with db_pool.acquire() as conn:
        await conn.execute(
            sql,
            person_id,
            data.get("first_name"),
            data.get("last_name"),
            data.get("gender"),
            data.get("email"),
            data.get("du_sie"),
            data.get("position"),
            data.get("linkedin_url"),
            _utcnow()
        )


@app.get("/dq/contacts/first_name/missing", response_class=HTMLResponse)
async def dq_first_name_missing_db(after_id: int = 0, limit: int = 200):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)

    limit = max(50, min(int(limit), 500))
    rows = await db_fetch_missing_first_name(after_id=after_id, limit=limit)

    trs = []
    last_id = after_id
    for r in rows:
        pid = r["id"]
        last_id = pid
        display = (f"{(r.get('first_name') or '').strip()} {(r.get('last_name') or '').strip()}").strip() or "-"
        trs.append(f"""
          <tr>
            <td><code class="badge">{pid}</code></td>
            <td>{html_escape(display)}</td>
            <td>{html_escape((r.get("email") or "").strip() or "-")}</td>
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
              <th>Kontakt</th>
              <th>E-Mail</th>
              <th style="width:160px;">Aktion</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trs) if trs else '<tr><td colspan="4">✅ Keine Treffer (oder Sync noch nicht gelaufen).</td></tr>'}
          </tbody>
        </table>
      </div>
    """
    return HTMLResponse(page_shell("Vorname – Fehlende Daten", body))


@app.get("/dq/contacts/person/{person_id}", response_class=HTMLResponse)
async def dq_person_detail_db(person_id: int, saved: int = 0):
    if "default" not in user_tokens:
        return RedirectResponse("/login")
    if not db_pool:
        return HTMLResponse("DB nicht initialisiert (DATABASE_URL fehlt)", status_code=500)

    p = await db_fetch_person_detail(person_id)
    if not p:
        return HTMLResponse("Kontakt nicht im Cache gefunden. Bitte Sync laufen lassen.", status_code=404)

    notice = ""
    if saved == 1:
        notice = '<div class="panel" style="margin-bottom:12px; border-color: rgba(14,165,233,.35);">✅ Gespeichert.</div>'

    def val(k: str) -> str:
        return html_escape((p.get(k) or "").strip())

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
            <tr><th>Geschlecht</th><td><input class="field-input" id="gender" value="{val("gender")}" /></td></tr>
            <tr><th>E-Mail-Adresse</th><td><input class="field-input" id="email" value="{val("email")}" /></td></tr>
            <tr><th>Du oder Sie</th><td><input class="field-input" id="du_sie" value="{val("du_sie")}" /></td></tr>
            <tr><th>Position</th><td><input class="field-input" id="position" value="{val("position")}" /></td></tr>
            <tr><th>LinkedIn-URL</th><td><input class="field-input" id="linkedin_url" value="{val("linkedin_url")}" /></td></tr>
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

          const data = await res.json();
          if(data.ok) {{
            window.location.href = `/dq/contacts/person/${{personId}}?saved=1`;
          }} else {{
            alert("❌ Fehler: " + (data.error || "Unbekannt"));
          }}
        }}
      </script>
    """
    return HTMLResponse(page_shell("Kontakt bearbeiten", body))


@app.post("/dq/contacts/person/{person_id}/update")
async def dq_person_update_db(person_id: int, payload: dict = Body(...)):
    if "default" not in user_tokens:
        return JSONResponse({"ok": False, "error": "Nicht eingeloggt"}, status_code=401)

    headers = get_headers()

    # Mapping: Cache-Feldnamen -> Pipedrive-Feldkeys
    pd_payload = {
        "first_name": (payload.get("first_name") or "").strip(),
        "last_name": (payload.get("last_name") or "").strip(),
        PD_PERSON_GENDER_KEY: (payload.get("gender") or "").strip(),
        PD_PERSON_DU_SIE_KEY: (payload.get("du_sie") or "").strip(),
        PD_PERSON_POSITION_KEY: (payload.get("position") or "").strip(),
        PD_PERSON_LINKEDIN_KEY: (payload.get("linkedin_url") or "").strip(),
    }

    email = (payload.get("email") or "").strip()
    pd_payload["email"] = [{"value": email, "primary": True}] if email else []

    try:
        await pipedrive_update_v2("persons", person_id, pd_payload, headers)

        # Cache aktualisieren (ohne extra GET)
        if db_pool:
            await db_update_person_cache(person_id, {
                "first_name": pd_payload["first_name"],
                "last_name": pd_payload["last_name"],
                "gender": pd_payload[PD_PERSON_GENDER_KEY],
                "email": email,
                "du_sie": pd_payload[PD_PERSON_DU_SIE_KEY],
                "position": pd_payload[PD_PERSON_POSITION_KEY],
                "linkedin_url": pd_payload[PD_PERSON_LINKEDIN_KEY],
            })

        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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
    entity_endpoint = "persons" if entity_type == "person" else "organizations"

    try:
        result = await pipedrive_update_v2(entity_endpoint, entity_id, {field_key: value}, headers)
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


########################################################################
#
#  LOKALER START
#
########################################################################
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
