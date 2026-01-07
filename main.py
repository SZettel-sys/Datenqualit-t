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


DQ_TILES = [
    # Kontakte
    {
        "group": "Kontakte",
        "title": "Vorname",
        "subtitle": "Fehlende Daten",
        "href": "/dq/contacts/missing?field=first_name",
        "meta": "Field-Key: first_name",
    },
    {
        "group": "Kontakte",
        "title": "Vorname",
        "subtitle": "Sonderzeichen / ungültige Zeichen",
        "href": "/dq/contacts/invalidchars?field=first_name",
        "meta": "Field-Key: first_name",
    },
    {
        "group": "Kontakte",
        "title": "Vorname",
        "subtitle": "Titel im Vornamen (Dr. Simone → Simone)",
        "href": "/dq/contacts/title_in_first_name",
        "meta": "Field-Key: first_name",
    },
    {
        "group": "Kontakte",
        "title": "Nachname",
        "subtitle": "Fehlende Daten",
        "href": "/dq/contacts/missing?field=last_name",
        "meta": "Field-Key: last_name",
    },
    {
        "group": "Kontakte",
        "title": "Nachname",
        "subtitle": "Sonderzeichen / ungültige Zeichen",
        "href": "/dq/contacts/invalidchars?field=last_name",
        "meta": "Field-Key: last_name",
    },
    {
        "group": "Kontakte",
        "title": "Geschlecht",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/contacts/missing?field={PD_PERSON_GENDER_KEY}",
        "meta": f"Field-Key: {PD_PERSON_GENDER_KEY}",
    },
    {
        "group": "Kontakte",
        "title": "E-Mail-Adresse",
        "subtitle": "Fehlende Daten",
        "href": "/dq/contacts/missing?field=email",
        "meta": "Field-Key: email",
    },
    {
        "group": "Kontakte",
        "title": "Du oder Sie",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/contacts/missing?field={PD_PERSON_DU_SIE_KEY}",
        "meta": f"Field-Key: {PD_PERSON_DU_SIE_KEY}",
    },
    {
        "group": "Kontakte",
        "title": "Position",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/contacts/missing?field={PD_PERSON_POSITION_KEY}",
        "meta": f"Field-Key: {PD_PERSON_POSITION_KEY}",
    },
    {
        "group": "Kontakte",
        "title": "LinkedIn-URL",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/contacts/missing?field={PD_PERSON_LINKEDIN_KEY}",
        "meta": f"Field-Key: {PD_PERSON_LINKEDIN_KEY}",
    },
    # Organisationen
    {
        "group": "Organisationen",
        "title": "Name / Rechtsform",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/orgs/missing?field={PD_ORG_NAME_KEY}",
        "meta": f"Field-Key: {PD_ORG_NAME_KEY}",
    },
    {
        "group": "Organisationen",
        "title": "Name / Rechtsform",
        "subtitle": "Sonderzeichen / ungültige Zeichen",
        "href": f"/dq/orgs/invalidchars?field={PD_ORG_NAME_KEY}",
        "meta": f"Field-Key: {PD_ORG_NAME_KEY}",
    },
    {
        "group": "Organisationen",
        "title": "Adresse",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/orgs/missing?field={PD_ORG_ADDRESS_KEY}",
        "meta": f"Field-Key: {PD_ORG_ADDRESS_KEY}",
    },
    {
        "group": "Organisationen",
        "title": "Website",
        "subtitle": "Fehlende Daten",
        "href": f"/dq/orgs/missing?field={PD_ORG_WEBSITE_KEY}",
        "meta": f"Field-Key: {PD_ORG_WEBSITE_KEY}",
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
      <style>
        :root{{
          --bg:#f6f7fb;
          --card:#ffffff;
          --text:#0f172a;
          --muted:#64748b;
          --border:#e2e8f0;
          --brand:#0ea5e9;
          --brand-hover:#0284c7;
          --shadow:0 10px 25px rgba(15,23,42,.08);
        }}
        *{{ box-sizing:border-box; }}
        body{{
          font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
          background:var(--bg);
          margin:0;
          color:var(--text);
        }}
        header{{
          background:linear-gradient(90deg,#ffffff 0%, #f8fbff 60%, #ffffff 100%);
          border-bottom:1px solid var(--border);
          padding:14px 16px;
          display:flex;
          justify-content:center;
          align-items:center;
          position:sticky;
          top:0;
          z-index:10;
        }}
        header img{{ height:48px; }}
        .container{{
          max-width:1400px;
          margin:18px auto 90px;
          padding:0 14px;
        }}
        .topbar{{
          display:flex;
          justify-content:space-between;
          align-items:center;
          gap:10px;
          flex-wrap:wrap;
          margin:10px 0 14px;
        }}
        .title{{
          font-size:20px;
          font-weight:900;
          letter-spacing:.2px;
        }}
        .subtitle{{
          margin-top:4px;
          color:var(--muted);
          font-weight:700;
        }}
        .btn{{
          appearance:none;
          border:1px solid transparent;
          border-radius:12px;
          padding:10px 14px;
          font-weight:800;
          cursor:pointer;
          display:inline-flex;
          align-items:center;
          gap:8px;
          transition:background .15s ease, box-shadow .15s ease, transform .05s ease;
          box-shadow:0 2px 10px rgba(15,23,42,.06);
          text-decoration:none;
        }}
        .btn:active{{ transform:translateY(1px); }}
        .btn-primary{{ background:var(--brand); color:white; }}
        .btn-primary:hover{{ background:var(--brand-hover); }}
        .btn-outline{{ background:white; color:var(--text); border-color:var(--border); box-shadow:0 2px 10px rgba(15,23,42,.04);}}
        .btn-outline:hover{{ background:#f8fafc; }}

        .grid{{
          display:grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap:18px;
        }}
        @media (max-width: 1100px){{ .grid{{ grid-template-columns: repeat(2, minmax(0,1fr)); }} }}
        @media (max-width: 700px){{ .grid{{ grid-template-columns: 1fr; }} }}

        .card{{
          background:var(--card);
          border:1px solid var(--border);
          border-radius:18px;
          box-shadow:var(--shadow);
          padding:18px 18px 16px;
          display:flex;
          flex-direction:column;
          min-height:170px;
        }}
        .card h3{{
          margin:0;
          font-size:18px;
          font-weight:950;
          letter-spacing:.2px;
        }}
        .card .desc{{
          margin-top:10px;
          color:var(--muted);
          font-weight:700;
          line-height:1.35;
          flex:1;
        }}
        .card .meta{{
          margin-top:10px;
          font-size:12px;
          color:var(--muted);
          font-weight:800;
        }}
        .card .actions{{
          margin-top:14px;
          display:flex;
          justify-content:flex-start;
        }}
        .group-title{{
          margin:22px 0 10px;
          font-size:14px;
          color:var(--muted);
          font-weight:950;
          letter-spacing:.12em;
          text-transform:uppercase;
        }}

        /* Table */
        .panel{{
          background:var(--card);
          border:1px solid var(--border);
          border-radius:18px;
          box-shadow:var(--shadow);
          padding:14px;
        }}
        table{{
          width:100%;
          border-collapse:separate;
          border-spacing:0;
          overflow:hidden;
        }}
        th, td{{
          text-align:left;
          padding:10px 10px;
          border-bottom:1px solid var(--border);
          vertical-align:top;
          font-size:14px;
        }}
        th{{
          font-size:13px;
          color:var(--muted);
          font-weight:900;
          background:linear-gradient(180deg,#ffffff,#f8fbff);
        }}
        tr:last-child td{{ border-bottom:none; }}
        code.badge{{
          background:#f1f5f9;
          border:1px solid var(--border);
          padding:3px 8px;
          border-radius:999px;
          font-size:12px;
          font-weight:900;
        }}
        .small{{ font-size:12px; color:var(--muted); font-weight:700; }}
        .field-input{{
          width: 100%;
          max-width: 420px;
          padding:8px 10px;
          border-radius:12px;
          border:1px solid var(--border);
          outline:none;
          font-weight:700;
        }}
        .row-actions{{ display:flex; gap:8px; align-items:center; }}
      </style>
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


def _render_tiles(group: str) -> str:
    tiles = [t for t in DQ_TILES if t["group"] == group]
    cards = []
    for t in tiles:
        cards.append(f"""
          <div class="card">
            <h3>{t["title"]}</h3>
            <div class="desc">{t["subtitle"]}</div>
            <div class="meta">{t["meta"]}</div>
            <div class="actions">
              <a class="btn btn-primary" href="{t["href"]}">Öffnen</a>
            </div>
          </div>
        """)
    return f"""
      <div class="group-title">{group}</div>
      <div class="grid">
        {''.join(cards)}
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

      {_render_tiles("Kontakte")}
      {_render_tiles("Organisationen")}
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
#  LOKALER START
#
########################################################################
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
