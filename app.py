import json, time, math, io
import pandas as pd
import requests
import streamlit as st
from typing import List, Dict, Set

# =========================
# CONFIG FIJA (sin sidebar)
# =========================
CLIENT_ID     = "1000.YAZ4PANZGEM7J6IQ1SWP9GY8NJBD4V"
CLIENT_SECRET = "3d707459d66ea9afc1df5bdeac7f8fb46945e472bb"
REFRESH_TOKEN = "1000.27580f95a6dc620cad7fadef2011a212.e504a6a43036e25ff2e7077fa57a7370"
DC            = "com"  # MX -> "com"

# Si ya conoces una lista fija, colócala aquí (si está vacío se mostrará selector)
FIXED_LIST_KEY = ""

# Rendimiento / rate-limit
BATCH_SIZE    = 10         # Zoho permite 1..10
SLEEP_BULK    = 0.2        # segundos entre lotes
SLEEP_SINGLE  = 0.05       # segundos por upsert
RETRY_429_MAX = 5          # reintentos exponenciales

ACCOUNTS = f"https://accounts.zoho.{DC}"
BASE     = f"https://campaigns.zoho.{DC}/api/v1.1"

# =========================
# Funciones de API
# =========================
def get_access_token() -> str:
    r = requests.post(
        f"{ACCOUNTS}/oauth/v2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "refresh_token": REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    tok = data.get("access_token")
    if not tok:
        raise RuntimeError(f"No access_token: {data}")
    return tok

def get_all_fields(access_token: str) -> Set[str]:
    r = requests.get(
        f"{BASE}/contact/allfields?type=json",
        headers={"Authorization": f"Zoho-oauthtoken {access_token}"},
        timeout=30,
    )
    if r.status_code == 401:
        return {"Contact Email", "First Name", "Last Name", "Full Name", "Title", "Job Title"}
    r.raise_for_status()
    data = r.json()
    names = set()
    for f in data.get("response", {}).get("fieldnames", {}).get("fieldname", []):
        dn = f.get("DISPLAY_NAME")
        if dn:
            names.add(dn)
    if not names:
        names = {"Contact Email", "First Name", "Last Name", "Full Name"}
    return names

def get_mailing_lists(access_token: str, start: int = 1, range_: int = 200) -> List[Dict]:
    url = f"{BASE}/getmailinglists?resfmt=JSON&fromindex={start}&range={range_}&sort=asc"
    r = requests.get(url, headers={"Authorization": f"Zoho-oauthtoken {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("list_of_details", [])
    return [{"listname": it.get("listname"), "listkey": it.get("listkey"), "is_public": it.get("is_public")} for it in items]

def create_list_and_contacts(access_token: str, listname: str, description: str, emails_first_batch: List[str], signupform: str = "private") -> str:
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}", "Content-Type": "application/x-www-form-urlencoded"}
    params = {
        "resfmt": "JSON",
        "listname": listname,
        "signupform": signupform,   # "private" recomendado (evita confirmaciones)
        "mode": "newlist",
        "listdescription": description or "",
        "emailids": ",".join(emails_first_batch) if emails_first_batch else "",
    }
    r = requests.post(f"{BASE}/addlistandcontacts", headers=headers, data=params, timeout=60)
    if r.status_code == 400:
        st.error(f"❌ Error al crear lista: {r.text}")
    r.raise_for_status()
    data = r.json()

    if data.get("status") == "success" and data.get("listkey"):
        return data["listkey"]

    if data.get("code") in ("2205", 2205):
        lists = get_mailing_lists(access_token)
        for it in lists:
            if (it.get("listname") or "").strip().lower() == listname.strip().lower():
                return it["listkey"]

    raise RuntimeError(f"No se pudo obtener listkey. Respuesta: {data}")

def bulk_add_emails(access_token: str, listkey: str, emails: List[str]) -> Dict:
    assert 1 <= len(emails) <= 10
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"listkey": listkey, "resfmt": "JSON", "emailids": json.dumps(emails)}
    retries = 0
    while True:
        r = requests.post(f"{BASE}/addlistsubscribersinbulk", headers=headers, data=payload, timeout=60)
        if r.status_code == 429 and retries < RETRY_429_MAX:
            wait = 2 ** retries
            st.warning(f"[429] Rate limit. Reintentando en {wait}s…")
            time.sleep(wait); retries += 1
            continue
        if r.status_code == 401:
            raise RuntimeError("401 en bulk; renueva token y reintenta.")
        r.raise_for_status()
        return r.json()

def upsert_contact_fields(access_token: str, listkey: str, contactinfo: Dict) -> Dict:
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"listkey": listkey, "resfmt": "JSON", "contactinfo": json.dumps(contactinfo, ensure_ascii=False)}
    r = requests.post(f"{BASE}/json/listsubscribe", headers=headers, data=payload, timeout=60)
    r.raise_for_status()
    return r.json()

# =========================
# Helpers Email API (Templates v2)
# =========================
EMAILAPI_BASE = f"https://campaigns.zoho.{DC}/emailapi/v2"

def _auth_headers(access_token: str):
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Accept": "application/json",
    }

def list_templates(access_token: str, start_index: int = 1, end_index: int = 200):
    """
    Devuelve [{'template_id','template_name', ...}] o lanza Exception con detalle
    si la respuesta no es JSON (p.ej. HTML por 401/403/404).
    """
    url = f"{EMAILAPI_BASE}/templates?start_index={start_index}&end_index={end_index}"
    r = requests.get(url, headers=_auth_headers(access_token), timeout=30)

    # Guarda datos útiles para depurar
    status = r.status_code
    ctype = r.headers.get("Content-Type", "")

    if not r.ok:
        # Levanta error pero con cuerpo adjunto
        raise RuntimeError(f"HTTP {status} {url}\nContent-Type: {ctype}\nBody:\n{r.text[:800]}")

    # Intenta parsear JSON; si no, muestra "body" crudo
    try:
        data = r.json()
    except ValueError:
        raise RuntimeError(f"Respuesta no-JSON (HTTP {status}, {ctype}). Body (primeros 800 chars):\n{r.text[:800]}")

    templates = data.get("templates", [])
    return templates or []

def get_template_html(access_token: str, template_id: str) -> str:
    url = f"{EMAILAPI_BASE}/templates/{template_id}"
    r = requests.get(url, headers=_auth_headers(access_token), timeout=30)
    status = r.status_code
    ctype = r.headers.get("Content-Type", "")
    if not r.ok:
        raise RuntimeError(f"HTTP {status} {url}\nContent-Type: {ctype}\nBody:\n{r.text[:800]}")
    try:
        info = r.json()
    except ValueError:
        raise RuntimeError(f"Respuesta no-JSON (HTTP {status}, {ctype}). Body:\n{r.text[:800]}")
    return info.get("content") if info.get("content_type") == "html" else ""


# =========================
# UI
# =========================
st.set_page_config(page_title="Zoho Campaigns: Cargar contactos", page_icon="📧", layout="wide")
st.title("📧 Zoho Campaigns — Cargar contactos desde CSV")

with st.expander("¿Cómo evitar confirmaciones?"):
    st.markdown("- Crea la lista como **PRIVATE** (esta app lo hace por defecto al crearla).")
    st.markdown("- Si cargas en listas **públicas**, Zoho puede enviar confirmaciones según su configuración.")

# Estado para campos extra — ahora arranca VACÍO
if "extra_maps" not in st.session_state:
    st.session_state["extra_maps"] = []  # sin filas al inicio
if "mapeo_ok" not in st.session_state:
    st.session_state["mapeo_ok"] = False

# ========= PASO 1: Cargar CSV =========
st.header("① Cargar CSV")
uploaded = st.file_uploader("Sube tu CSV de contactos", type=["csv"])
df = None
if uploaded:
    try:
        raw = uploaded.read()
        try:
            df = pd.read_csv(io.BytesIO(raw))
        except Exception:
            df = pd.read_csv(io.BytesIO(raw), encoding="latin-1")
        df = df.fillna("")
        st.success(f"CSV: {uploaded.name} — {len(df)} filas")
        st.dataframe(df.head(20), width='stretch')
    except Exception as e:
        st.error(f"No se pudo leer el CSV: {e}")

# 2) Autenticación temprana para catálogo de campos Zoho (sugerencias extras)
valid_display_names: Set[str] = set()
if df is not None and len(df) > 0:
    try:
        access_preview = get_access_token()
        valid_display_names = get_all_fields(access_preview)
    except Exception as e:
        st.warning(f"No se pudieron leer campos de Zoho (se usará catálogo mínimo): {e}")
        valid_display_names = {"Contact Email", "First Name", "Last Name", "Full Name"}

# ========= PASO 2: Mapeo de columnas =========
if df is not None and len(df) > 0:
    st.header("② Mapear columnas")

    cols_lc = {c.strip().lower(): c for c in df.columns}
    # Heurística simple (detector aproximado por alias comunes)
    detected_email = cols_lc.get("work email (enterprise)") or cols_lc.get("email") or cols_lc.get("work email")
    detected_fn    = cols_lc.get("first name") or cols_lc.get("nombre") or cols_lc.get("name")
    detected_ln    = cols_lc.get("last name")  or cols_lc.get("apellido") or cols_lc.get("surname")
    detected_full  = cols_lc.get("full name")  or cols_lc.get("nombre completo")

    with st.form("form_mapeo"):
        st.subheader("2.1 Selección de columnas fijas")

        c1, c2 = st.columns(2)
        with c1:
            col_email = st.selectbox(
                "Columna EMAIL (obligatoria)",
                df.columns.tolist(),
                index=(df.columns.get_loc(detected_email) if detected_email in df.columns else 0),
            )
            col_last_name = st.selectbox(
                "Last Name (opcional)",
                ["(ninguna)"] + df.columns.tolist(),
                index=(0 if not detected_ln else df.columns.get_loc(detected_ln)+1),
            )
        with c2:
            col_first_name = st.selectbox(
                "First Name (opcional)",
                ["(ninguna)"] + df.columns.tolist(),
                index=(0 if not detected_fn else df.columns.get_loc(detected_fn)+1),
            )
            col_full_name = st.selectbox(
                "Full Name (opcional)",
                ["(ninguna)"] + df.columns.tolist(),
                index=(0 if not detected_full else df.columns.get_loc(detected_full)+1),
            )

        st.markdown("---")
        st.subheader("2.2 Campos extra (opcional)")
        st.caption("Agrega mapeos Zoho ↔ CSV adicionales según necesites.")

        # Botón DENTRO del form para agregar la primera/otra fila
        add_extra = st.form_submit_button("➕ Agregar campo extra", type="secondary")

        # Si se presiona, guardamos lo elegido y agregamos una fila vacía
        if add_extra:
            st.session_state["map_col_email"] = col_email
            st.session_state["map_col_first"] = None if col_first_name == "(ninguna)" else col_first_name
            st.session_state["map_col_last"]  = None if col_last_name  == "(ninguna)" else col_last_name
            st.session_state["map_col_full"]  = None if col_full_name  == "(ninguna)" else col_full_name
            st.session_state["extra_maps"].append({"zoho": "", "csv": ""})

        # Render de filas extra (si ya existen)
        zoho_options = sorted(valid_display_names)
        for i in range(len(st.session_state["extra_maps"])):
            zcol, ccol = st.columns(2)
            row = st.session_state["extra_maps"][i]

            # Campo Zoho
            idx_zoho = zoho_options.index(row["zoho"]) if row.get("zoho") in zoho_options else 0
            sel_zoho = zcol.selectbox(
                f"Campo Zoho #{i+1}",
                zoho_options,
                index=idx_zoho,
                key=f"extra_zoho_sel_{i}",
            )
            # Columna CSV
            csv_cols = df.columns.tolist()
            idx_csv = csv_cols.index(row["csv"]) if row.get("csv") in csv_cols else 0
            sel_csv = ccol.selectbox(
                f"Columna CSV #{i+1}",
                csv_cols,
                index=idx_csv,
                key=f"extra_csv_sel_{i}",
            )
            # Actualiza fila en memoria
            st.session_state["extra_maps"][i] = {"zoho": sel_zoho, "csv": sel_csv}

        # Botón para GUARDAR el mapeo completo
        saved = st.form_submit_button("💾 Guardar mapeo", type="primary")

    # Validación al guardar
    if saved:
        st.session_state["map_col_email"] = col_email
        st.session_state["map_col_first"] = None if col_first_name == "(ninguna)" else col_first_name
        st.session_state["map_col_last"]  = None if col_last_name  == "(ninguna)" else col_last_name
        st.session_state["map_col_full"]  = None if col_full_name  == "(ninguna)" else col_full_name

        # Limpia extras vacíos y evita sobrescribir Contact Email
        cleaned = []
        for m in st.session_state["extra_maps"]:
            z = str(m.get("zoho", "")).strip()
            c = str(m.get("csv", "")).strip()
            if z and c and z != "Contact Email":
                cleaned.append({"zoho": z, "csv": c})

        # Duplicados de CAMPO ZOHO en extras
        zoho_fields = [m["zoho"] for m in cleaned]
        dups = sorted({z for z in zoho_fields if zoho_fields.count(z) > 1})
        if dups:
            st.session_state["mapeo_ok"] = False
            st.error(f"Hay campo(s) Zoho repetido(s) en extras: {', '.join(dups)}. Corrige antes de continuar.")
        else:
            st.session_state["extra_maps"] = cleaned
            st.session_state["mapeo_ok"] = True
            st.success(f"✅ Mapeo guardado. Campos extra: {len(cleaned)}")

# ========= PASO 3: Lista (nueva o existente) y carga =========
if df is not None and len(df) > 0 and st.session_state.get("mapeo_ok"):
    st.header("③ Seleccionar lista y cargar")

    # Conectar a Zoho "real"
    try:
        access = get_access_token()
        valid_display_names = get_all_fields(access)
    except Exception as e:
        st.error(f"Error autenticando con Zoho: {e}")
        st.stop()

    # Helpers de mapeo
    m_email = st.session_state["map_col_email"]
    m_first = st.session_state.get("map_col_first")
    m_last  = st.session_state.get("map_col_last")
    m_full  = st.session_state.get("map_col_full")
    extra_maps: List[Dict[str, str]] = st.session_state.get("extra_maps", [])

    def enrich_all(listkey: str):
        st.subheader("Enriqueciendo campos por contacto…")
        updated, errors = 0, 0
        has_full_name = "Full Name" in valid_display_names if valid_display_names else False
        prog2 = st.progress(0.0, text="Enriqueciendo…")
        total_rows = len(df)
        for idx, row in df.iterrows():
            email = str(row.get(m_email, "")).strip()
            if not email or "@" not in email:
                prog2.progress((idx+1)/total_rows, text="Enriqueciendo…")
                continue

            ci = {"Contact Email": email}

            if m_first:
                v = str(row.get(m_first, "")).strip()
                if v: ci["First Name"] = v
            if m_last:
                v = str(row.get(m_last, "")).strip()
                if v: ci["Last Name"] = v
            if m_full and has_full_name:
                v = str(row.get(m_full, "")).strip()
                if v: ci["Full Name"] = v

            # Campos extra
            for m in extra_maps:
                zf, cf = m["zoho"], m["csv"]
                if zf == "Contact Email":
                    continue
                val = str(row.get(cf, "")).strip()
                if val:
                    ci[zf] = val

            try:
                upsert_contact_fields(access, listkey, ci)
                updated += 1
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 401:
                    new_access = get_access_token()
                    upsert_contact_fields(new_access, listkey, ci)
                    updated += 1
                else:
                    errors += 1
            time.sleep(SLEEP_SINGLE)
            prog2.progress((idx+1)/total_rows, text="Enriqueciendo…")

        # ahora devolvemos los números para que el caller arme el mensaje final
        return updated, errors


    # Elección de modo
    mode = st.radio("¿Qué deseas hacer?", ["Crear lista nueva", "Usar una existente"], horizontal=True)

    if mode == "Crear lista nueva":
        with st.form("form_crear_lista"):
            listname = st.text_input("Nombre de la nueva lista", value="")
            description = st.text_area("Descripción (opcional)", value="")
            private = st.checkbox("Crear como lista PRIVATE (evita confirmaciones)", value=True)
            submit_create = st.form_submit_button("Crear lista y cargar contactos", type="primary")

        if submit_create:
            if not listname.strip():
                st.error("Debes indicar un nombre de lista.")
                st.stop()

            emails_all = df[m_email].astype(str).str.strip()
            emails_valid = emails_all[emails_all.str.contains("@")].tolist()
            first_batch = emails_valid[:min(len(emails_valid), 10)]
            signupform = "private" if private else "public"

            try:
                with st.spinner("Creando lista en Zoho…"):
                    lk = create_list_and_contacts(access, listname.strip(), description.strip(), first_batch, signupform=signupform)
                st.success(f"Lista creada/obtenida ✅")


                remaining_emails = emails_valid[len(first_batch):]
                total_batches = math.ceil(len(remaining_emails) / BATCH_SIZE) if remaining_emails else 0
                if total_batches > 0:
                    prog = st.progress(0.0, text="Cargando emails en lotes…")
                    for i in range(total_batches):
                        batch = remaining_emails[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                        if not batch: break
                        try:
                            resp = bulk_add_emails(access, lk, batch)
                            st.write(f"Lote {i+1}/{total_batches} OK: {resp.get('message') or resp.get('status') or resp}")
                        except RuntimeError as e:
                            if "401 en bulk" in str(e):
                                st.info("401 en bulk: renovando token…")
                                access = get_access_token()
                                resp = bulk_add_emails(access, lk, batch)
                                st.write(f"Lote {i+1}/{total_batches} OK tras renovar: {resp.get('message') or resp}")
                            else:
                                st.warning(f"Lote {i+1} FALLÓ: {e} — continúo…")
                        except requests.HTTPError as e:
                            st.warning(f"Lote {i+1} FALLÓ: {e} — continúo…")
                        time.sleep(SLEEP_BULK)
                        prog.progress((i+1)/total_batches, text="Cargando emails en lotes…")
                else:
                    st.info("No hay correos restantes para cargar en bulk.")

                updated, errors = enrich_all(lk)

                st.success("Contactos cargados exitosamente.")
                st.write(f"Contactos cargados = {updated}")
                st.write(f"Errores = {errors}")


            except Exception as e:
                st.error(f"Error en creación/carga: {e}")

    else:
        lk_selected = None
        if FIXED_LIST_KEY.strip():
            st.info("Usando FIXED_LIST_KEY configurado en el código.")
            lk_selected = FIXED_LIST_KEY.strip()
        else:
            try:
                lists = get_mailing_lists(access)
                if not lists:
                    st.warning("No hay listas creadas. Crea una nueva en la pestaña anterior.")
                else:
                    name_to_key = {f"{it['listname']} (public={it.get('is_public')})": it["listkey"] for it in lists}
                    opt = st.selectbox("Elige una lista", list(name_to_key.keys()))
                    lk_selected = name_to_key.get(opt)
            except Exception as e:
                st.error(f"No se pudieron obtener listas: {e}")

        if lk_selected:
            if st.button("Cargar contactos a la lista seleccionada", type="primary"):
                try:
                    emails_all = df[m_email].astype(str).str.strip()
                    remaining_emails = emails_all[emails_all.str.contains("@")].tolist()
                    total_batches = math.ceil(len(remaining_emails) / BATCH_SIZE) if remaining_emails else 0

                    if total_batches == 0:
                        st.info("No hay emails válidos para cargar.")
                    else:
                        prog = st.progress(0.0, text="Cargando emails en lotes…")
                        for i in range(total_batches):
                            batch = remaining_emails[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                            if not batch: break
                            try:
                                resp = bulk_add_emails(access, lk_selected, batch)
                                st.write(f"Lote {i+1}/{total_batches} OK: {resp.get('message') or resp.get('status') or resp}")
                            except RuntimeError as e:
                                if "401 en bulk" in str(e):
                                    st.info("401 en bulk: renovando token…")
                                    access = get_access_token()
                                    resp = bulk_add_emails(access, lk_selected, batch)
                                    st.write(f"Lote {i+1}/{total_batches} OK tras renovar: {resp.get('message') or resp}")
                                else:
                                    st.warning(f"Lote {i+1} FALLÓ: {e} — continúo…")
                            except requests.HTTPError as e:
                                st.warning(f"Lote {i+1} FALLÓ: {e} — continúo…")
                            time.sleep(SLEEP_BULK)
                            prog.progress((i+1)/total_batches, text="Cargando emails en lotes…")

                    updated, errors = enrich_all(lk_selected)

                    st.success("Contactos cargados exitosamente.")
                    st.write(f"Contactos cargados = {updated}")
                    st.write(f"Errores = {errors}")


                except Exception as e:
                    st.error(f"Error cargando a lista existente: {e}")


# ========= PASO 4: Plantillas guardadas (solo listado/desplegable) =========
st.header("④ Plantillas guardadas (Templates API v2)")

try:
    # Reutiliza 'access' si ya existe; si no, renueva
    access_tpl = access if 'access' in locals() else get_access_token()

    c1, c2 = st.columns([1, 1])
    with c1:
        start_idx = st.number_input("start_index", min_value=1, value=1, step=1)
    with c2:
        end_idx = st.number_input("end_index", min_value=start_idx, value=start_idx + 199, step=1)

    if st.button("Listar plantillas"):
        try:
            templates = list_templates(access_tpl, start_index=start_idx, end_index=end_idx)
            if not templates:
                st.info("No se encontraron plantillas en ese rango.")
            else:
                options = {
                    f"{t.get('template_name','(sin nombre)')} — ID: {t.get('template_id')}": t.get('template_id')
                    for t in templates
                }
                sel = st.selectbox("Selecciona una plantilla", list(options.keys()))
                st.success("Plantillas cargadas.")
        except Exception as e:
            st.error(f"Error listando plantillas:\n{e}")


except requests.HTTPError as e:
    st.error(f"Error {e.response.status_code if e.response is not None else '?'} autenticando para templates: {getattr(e.response,'text',e)}")
except Exception as e:
    st.error(f"No se pudo preparar la sección de plantillas: {e}")
