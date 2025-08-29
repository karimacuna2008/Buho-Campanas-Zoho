import json, time, math, sys
import pandas as pd
import requests

# ========= CONFIG DIRECTO (ajusta SOLO esto) =========
CLIENT_ID     = "1000.YAZ4PANZGEM7J6IQ1SWP9GY8NJBD4V"
CLIENT_SECRET = "3d707459d66ea9afc1df5bdeac7f8fb46945e472bb"
# Refresh token con scopes: CREATE, UPDATE, READ
REFRESH_TOKEN = "1000.27580f95a6dc620cad7fadef2011a212.e504a6a43036e25ff2e7077fa57a7370"
DC            = "com"   # México -> "com"
# Si eliges usar lista existente y NO quieres que el script la pregunte por API,
# puedes fijarla aquí; si está vacío, el script te mostrará las listas por API.
FIXED_LIST_KEY = ""  # ej. "3zf9e..."  o déjalo vacío para seleccionar en tiempo de ejecución
# =====================================================

BATCH_SIZE    = 10
SLEEP_BULK    = 0.2
SLEEP_SINGLE  = 0.05
RETRY_429_MAX = 5

ACCOUNTS = f"https://accounts.zoho.{DC}"
BASE     = f"https://campaigns.zoho.{DC}/api/v1.1"

# ---------- OAuth ----------
def get_access_token() -> str:
    r = requests.post(f"{ACCOUNTS}/oauth/v2/token",
                      headers={"Content-Type": "application/x-www-form-urlencoded"},
                      data={
                          "refresh_token": REFRESH_TOKEN,
                          "client_id": CLIENT_ID,
                          "client_secret": CLIENT_SECRET,
                          "grant_type": "refresh_token",
                      }, timeout=30)
    r.raise_for_status()
    data = r.json()
    tok = data.get("access_token")
    if not tok:
        raise RuntimeError(f"No access_token: {data}")
    return tok

# ---------- Helpers de API ----------
def get_all_fields(access_token: str) -> set[str]:
    """Devuelve los Display Names válidos (p.ej. 'First Name', 'Job Title')."""
    r = requests.get(f"{BASE}/contact/allfields?type=json",
        headers={"Authorization": f"Zoho-oauthtoken {access_token}"}, timeout=30)
    if r.status_code == 401:
        # Fallback si falta READ (no debería, pero lo manejamos)
        print("⚠️  No se pudo leer campos (falta READ). Sigo con estándar.")
        return {"Contact Email", "First Name", "Last Name", "Job Title", "Title"}
    r.raise_for_status()
    data = r.json()
    names = set()
    for f in data.get("response", {}).get("fieldnames", {}).get("fieldname", []):
        dn = f.get("DISPLAY_NAME")
        if dn: names.add(dn)
    return names

def bulk_add_emails(access_token: str, listkey: str, emails: list[str]) -> dict:
    """addlistsubscribersinbulk: SOLO emails (1..10)"""
    assert 1 <= len(emails) <= 10
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    payload = {"listkey": listkey, "resfmt": "JSON", "emailids": json.dumps(emails)}
    retries = 0
    while True:
        r = requests.post(f"{BASE}/addlistsubscribersinbulk",
                          headers=headers, data=payload, timeout=60)
        if r.status_code == 429 and retries < RETRY_429_MAX:
            wait = 2 ** retries
            print(f"[429] Rate limit. Reintentando en {wait}s…")
            time.sleep(wait); retries += 1; continue
        if r.status_code == 401:
            raise RuntimeError("401 en bulk; renueva token y reintenta.")
        r.raise_for_status()
        return r.json()

def upsert_contact_fields(access_token: str, listkey: str, contactinfo: dict) -> dict:
    """listsubscribe: upsert por contacto (Display Name -> valor)"""
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    payload = {"listkey": listkey, "resfmt": "JSON", "contactinfo": json.dumps(contactinfo, ensure_ascii=False)}
    r = requests.post(f"{BASE}/json/listsubscribe", headers=headers, data=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def get_mailing_lists(access_token: str, start: int = 1, range_: int = 200) -> list[dict]:
    """Obtiene listas (listname, listkey) para seleccionar."""
    url = f"{BASE}/getmailinglists?resfmt=JSON&fromindex={start}&range={range_}&sort=asc"
    r = requests.get(url, headers={"Authorization": f"Zoho-oauthtoken {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("list_of_details", [])
    out = []
    for it in items:
        out.append({"listname": it.get("listname"), "listkey": it.get("listkey"), "is_public": it.get("is_public")})
    return out

def create_list_and_contacts(access_token: str, listname: str, description: str, emails_first_batch: list[str], signupform: str = "private") -> str:
    """
    Crea lista y agrega hasta 10 emails iniciales.
    signupform: 'private' para evitar confirmaciones (recomendado).
    Devuelve el listkey.
    """
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}", "Content-Type": "application/x-www-form-urlencoded"}
    params = {
        "resfmt": "JSON",
        "listname": listname,
        "signupform": signupform,     # 'private' o 'public'
        "mode": "newlist",
        "listdescription": description or "",
        "emailids": ",".join(emails_first_batch) if emails_first_batch else "",
    }
    r = requests.post(f"{BASE}/addlistandcontacts", headers=headers, data=params, timeout=60)
    if r.status_code == 400:
        print("❌ Error al crear lista:", r.text)
    r.raise_for_status()
    data = r.json()
    if data.get("status") == "success" and data.get("listkey"):
        return data["listkey"]
    # Duplicado de nombre (2205): buscamos el listkey por nombre
    if data.get("code") in ("2205", 2205):
        print("ℹ️  Ya existe una lista con ese nombre. Buscando su listkey…")
        lists = get_mailing_lists(access_token)
        for it in lists:
            if (it.get("listname") or "").strip().lower() == listname.strip().lower():
                return it["listkey"]
    raise RuntimeError(f"No se pudo obtener listkey. Respuesta: {data}")

# ---------- File picker ----------
def pick_csv_path() -> str:
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk(); root.withdraw()
        path = filedialog.askopenfilename(
            title="Selecciona el CSV de contactos",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if not path:
            print("No seleccionaste archivo. Saliendo.")
            sys.exit(0)
        return path
    except Exception:
        return input("Ruta del CSV: ").strip()

# ---------- MAIN ----------
def main():
    # 0) Elegir CSV
    csv_path = pick_csv_path()
    print(f"CSV: {csv_path}")
    df = pd.read_csv(csv_path).fillna("")
    cols = {c.strip().lower(): c for c in df.columns}

    col_email      = cols.get("work email (enterprise)") or cols.get("email") or cols.get("work email")
    col_first_name = cols.get("first name")
    col_last_name  = cols.get("last name")
    col_full_name  = cols.get("full name")
    col_title      = cols.get("title")

    if not col_email:
        raise SystemExit("El CSV debe tener columna 'Work Email (Enterprise)' o 'email'.")

    # 1) Token + catálogo de campos
    access = get_access_token()
    valid_display_names = get_all_fields(access)
    has_full_name = "Full Name" in valid_display_names

    # 2) Decidir: crear lista o usar existente
    mode = input("¿Qué deseas hacer? [1=Crear lista nueva, 2=Usar una existente] (1/2): ").strip() or "2"

    listkey = FIXED_LIST_KEY.strip()
    if mode == "1":
        listname = input("Nombre para la nueva lista: ").strip()
        if not listname:
            raise SystemExit("Debes indicar un nombre de lista.")
        desc = input("Descripción (opcional): ").strip()
        # Primeros emails (hasta 10) para crear + poblar
        emails_all = df[col_email].astype(str).str.strip()
        emails_valid = emails_all[emails_all.str.contains("@")].tolist()
        first_batch = emails_valid[:min(len(emails_valid), 10)]
        print(f"Creando lista '{listname}' (private) con {len(first_batch)} email(s) inicial(es)…")
        listkey = create_list_and_contacts(access, listname, desc, first_batch, signupform="private")
        print(f"✅ Lista creada/obtenida. listkey = {listkey}")
        # Quita los que ya se enviaron en la creación
        remaining_emails = emails_valid[len(first_batch):]
    else:
        if not listkey:
            # Mostrar listas por API para que elijas
            lists = get_mailing_lists(access)
            if not lists:
                print("No hay listas. Crea una nueva (opción 1).")
                return
            print("\nListas disponibles:")
            for i, it in enumerate(lists, 1):
                print(f"{i:2d}. {it['listname']}   (listkey={it['listkey']}, public={it.get('is_public')})")
            idx = int(input("Selecciona # de lista: ").strip())
            listkey = lists[idx-1]["listkey"]
        print(f"Usando lista existente: {listkey}")
        emails_all = df[col_email].astype(str).str.strip()
        remaining_emails = emails_all[emails_all.str.contains("@")].tolist()

    # 3) BULK de emails restantes en lotes de 10
    print(f"\nCargando {len(remaining_emails)} email(s) en lotes de {BATCH_SIZE}…")
    total_batches = math.ceil(len(remaining_emails) / BATCH_SIZE) if remaining_emails else 0
    for i in range(total_batches):
        batch = remaining_emails[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
        if not batch: break
        try:
            resp = bulk_add_emails(access, listkey, batch)
            print(f"Lote {i+1}/{total_batches} OK: {resp.get('message') or resp.get('status') or resp}")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                print("401 en bulk: renovando token…")
                access = get_access_token()
                resp = bulk_add_emails(access, listkey, batch)
                print(f"Lote {i+1}/{total_batches} OK tras renovar: {resp.get('message') or resp}")
            else:
                print(f"Lote {i+1} FALLÓ: {e} — continúo…")
        time.sleep(SLEEP_BULK)

    # 4) Enriquecimiento por contacto (First/Last/Title/Full Name si existe)
    print("\nEnriqueciendo campos por contacto (listsubscribe)…")
    updated, errors = 0, 0
    for idx, row in df.iterrows():
        email = str(row.get(col_email, "")).strip()
        if not email or "@" not in email:
            continue
        ci = {"Contact Email": email}
        if col_first_name:
            v = str(row.get(col_first_name, "")).strip()
            if v: ci["First Name"] = v
        if col_last_name:
            v = str(row.get(col_last_name, "")).strip()
            if v: ci["Last Name"] = v
        if col_title:
            v = str(row.get(col_title, "")).strip()
            if v:
                if "Job Title" in valid_display_names:
                    ci["Job Title"] = v
                elif "Title" in valid_display_names:
                    ci["Title"] = v
        if col_full_name and has_full_name:
            v = str(row.get(col_full_name, "")).strip()
            if v: ci["Full Name"] = v

        try:
            upsert_contact_fields(access, listkey, ci)
            updated += 1
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                access = get_access_token()
                upsert_contact_fields(access, listkey, ci)
                updated += 1
            else:
                print(f"Error fila {idx+1} ({email}): {e}")
                errors += 1
        time.sleep(SLEEP_SINGLE)

    print(f"\n✅ Terminado. listkey={listkey}  Enriquecidos={updated}  Errores={errors}")
    print("Si no quieres correos de confirmación, mantén la lista como 'private' (sin signup form).")

if __name__ == "__main__":
    main()
