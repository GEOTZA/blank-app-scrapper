# gemi_export_min.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import requests
import io, time, re
from urllib.parse import urljoin

st.set_page_config(page_title="ΓΕΜΗ – Εξαγωγή Excel ανά Περιοχή", layout="wide")
st.title("🏷️ ΓΕΜΗ – Εξαγωγή Επιχειρήσεων σε Excel (ανά περιοχή)")

# ------------- Helpers -------------
TIMEOUT = 40

def _fix_base(base: str) -> str:
    # καθάρισμα πιθανής ελληνικής 'ο' στο 'opendata'
    return (base or "").replace("οpendata", "opendata").rstrip("/")

def _headers(api_key: str, header_name: str):
    h = {"Accept": "application/json"}
    if api_key:
        h[header_name] = api_key
    return h

def _safe_get(url, headers, params=None, timeout=TIMEOUT, retries=3, base_delay=0.7):
    """
    GET με exponential backoff & σεβασμό Retry-After για 429.
    """
    last = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra is not None:
                    try:
                        wait = max(0.5, float(ra))
                    except Exception:
                        wait = base_delay * (2 ** i)
                else:
                    wait = base_delay * (2 ** i)
                time.sleep(wait)
                if i < retries:
                    continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            if i < retries:
                time.sleep(base_delay * (2 ** i))
            else:
                raise last

@st.cache_data(ttl=3600, show_spinner=False)
def get_params_cached(api_key: str, base: str, header_name: str, what: str, region_id=None):
    return gemi_params(api_key, base, header_name, what, region_id=region_id)

def gemi_params(api_key: str, base: str, header_name: str, what: str, *, region_id=None):
    """
    Φέρνει λίστες φίλτρων με λίγα και «ασφαλή» endpoints + fallbacks.
    """
    base = _fix_base(base)
    headers = _headers(api_key, header_name)
    def E(ep): return urljoin(base + "/", ep.lstrip("/"))

    endpoints = []
    if what == "regions":
        endpoints = ["params/regions", "params/perifereies", "params/peripheries", "params/nomoi"]
    elif what in ("regional_units","perifereiakes_enotites"):
        if region_id:
            endpoints = [f"params/regional_units/{region_id}",
                         f"params/perifereiakes_enotites/{region_id}",
                         f"params/periferiakes_enotites/{region_id}",
                         f"params/prefectures/{region_id}"]
        else:
            endpoints = ["params/regional_units", "params/perifereiakes_enotites",
                         "params/periferiakes_enotites", "params/prefectures"]
    elif what in ("dimoi","municipalities"):
        if region_id:
            endpoints = [f"params/dimoi/{region_id}", f"params/municipalities/{region_id}"]
        else:
            endpoints = ["params/dimoi", "params/municipalities"]
    elif what in ("statuses",):
        endpoints = ["params/statuses", "params/status", "params/company_statuses"]
    elif what in ("kad","kads"):
        endpoints = ["params/kad","params/kads","params/activity_codes","params/kad_codes","params/nace"]
    else:
        endpoints = [f"params/{what}"]

    last_err, tried = "", []
    for ep in endpoints:
        u = E(ep)
        tried.append(u)
        try:
            r = _safe_get(u, headers=headers)
            js = r.json()
            if isinstance(js, (list, dict)):
                return js
        except Exception as e:
            last_err = str(e)
            continue
    raise RuntimeError(f"ΓΕΜΗ: δεν βρέθηκε endpoint για '{what}'. Τελ. σφάλμα: {last_err}\nΔοκιμάστηκαν:\n" + "\n".join(tried[-6:]))

def gemi_companies_search(api_key: str, base: str, header_name: str, *,
                          page=1, per_page=200,
                          name_part=None,
                          region_id=None, regional_unit_id=None, municipality_id=None,
                          status_id=None, kad_list=None,
                          date_from=None, date_to=None):
    """
    GET {base}/companies (δύο παραλλαγές ονομάτων παραμέτρων).
    """
    base = _fix_base(base)
    headers = _headers(api_key, header_name)
    def B(path): return urljoin(base + "/", path.lstrip("/"))

    variants = [
        {
            "page": page, "per_page": per_page,
            "name": name_part, "name_part": name_part,
            "region_id": region_id, "regional_unit_id": regional_unit_id, "municipality_id": municipality_id,
            "perifereia_id": region_id, "perifereiaki_enotita_id": regional_unit_id, "dimos_id": municipality_id,
            "status_id": status_id,
            "kad": ",".join(kad_list) if kad_list else None,
            "incorporation_date_from": date_from, "incorporation_date_to": date_to,
            "foundation_date_from": date_from, "foundation_date_to": date_to,
            "registration_date_from": date_from, "registration_date_to": date_to,
        },
        {
            "page": page, "page_size": per_page,
            "name": name_part, "name_part": name_part,
            "regionId": region_id, "regionalUnitId": regional_unit_id, "municipalityId": municipality_id,
            "nomosId": regional_unit_id, "dimosId": municipality_id,
            "statusId": status_id,
            "kad": ",".join(kad_list) if kad_list else None,
            "incorporationDateFrom": date_from, "incorporationDateTo": date_to,
            "foundationDateFrom": date_from, "foundationDateTo": date_to,
            "registrationDateFrom": date_from, "registrationDateTo": date_to,
        },
    ]

    url = B("companies")
    last_err, last_keys = "", []
    for q in variants:
        q = {k: v for k, v in q.items() if v not in (None, "", [])}
        try:
            r = _safe_get(url, headers=headers, params=q)
            return r.json()
        except Exception as e:
            last_err = str(e)
            last_keys = list(q.keys())
            continue
    raise RuntimeError(f"ΓΕΜΗ: αναζήτηση απέτυχε. Τελευταίο σφάλμα: {last_err} (url={url}, keys={last_keys})")

def gemi_companies_all(api_key: str, base: str, header_name: str, *,
                       name_part=None,
                       region_id=None, regional_unit_id=None, municipality_id=None,
                       status_id=None, kad_list=None,
                       date_from=None, date_to=None,
                       per_page=200, max_pages=120):
    items = []
    for p in range(1, max_pages + 1):
        js = gemi_companies_search(
            api_key, base, header_name,
            page=p, per_page=per_page,
            name_part=name_part,
            region_id=region_id, regional_unit_id=regional_unit_id, municipality_id=municipality_id,
            status_id=status_id, kad_list=kad_list,
            date_from=date_from, date_to=date_to,
        )
        arr = js.get("items") or js.get("data") or js.get("results") or []
        items.extend(arr)
        total = js.get("total") or js.get("total_count")
        if total and len(items) >= int(total):
            break
        if not arr or len(arr) < per_page:
            break
        time.sleep(0.25)  # μικρή παύση για rate limit
    return items

def items_to_df(items: list[dict]) -> pd.DataFrame:
    def first(d, keys, default=""):
        for k in keys:
            v = d.get(k)
            if v is not None and str(v).strip() != "":
                return v
        return default

    rows = []
    for it in items:
        raw_kads = it.get("kads") or it.get("kad") or it.get("activity_codes")
        if isinstance(raw_kads, list):
            def _x(x):
                if isinstance(x, dict):
                    return x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
                return str(x)
            kad_join = ";".join([_x(x) for x in raw_kads if x])
        else:
            kad_join = str(raw_kads or "")
        rows.append({
            "region": first(it, ["region","perifereia","region_name"]),
            "regional_unit": first(it, ["regional_unit","perifereiaki_enotita","nomos_name","prefecture"]),
            "municipality": first(it, ["municipality","dimos_name","city","town"]),
            "name": first(it, ["name","company_name","commercial_name","registered_name"]),
            "afm": first(it, ["afm","vat_number","tin"]),
            "gemi": first(it, ["gemi_number","registry_number","commercial_registry_no","ar_gemi","arGemi"]),
            "legal_form": first(it, ["legal_form","company_type","form"]),
            "status": first(it, ["status","company_status","status_name"]),
            "incorporation_date": first(it, ["incorporation_date","foundation_date","establishment_date","founded_at","registration_date"]),
            "address": first(it, ["address","postal_address","registered_address","address_line"]),
            "postal_code": first(it, ["postal_code","zip","tk","postcode"]),
            "phone": first(it, ["phone","telephone","contact_phone","phone_number"]),
            "email": first(it, ["email","contact_email","email_address"]),
            "website": first(it, ["website","site","url","homepage"]),
            "kad_codes": kad_join,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["incorporation_date"] = df["incorporation_date"].astype(str).str.strip()
        df = df.drop_duplicates().reset_index(drop=True)
    return df

def to_excel_bytes(df: pd.DataFrame):
    out = io.BytesIO()
    safe = df.copy()
    if safe is None or safe.empty:
        safe = pd.DataFrame([{"info": "no data"}])
    safe.columns = [str(c) for c in safe.columns]
    for c in safe.columns:
        safe[c] = safe[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        safe.to_excel(w, index=False, sheet_name="companies")
    out.seek(0)
    return out

# ------------- UI (μινιμαλιστικό) -------------
with st.sidebar:
    st.header("API")
    base = st.text_input("Base URL", value="https://opendata-api.businessportal.gr/api/opendata/v1")
    header_name = st.text_input("Header name", value="api_key")
    api_key = st.text_input("API Key", type="password")

    st.markdown("---")
    st.caption("Για να μειώσουμε τα 429, οι λίστες φορτώνουν μόνο με κουμπιά:")
    b_regions = st.button("① Φόρτωσε Περιφέρειες")
    b_units   = st.button("② Φόρτωσε Περιφ. Ενότητες (μετά την επιλογή Περιφέρειας)")
    b_muni    = st.button("③ Φόρτωσε Δήμους (μετά την επιλογή Π.Ε.)")
    b_status  = st.button("④ Φόρτωσε Καταστάσεις (προαιρετικό)")
    b_kad     = st.button("⑤ Φόρτωσε ΚΑΔ (προαιρετικό)")

# session stores
for key in ["regions_map","runits_map","muni_map","status_map","kad_label_to_code"]:
    if key not in st.session_state:
        st.session_state[key] = {}

# Load lists (on demand)
try:
    if b_regions:
        r = get_params_cached(api_key, base, header_name, "regions")
        mp = {}
        if isinstance(r, list):
            for x in r:
                rid = x.get("id") or x.get("code") or x.get("region_id") or x.get("nomos_id")
                rname = x.get("name") or x.get("title") or x.get("label")
                if rid and rname:
                    mp[rname] = rid
        st.session_state["regions_map"] = mp
        st.success(f"Φορτώθηκαν Περιφέρειες: {len(mp)}")

    # selections (show whatever we have)
    region_names = ["— Όλες —"] + sorted(st.session_state["regions_map"].keys()) if st.session_state["regions_map"] else ["— Όλες —"]
    sel_region_name = st.selectbox("Περιφέρεια", region_names, index=0)
    sel_region_id = st.session_state["regions_map"].get(sel_region_name)

    if b_units:
        if not sel_region_id:
            st.warning("Διάλεξε πρώτα Περιφέρεια.")
        else:
            u = get_params_cached(api_key, base, header_name, "regional_units", region_id=sel_region_id)
            mp = {}
            if isinstance(u, list):
                for x in u:
                    uid = x.get("id") or x.get("code") or x.get("regional_unit_id") or x.get("prefecture_id")
                    uname = x.get("name") or x.get("title") or x.get("label")
                    if uid and uname:
                        mp[uname] = uid
            st.session_state["runits_map"] = mp
            st.success(f"Φορτώθηκαν Περιφερειακές Ενότητες: {len(mp)}")

    runit_names = ["— Όλες —"] + sorted(st.session_state["runits_map"].keys()) if st.session_state["runits_map"] else ["— Όλες —"]
    sel_runit_name = st.selectbox("Περιφερειακή Ενότητα", runit_names, index=0)
    sel_runit_id = st.session_state["runits_map"].get(sel_runit_name)

    if b_muni:
        if not sel_runit_id:
            st.warning("Διάλεξε πρώτα Περιφερειακή Ενότητα.")
        else:
            m = get_params_cached(api_key, base, header_name, "dimoi", region_id=sel_runit_id)
            mp = {}
            if isinstance(m, list):
                for x in m:
                    mid = x.get("id") or x.get("code") or x.get("municipality_id") or x.get("dimos_id")
                    mname = x.get("name") or x.get("title") or x.get("label")
                    if mid and mname:
                        mp[mname] = mid
            st.session_state["muni_map"] = mp
            st.success(f"Φορτώθηκαν Δήμοι: {len(mp)}")

    muni_names = ["— Όλοι —"] + sorted(st.session_state["muni_map"].keys()) if st.session_state["muni_map"] else ["— Όλοι —"]
    sel_muni_name = st.selectbox("Δήμος", muni_names, index=0)
    sel_muni_id = st.session_state["muni_map"].get(sel_muni_name)

    if b_status:
        s = get_params_cached(api_key, base, header_name, "statuses")
        mp = {}
        if isinstance(s, list):
            for x in s:
                sid = x.get("id") or x.get("code")
                sname = x.get("name") or x.get("title")
                if sid and sname:
                    mp[sname] = sid
        st.session_state["status_map"] = mp
        st.success(f"Φορτώθηκαν καταστάσεις: {len(mp)}")

    status_names = ["— Όλες —"] + sorted(st.session_state["status_map"].keys()) if st.session_state["status_map"] else ["— Όλες —"]
    # προσπαθώ να προεπιλέξω «ενεργές»
    default_idx = 0
    for i, nm in enumerate(status_names):
        if "ενεργ" in nm.lower():
            default_idx = i; break
    sel_status_name = st.selectbox("Κατάσταση", status_names, index=default_idx)
    sel_status_id = st.session_state["status_map"].get(sel_status_name)

    if b_kad:
        k = get_params_cached(api_key, base, header_name, "kad")
        lbl_to_code = {}
        if isinstance(k, list):
            def _lbl(x):
                if isinstance(x, dict):
                    code = x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
                    desc = x.get("name") or x.get("title") or x.get("description") or ""
                    return f"{code} — {desc}".strip(" —")
                return str(x)
            for x in k:
                if not isinstance(x, dict): continue
                code = (x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or "").strip()
                if code:
                    lbl_to_code[_lbl(x)] = code
        st.session_state["kad_label_to_code"] = lbl_to_code
        st.success(f"Φορτώθηκαν ΚΑΔ: {len(lbl_to_code)}")

    kad_labels = sorted(st.session_state["kad_label_to_code"].keys())
    sel_kad_labels = st.multiselect("ΚΑΔ (προαιρετικό)", kad_labels, default=[])
    sel_kads = [st.session_state["kad_label_to_code"][l] for l in sel_kad_labels]

except Exception as e:
    st.error(f"Σφάλμα φόρτωσης λιστών: {e}")

# --- Ελεύθερα φίλτρα ---
name_part = st.text_input("Επωνυμία περιέχει (προαιρετικό)", "")
c1, c2 = st.columns(2)
with c1:
    date_from = st.text_input("Σύσταση από (YYYY-MM-DD)", "")
with c2:
    date_to = st.text_input("Σύσταση έως (YYYY-MM-DD)", "")

cA, cB = st.columns(2)
with cA:
    do_preview = st.button("🔎 Προεπισκόπηση (μέχρι 200 εγγραφές)")
with cB:
    do_export = st.button("⬇️ Εξαγωγή Excel (όλες οι σελίδες)")

def _apply_safety_filters(df: pd.DataFrame):
    out = df.copy()
    if not out.empty and (date_from or date_to):
        dser = pd.to_datetime(out["incorporation_date"], errors="coerce").dt.date
        if date_from:
            try:
                dmin = pd.to_datetime(date_from, errors="coerce").date()
                out = out[dser >= dmin]
            except Exception:
                pass
        if date_to:
            try:
                dmax = pd.to_datetime(date_to, errors="coerce").date()
                out = out[dser <= dmax]
            except Exception:
                pass
    if not out.empty and sel_kads:
        patt = "|".join([re.escape(k) for k in sel_kads])
        out = out[out["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]
    return out

if do_preview:
    try:
        js = gemi_companies_search(
            api_key, base, header_name,
            page=1, per_page=200,
            name_part=(name_part or None),
            region_id=sel_region_id, regional_unit_id=sel_runit_id, municipality_id=sel_muni_id,
            status_id=sel_status_id, kad_list=sel_kads or None,
            date_from=(date_from or None), date_to=(date_to or None),
        )
        items = js.get("items") or js.get("data") or js.get("results") or []
        df = items_to_df(items)
        df = _apply_safety_filters(df)
        if df.empty:
            st.warning("Δεν βρέθηκαν εγγραφές.")
        else:
            st.success(f"Βρέθηκαν {len(df)} εγγραφές (προεπισκόπηση).")
            st.dataframe(df, use_container_width=True)
            st.download_button("⬇️ Κατέβασμα προεπισκόπησης (Excel)", to_excel_bytes(df), file_name="gemi_preview.xlsx")
    except Exception as e:
        st.error(f"Σφάλμα αναζήτησης: {e}")

if do_export:
    try:
        with st.spinner("Γίνεται εξαγωγή…"):
            all_items = gemi_companies_all(
                api_key, base, header_name,
                name_part=(name_part or None),
                region_id=sel_region_id, regional_unit_id=sel_runit_id, municipality_id=sel_muni_id,
                status_id=sel_status_id, kad_list=sel_kads or None,
                date_from=(date_from or None), date_to=(date_to or None),
                per_page=200, max_pages=200
            )
            df = items_to_df(all_items)
            df = _apply_safety_filters(df)
            if df.empty:
                st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
            else:
                st.success(f"Έτοιμο: {len(df)} εγγραφές.")
                st.dataframe(df.head(50), use_container_width=True)
                st.download_button("⬇️ Excel – Επιχειρήσεις (με φίλτρα)", to_excel_bytes(df), file_name="gemi_filtered.xlsx")
    except Exception as e:
        st.error(f"Σφάλμα εξαγωγής: {e}")
