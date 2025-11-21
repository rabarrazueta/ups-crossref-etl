# AUTOR: Robinson Barrazueta

import requests
import unicodedata
import re
import time
import html
import sqlite3
import json
import os
import pandas as pd
from typing import Optional

# Configuración de Request hacia API de CrossRef
BASE = "https://api.crossref.org/works" # API
MAILTO = "rabarrazueta@utpl.edu.ec" # Como buena práctica, se debe indicar la identidad del usuario que hace la petición
HEADERS = {"User-Agent": f"UPS-ETL/1.0 (mailto:{MAILTO})"}
DB_NAME = "barrazueta_db_ups_crossref.db" # Nombre de la base de datos

UPS_TARGET = "Universidad Politécnica Salesiana"  # Busca solo aquellos trabajos afiliados a UPS
UPS_TARGET_NORM = None  # para normalización

# filtros de búsqueda
FROM = "2022-01-01"
UNTIL = "2025-11-30"
ROWS = 500
MAX_WORKS = 1_000_000 # Variable para limitar la búsqueda de obras
NO_HITS_LIMIT = 15 # Para evitar bucles en paginación

USE_VARIANTS = False  # cambiar a True para cubrir más posibles afiliaciones no detectadas
UPS_VARIANTS = [
    "universidad politécnica salesiana",
    "universidad politecnica salesiana",
    "salesian polytechnic university",
]

INSERT_SUBJECTS = True  # para insertar el tema del artículo de encontrarlo

# Generación de Sesión HTTP
session = requests.Session()
session.headers.update(HEADERS)

# Función para reintentos en caso de errores de conexión
def get_with_retry(url, params, max_tries=6, base_backoff=1.0, max_backoff=30.0, timeout=60):
    tries = 0
    backoff = base_backoff
    local_params = dict(params)
    while True:
        resp = session.get(url, params=local_params, timeout=timeout)

        if resp.status_code == 400:
            try:
                print("400 Bad Request:", resp.json())
            except Exception:
                print("400 Bad Request (raw):", resp.text[:500])

            if "select" in local_params:
                local_params.pop("select", None)
                print("Reintentando ahora sin 'select'…")
                continue

            changed = False
            if "sort" in local_params:
                local_params.pop("sort", None); changed = True
            if "order" in local_params:
                local_params.pop("order", None); changed = True
            if changed:
                print("Reintentando ahora sin 'sort/order'…")
                continue

            if "filter" in local_params and "has-affiliation:true" in local_params["filter"]:
                only_dates = ",".join([f"from-pub-date:{FROM}", f"until-pub-date:{UNTIL}"])
                local_params["filter"] = only_dates
                print("Reintentando ahora con filter solo por fechas…")
                continue

            resp.raise_for_status()

        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else backoff
            time.sleep(wait)
            tries += 1
            backoff = min(backoff * 2, max_backoff)
            if tries >= max_tries:
                try:
                    print("Última respuesta:", resp.json())
                except Exception:
                    print("Última respuesta (raw):", resp.text[:500])
                resp.raise_for_status()
            continue

        resp.raise_for_status()
        return resp

# FUNCIONES DE NORMALIZACIÓN
def norm_text_nfc(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_text_nfkd_lower(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

# para convertir el DOI de un link a un formato estándar
def standardize_doi(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    s = html.unescape(s)
    s = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:\s*)", "", s, flags=re.IGNORECASE)
    s = s.strip().lower()
    return s if s.startswith("10.") else s

# para obtener el año de la publicación analziada
def extract_year_any(msg: dict) -> Optional[int]:
    def _first_year(key):
        parts = (msg.get(key) or {}).get("date-parts") or []
        if parts and parts[0] and parts[0][0]:
            try:
                return int(parts[0][0])
            except Exception:
                return None
        return None
    for key in ("published-online", "published-print", "issued", "created"):
        y = _first_year(key)
        if y and 1600 <= y <= 2100:
            return y
    return None

# para construcción de nombres de los autores
def author_full_name(au: dict) -> str:
    name = " ".join(p for p in [au.get("given", ""), au.get("family", "")] if p).strip()
    if not name and au.get("name"):
        name = au["name"].strip()
    return norm_text_nfc(name)

# para normalización de la afiliación del documento
def normalize_aff_name(aff_name: str):
    return norm_text_nfc(aff_name), norm_text_nfkd_lower(aff_name)

# para extracción de la fecha completa de publicación del documento
# si es posible, devuelve: fecha publicado online, publicado impreso, emitido y creado
def extract_date_iso(msg: dict) -> Optional[str]:
    def _parts(key):
        dp = (msg.get(key) or {}).get("date-parts") or []
        return dp[0] if dp and dp[0] else None

    for key in ("published-online", "published-print", "issued", "created"):
        p = _parts(key)
        if p and p[0]:
            y = int(p[0]); m = int(p[1]) if len(p) > 1 else 1; d = int(p[2]) if len(p) > 2 else 1
            if 1600 <= y <= 2100:
                return f"{y:04d}-{m:02d}-{d:02d}"
    return None

# códigos de país para clasificación en dashboard
COUNTRY_PATTERNS = {
    "ecuador": ("EC", "Ecuador"),
    "spain|españa": ("ES", "Spain"),
    "peru|perú": ("PE", "Peru"),
    "colombia": ("CO", "Colombia"),
    "chile": ("CL", "Chile"),
    "argentina": ("AR", "Argentina"),
    "mexico|méxico": ("MX", "Mexico"),
    "brazil|brasil": ("BR", "Brazil"),
    "united states|usa|u\\.s\\.a\\.|u\\.s\\.|estados unidos": ("US", "United States"),
    "canada|canadá": ("CA", "Canada"),
    "united kingdom|uk|u\\.k\\.|inglaterra|reino unido": ("GB", "United Kingdom"),
    "france|francia": ("FR", "France"),
    "germany|alemania": ("DE", "Germany"),
    "italy|italia": ("IT", "Italy"),
    "china": ("CN", "China"),
    "japan|japón": ("JP", "Japan"),
    # se puede añadir más países
}

# para encontrar el país de origen de la publicación, enlazado a los códigos anteriores
def guess_country_from_text(s: str):
    if not s:
        return None, None
    s_norm = norm_text_nfkd_lower(s)
    for pat, (cc, name) in COUNTRY_PATTERNS.items():
        if re.search(rf"\b({pat})\b", s_norm):
            return cc, name
    return None, None

# CREACIÓN DE LA BASE DE DATOS

# función para asegurar idempotencia en la db (no se altera las columnas de la db sin importar que se ejecute el código varias veces)
def _ensure_column(conn: sqlite3.Connection, table: str, col: str, decl: str):
    cur = conn.cursor()
    have = [r[1].lower() for r in cur.execute(f"PRAGMA table_info('{table}')")]
    if col.lower() not in have:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl};")
    conn.commit()

# función para crear la estructura de la db lista para la introducción de datos
def create_db_schema(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;") # buenas prácticas de SQLite

    # creación de estructura de tablas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Sedes_Areas (
            SedeID INTEGER PRIMARY KEY,
            Sede TEXT NOT NULL,
            AreaAcademica TEXT
        );
    """)
    cursor.executemany("INSERT OR IGNORE INTO Sedes_Areas (SedeID, Sede, AreaAcademica) VALUES (?, ?, ?)", [
        (1, "Sede Cuenca", "Ciencias de la Vida"),
        (2, "Sede Quito", "Ingenierías y Arquitectura"),
        (3, "Sede Guayaquil", "Ciencias Sociales y Humanas"),
        (4, "Otra", "No definida"),
    ])

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Obras (
            DOI TEXT PRIMARY KEY,
            Titulo TEXT,
            Anio INTEGER,
            Revista TEXT,
            Editorial TEXT,
            Tipo TEXT,
            Citas INTEGER,
            Referencias INTEGER
        );
    """)

    _ensure_column(conn, "Obras", "FechaPublicacion", "TEXT")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Obra_Tema (
            DOI TEXT,
            Tema TEXT,
            PRIMARY KEY (DOI, Tema),
            FOREIGN KEY (DOI) REFERENCES Obras(DOI)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Autores (
            AutorID INTEGER PRIMARY KEY AUTOINCREMENT,
            NombreLimpio TEXT NOT NULL,
            NombreBusqueda TEXT NOT NULL UNIQUE,
            Orcid TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Afiliaciones (
            AfiliacionID INTEGER PRIMARY KEY AUTOINCREMENT,
            CadenaLiteral TEXT NOT NULL,
            AfiliacionBusqueda TEXT NOT NULL UNIQUE,
            SedeID INTEGER,
            FOREIGN KEY(SedeID) REFERENCES Sedes_Areas(SedeID)
        );
    """)

    _ensure_column(conn, "Afiliaciones", "CountryCode", "TEXT")
    _ensure_column(conn, "Afiliaciones", "CountryName", "TEXT")
    _ensure_column(conn, "Afiliaciones", "EsUPS", "INTEGER DEFAULT 0")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Obra_Autor_Afiliacion (
            DOI TEXT,
            AutorID INTEGER,
            AfiliacionID INTEGER,
            AutorSecuencia TEXT,
            PRIMARY KEY (DOI, AutorID, AfiliacionID),
            FOREIGN KEY(DOI) REFERENCES Obras(DOI),
            FOREIGN KEY(AutorID) REFERENCES Autores(AutorID),
            FOREIGN KEY(AfiliacionID) REFERENCES Afiliaciones(AfiliacionID)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Runs (
            RunID INTEGER PRIMARY KEY AUTOINCREMENT,
            StartedAt TEXT,
            EndedAt TEXT,
            Query TEXT,
            CursorInicio TEXT,
            CursorFin TEXT,
            RowsIngested INTEGER,
            Notes TEXT
        );
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_Obras_Anio ON Obras(Anio);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_Obras_Fecha ON Obras(FechaPublicacion);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_OAA_Autor ON Obra_Autor_Afiliacion(AutorID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_OAA_Afiliacion ON Obra_Autor_Afiliacion(AfiliacionID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_Afiliaciones_Sede ON Afiliaciones(SedeID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_Afiliaciones_CC ON Afiliaciones(CountryCode);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_Afiliaciones_UPS ON Afiliaciones(EsUPS);")
    conn.commit()

# para obtención de autores, orcid, etc. evita duplicar autores
def get_or_insert_author(conn: sqlite3.Connection, name_nfc: str, orcid: Optional[str] = None) -> int:
    cursor = conn.cursor()
    name_norm = norm_text_nfkd_lower(name_nfc)

    if orcid:
        orcid_clean = orcid.replace("https://orcid.org/", "").strip()
        cursor.execute("SELECT AutorID FROM Autores WHERE Orcid = ?", (orcid_clean,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("SELECT AutorID FROM Autores WHERE NombreBusqueda = ?", (name_norm,))
        row = cursor.fetchone()
        if row:
            cursor.execute("UPDATE Autores SET Orcid = ? WHERE AutorID = ?", (orcid_clean, row[0]))
            conn.commit()
            return row[0]
        cursor.execute("INSERT INTO Autores (NombreLimpio, NombreBusqueda, Orcid) VALUES (?, ?, ?)",
                       (name_nfc, name_norm, orcid_clean))
        conn.commit()
        return cursor.lastrowid

    cursor.execute("SELECT AutorID FROM Autores WHERE NombreBusqueda = ?", (name_norm,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO Autores (NombreLimpio, NombreBusqueda, Orcid) VALUES (?, ?, ?)",
                   (name_nfc, name_norm, None))
    conn.commit()
    return cursor.lastrowid

# evita duplicar afiliaciones
def get_or_insert_affiliation(conn: sqlite3.Connection, aff_literal: str, sede_id: int) -> int:
    cursor = conn.cursor()
    lit = norm_text_nfc(aff_literal)
    busc = norm_text_nfkd_lower(aff_literal)

    cursor.execute("SELECT AfiliacionID FROM Afiliaciones WHERE AfiliacionBusqueda = ?", (busc,))
    row = cursor.fetchone()
    if row:
        # si existe pero sin SedeID, rellena
        cursor.execute("UPDATE Afiliaciones SET SedeID = COALESCE(SedeID, ?) WHERE AfiliacionID = ?", (sede_id, row[0]))
        conn.commit()
        return row[0]
    cursor.execute("""
        INSERT INTO Afiliaciones (CadenaLiteral, AfiliacionBusqueda, SedeID) VALUES (?, ?, ?)
    """, (lit, busc, sede_id))
    conn.commit()
    return cursor.lastrowid

# si tiene afiliación con UPS, lo marca como TRUE
def update_affiliation_meta(conn: sqlite3.Connection, afiliacion_id: int, is_ups: bool,
                            country_code: Optional[str], country_name: Optional[str]):
    cur = conn.cursor()
    cur.execute("""
        UPDATE Afiliaciones
           SET EsUPS = CASE WHEN ?=1 THEN 1 ELSE EsUPS END,
               CountryCode = COALESCE(?, CountryCode),
               CountryName = COALESCE(?, CountryName)
         WHERE AfiliacionID = ?
    """, (1 if is_ups else 0, country_code, country_name, afiliacion_id))
    conn.commit()

# PARAMETROS DE CONSULTA A API
params = {
    "query.affiliation": UPS_TARGET,  # '"Universidad Politécnica Salesiana"' si quieres modo exacto
    "filter": ",".join([
        "has-affiliation:true",
        f"from-pub-date:{FROM}",
        f"until-pub-date:{UNTIL}",
    ]),
    "rows": ROWS,
    "cursor": "*",
}

# CREACIÓN DE CSV COMPLEMENTARIO PARA DASHBOARD
UPS_CSV = "ups_institucional.csv"

def create_ups_catalog_csv(csv_path: str = UPS_CSV):
    data = [
        (1, "Sede Cuenca", "Ciencias de la Vida", "cuenca;azuay"),
        (2, "Sede Quito", "Ingenierías y Arquitectura", "quito;pichincha"),
        (3, "Sede Guayaquil", "Ciencias Sociales y Humanas", "guayaquil;guayas"),
        (4, "Otra", "No definida", ""),
    ]
    df = pd.DataFrame(data, columns=["SedeID","Sede","AreaAcademica","PalabrasClave"])
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"[CSV] Archivo complementario creado: {csv_path} ({len(df)} filas)")

# INTEGRACION DE CSV A BASE DE DATOS
def integrate_ups_catalog_and_label_affiliations(db_name: str = DB_NAME, csv_path: str = UPS_CSV):
    with sqlite3.connect(db_name) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS Sedes_Areas (
            SedeID INTEGER PRIMARY KEY,
            Sede TEXT NOT NULL,
            AreaAcademica TEXT
        );
        """)

        sedes = pd.read_csv(csv_path)
        sedes.to_sql("Sedes_Areas_tmp", conn, if_exists="replace", index=False)

        # PARA IDENTIFICACIÓN DE SEDES
        conn.execute("""
        UPDATE Sedes_Areas
           SET Sede = (SELECT tmp.Sede FROM Sedes_Areas_tmp AS tmp WHERE tmp.SedeID = Sedes_Areas.SedeID),
               AreaAcademica = (SELECT tmp.AreaAcademica FROM Sedes_Areas_tmp AS tmp WHERE tmp.SedeID = Sedes_Areas.SedeID)
         WHERE SedeID IN (SELECT SedeID FROM Sedes_Areas_tmp);
        """)
        conn.execute("""
        INSERT INTO Sedes_Areas (SedeID, Sede, AreaAcademica)
        SELECT tmp.SedeID, tmp.Sede, tmp.AreaAcademica
          FROM Sedes_Areas_tmp AS tmp
         WHERE NOT EXISTS (SELECT 1 FROM Sedes_Areas s WHERE s.SedeID = tmp.SedeID);
        """)
        conn.execute("DROP TABLE Sedes_Areas_tmp;")
        print("[CSV→SQLite] Catálogo institucional integrado (compat-UPSERT).")

        # Etiquetado por palabras clave
        conn.execute("UPDATE Afiliaciones SET SedeID=4 WHERE SedeID IS NULL;")
        for _, row in sedes.iterrows():
            sede_id = int(row["SedeID"])
            keywords = str(row.get("PalabrasClave", "")).lower().split(";")
            for kw in (k.strip() for k in keywords if k.strip()):
                conn.execute(
                    "UPDATE Afiliaciones SET SedeID=? WHERE lower(AfiliacionBusqueda) LIKE ?;",
                    (sede_id, f"%{kw}%")
                )
        print("[Etiquetado] Afiliaciones actualizadas con SedeID según palabras clave.")

# PROCESO DE LIMPIEZA DE DATOS USANDO PANDAS
def pandas_cleanup_and_flatview(db_name: str = DB_NAME):
    def _norm_text_nfc(s):
        if pd.isna(s) or s is None: return ""
        s = html.unescape(str(s))
        s = unicodedata.normalize("NFC", s)
        return re.sub(r"\s+", " ", s).strip()

    def _standardize_doi(s):
        if pd.isna(s) or s is None: return ""
        s = html.unescape(str(s).strip())
        s = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:\s*)", "", s, flags=re.IGNORECASE)
        s = s.strip().lower()
        return s if s.startswith("10.") else s

    # INTEGRACIÓN DE DATOS LIMPIOS CON DB SQLITE
    with sqlite3.connect(db_name) as conn:
        obras = pd.read_sql_query("SELECT * FROM Obras", conn)
        autores = pd.read_sql_query("SELECT * FROM Autores", conn)
        afili = pd.read_sql_query("SELECT * FROM Afiliaciones", conn)
        oaa = pd.read_sql_query("SELECT * FROM Obra_Autor_Afiliacion", conn)
        try:
            temas = pd.read_sql_query("SELECT * FROM Obra_Tema", conn)
        except Exception:
            temas = pd.DataFrame(columns=["DOI","Tema"])
        sedes_df = pd.read_sql_query("SELECT SedeID, Sede, AreaAcademica FROM Sedes_Areas", conn)

        # Normalización
        obras["DOI"] = obras["DOI"].apply(_standardize_doi)
        for col in ["Titulo","Revista","Editorial"]:
            if col in obras.columns:
                obras[col] = obras[col].apply(_norm_text_nfc)
        for col in ["Anio","Citas","Referencias"]:
            if col in obras.columns:
                obras[col] = pd.to_numeric(obras[col], errors="coerce").astype("Int64")

        autores["NombreLimpio"] = autores["NombreLimpio"].apply(_norm_text_nfc)
        afili["CadenaLiteral"] = afili["CadenaLiteral"].apply(_norm_text_nfc)

        # Protección contra duplicados
        obras = obras.drop_duplicates(subset=["DOI"])
        autores = autores.drop_duplicates(subset=["AutorID"])
        afili = afili.drop_duplicates(subset=["AfiliacionID"])
        oaa = oaa.drop_duplicates(subset=["DOI","AutorID","AfiliacionID"])
        temas = temas.drop_duplicates(subset=["DOI","Tema"])

        # Integridad referencial
        vdoi = set(obras["DOI"].tolist())
        vaut = set(autores["AutorID"].tolist())
        vaf = set(afili["AfiliacionID"].tolist())
        oaa = oaa[oaa["DOI"].isin(vdoi) & oaa["AutorID"].isin(vaut) & oaa["AfiliacionID"].isin(vaf)]
        temas = temas[temas["DOI"].isin(vdoi)]

        # Escribir tablas limpias
        obras.to_sql("Obras_clean", conn, if_exists="replace", index=False)
        autores.to_sql("Autores_clean", conn, if_exists="replace", index=False)
        afili.to_sql("Afiliaciones_clean", conn, if_exists="replace", index=False)
        oaa.to_sql("Obra_Autor_Afiliacion_clean", conn, if_exists="replace", index=False)
        temas.to_sql("Obra_Tema_clean", conn, if_exists="replace", index=False)

        # Vista plana enriquecida
        flat = (
            oaa.merge(autores[["AutorID","NombreLimpio"]], on="AutorID", how="left")
               .merge(afili[["AfiliacionID","CadenaLiteral","SedeID","CountryCode","CountryName","EsUPS"]],
                      on="AfiliacionID", how="left")
               .merge(sedes_df, on="SedeID", how="left")
               .groupby("DOI", as_index=False)
               .agg(
                    Autores=("NombreLimpio", lambda s: "; ".join(sorted(set([x for x in s if isinstance(x, str) and x])))),
                    Afiliaciones=("CadenaLiteral", lambda s: "; ".join(sorted(set([x for x in s if isinstance(x, str) and x])))),
                    Sedes=("Sede", lambda s: "; ".join(sorted(set([x for x in s.dropna().tolist() if isinstance(x, str)])))),
                    Areas=("AreaAcademica", lambda s: "; ".join(sorted(set([x for x in s.dropna().tolist() if isinstance(x, str)])))),
                    Paises=("CountryName", lambda s: "; ".join(sorted(set([x for x in s.dropna().tolist() if isinstance(x, str)])))),
                    PaisesCodigo=("CountryCode", lambda s: "; ".join(sorted(set([x for x in s.dropna().tolist() if isinstance(x, str)])))),
                    UPS_Flag=("EsUPS", lambda s: int(any([int(x)==1 for x in s.dropna().tolist()])))
               )
        )
        cols_obras = ["DOI","Titulo","Anio","Revista","Editorial","Tipo","Citas","Referencias","FechaPublicacion"]
        cols_obras = [c for c in cols_obras if c in obras.columns]
        flat = obras[cols_obras].merge(flat, on="DOI", how="left")

        if not temas.empty:
            temas_g = temas.groupby("DOI", as_index=False).agg(Temas=("Tema", lambda s: "; ".join(sorted(set(s)))))
            flat = flat.merge(temas_g, on="DOI", how="left")
        else:
            flat["Temas"] = pd.NA

        flat.to_sql("Vista_Analisis", conn, if_exists="replace", index=False)

    print("Limpieza completada!")

# PROCESO ETL COMO TAL
if __name__ == "__main__":
    UPS_TARGET_NORM = norm_text_nfkd_lower(UPS_TARGET)

    conn = sqlite3.connect(DB_NAME)
    create_db_schema(conn)

    seen_dois = set()
    ups_processed = 0
    subjects_rows = 0

    print(f"Iniciando recolección de datos de Crossref (Query Estable: {UPS_TARGET})...")
    print(f"Límite práctico: {MAX_WORKS} obras.")
    print(f"corte automático tras {NO_HITS_LIMIT} páginas seguidas sin nuevos hallazgos.")

    run_start = time.strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Runs (StartedAt, Query, CursorInicio, RowsIngested, Notes) VALUES (?, ?, ?, ?, ?)",
                   (run_start, json.dumps({"query.affiliation": UPS_TARGET, "filter": params["filter"]}),
                    params["cursor"], 0,
                    f"MAX_WORKS={MAX_WORKS}; NO_HITS_LIMIT={NO_HITS_LIMIT}; USE_VARIANTS={USE_VARIANTS}; INSERT_SUBJECTS={INSERT_SUBJECTS}"))
    run_id = cursor.lastrowid
    conn.commit()

    page = 0
    no_hits_streak = 0
    prev_cursor_val = None

    while True:
        if ups_processed >= MAX_WORKS:
            print(f"Se alcanzó el límite de {MAX_WORKS} obras UPS. Deteniendo proceso.")
            break

        page += 1
        print(f"Consultando Página {page}")

        try:
            r = get_with_retry(BASE, params=params, max_tries=6)
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}. Deteniendo.")
            break

        try:
            msg = r.json().get("message", {})
        except ValueError:
            print("Respuesta no es JSON válido. Reintentando en la próxima página…")
            time.sleep(1.5)
            continue

        items = msg.get("items", [])
        if not items:
            print(f"Página {page}. No más resultados.")
            break

        page_ups = 0

        for it in items:
            if ups_processed >= MAX_WORKS:
                break

            doi = standardize_doi(it.get("DOI", ""))
            if not doi or doi in seen_dois:
                continue

            cursor.execute("SELECT 1 FROM Obras WHERE DOI = ?", (doi,))
            if cursor.fetchone():
                seen_dois.add(doi)
                continue

            # Análisis de Autores y Afiliaciones
            work_has_ups = False
            temp_authors = {}  # AutorID -> {"aff_ids": set([...]), "seq": "first"/"additional"}

            for au in it.get("author", []) or []:
                name = author_full_name(au)
                if not name:
                    continue

                orcid = (au.get("ORCID") or "").strip() or None
                seq = au.get("sequence") or None
                aff_ids = []

                for aff in au.get("affiliation", []) or []:
                    aff_name = aff.get("name")
                    if not aff_name:
                        continue

                    aff_literal, aff_norm = normalize_aff_name(aff_name)

                    # pertenece a UPS si o no?
                    match_ups = (UPS_TARGET_NORM in aff_norm)
                    if USE_VARIANTS and not match_ups:
                        if any(v in aff_norm for v in UPS_VARIANTS):
                            match_ups = True
                        # if not match_ups and re.search(r"\bups\b", aff_norm): match_ups = True

                    # sede para UPS; 'Otra' (4) si no-UPS
                    sede_id = 4
                    if match_ups:
                        for keyword, s_id in (("cuenca", 1), ("quito", 2), ("guayaquil", 3)):
                            if keyword in aff_norm:
                                sede_id = s_id
                                break

                    afiliacion_id = get_or_insert_affiliation(conn, aff_literal, sede_id)

                    # país estimado por texto
                    cc, cname = guess_country_from_text(aff_literal)
                    # si es UPS y no se identifica país explícito, se asume Ecuador para facilitar filtros
                    if match_ups and not cc:
                        cc, cname = "EC", "Ecuador"
                    update_affiliation_meta(conn, afiliacion_id, is_ups=match_ups, country_code=cc, country_name=cname)

                    if match_ups:
                        work_has_ups = True

                    aff_ids.append(afiliacion_id)

                if aff_ids:
                    autor_id = get_or_insert_author(conn, name, orcid=orcid)
                    if autor_id not in temp_authors:
                        temp_authors[autor_id] = {"aff_ids": set(), "seq": seq}
                    temp_authors[autor_id]["aff_ids"].update(aff_ids)
                    if temp_authors[autor_id]["seq"] != "first" and seq == "first":
                        temp_authors[autor_id]["seq"] = "first"

            # sólo se inserta la obra si existe al menos una afiliación UPS
            if not work_has_ups:
                continue

            # Inserción de Obra
            seen_dois.add(doi)
            ups_processed += 1
            page_ups += 1

            title = norm_text_nfc("; ".join(it.get("title", []) or []))
            year = extract_year_any(it)
            journal = norm_text_nfc("; ".join(it.get("container-title", []) or []))
            fecha_iso = extract_date_iso(it)  # NUEVO

            cursor.execute("""
                INSERT OR IGNORE INTO Obras 
                    (DOI, Titulo, Anio, Revista, Editorial, Tipo, Citas, Referencias, FechaPublicacion) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                doi, title, year, journal, norm_text_nfc(it.get("publisher", "")),
                it.get("type", ""), it.get("is-referenced-by-count", 0), it.get("reference-count", 0),
                fecha_iso
            ))

            # Temas (Crossref 'subject')
            subj_list = it.get("subject") or []
            if subj_list:
                print(f"[subjects] DOI={doi} -> {len(subj_list)} materias")

            if INSERT_SUBJECTS:
                for t in (it.get("subject") or []):
                    tema = norm_text_nfc(t)
                    if tema:
                        cursor.execute("""
                            INSERT OR IGNORE INTO Obra_Tema (DOI, Tema) VALUES (?, ?)
                        """, (doi, tema))
                        subjects_rows += 1

            # Relación Obra-Autor-Afiliación (todas las afiliaciones)
            for autor_id, data in temp_authors.items():
                seq_val = data.get("seq")
                for aff_id in data["aff_ids"]:
                    cursor.execute("""
                        INSERT OR IGNORE INTO Obra_Autor_Afiliacion (DOI, AutorID, AfiliacionID, AutorSecuencia) 
                        VALUES (?, ?, ?, ?)
                    """, (doi, autor_id, aff_id, seq_val))

        conn.commit()

        if page_ups == 0:
            no_hits_streak += 1
        else:
            no_hits_streak = 0

        print(f"Página {page} procesada. UPS en esta página: {page_ups}. "
              f"UPS acumuladas: {ups_processed}. Racha sin hallazgos: {no_hits_streak}")

        if ups_processed >= MAX_WORKS:
            print(f"Se alcanzó el límite de {MAX_WORKS} obras UPS. Deteniendo proceso.")
            break

        next_cursor = msg.get("next-cursor")
        if not next_cursor or next_cursor == prev_cursor_val:
            print("No hay cursor siguiente (o no avanza). Fin de resultados.")
            break
        prev_cursor_val = next_cursor
        params["cursor"] = next_cursor

        if no_hits_streak >= NO_HITS_LIMIT:
            print(f"Sin hallazgos en {NO_HITS_LIMIT} páginas seguidas. Cortando por temas de eficiencia.")
            break

        time.sleep(0.3)

    run_end = time.strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("UPDATE Runs SET EndedAt = ?, CursorFin = ?, RowsIngested = ? WHERE RunID = ?",
                   (run_end, params.get("cursor"), ups_processed, run_id))
    conn.commit()
    conn.close()

    print("\nPROCESO ETL FINALIZADO!")
    print(f"Base de datos '{DB_NAME}' creada y poblada con {ups_processed} obras UPS (afiliación por autor a '{UPS_TARGET}').")
    # print(f"Materias (subjects) insertadas en esta corrida: {subjects_rows}")

    # CSV institucional, integración, limpieza y vista plana
    create_ups_catalog_csv(UPS_CSV)
    integrate_ups_catalog_and_label_affiliations(DB_NAME, UPS_CSV)
    pandas_cleanup_and_flatview(DB_NAME)

    print("\nResultado esperado: ETL → CSV institucional → integración → limpieza pandas → Vista_Analisis lista para Superset.")