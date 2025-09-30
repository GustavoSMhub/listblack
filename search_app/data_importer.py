# Ruta: search_app/data_importer.py
# Nombre del archivo: data_importer.py
from __future__ import annotations

import os
import io
import csv
import re
import sys
import time
import zipfile
import hashlib
import threading # Necesario para el bloqueo
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin
from typing import List, Tuple, Any, Optional, Iterable

import requests
from requests.adapters import HTTPAdapter, Retry
import pymysql.cursors
import pymysql.err # Importar para capturar el error de conexión
from dotenv import load_dotenv
from bs4 import BeautifulSoup as BS 

# Cargar .env para asegurar que las credenciales estén disponibles
load_dotenv() 

# --- CONFIGURACIÓN GLOBAL Y PARCHES ---
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Bloqueo: Asegura que solo una función de actualización (EFOS u OFAC) se ejecute a la vez
UPDATE_LOCK = threading.Lock()

# Directorio de datos: Un nivel arriba de search_app/ (donde están tus scripts CLI)
DATA_DIR_ROOT = Path(os.path.dirname(os.path.abspath(__file__))).parent / "data"
DATA_DIR_EFOS = DATA_DIR_ROOT / "efos"
DATA_DIR_OFAC = DATA_DIR_ROOT / "ofac"

# --- UTILIDADES GENERALES ---

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

# --- CONEXIÓN A MySQL ---

def get_mysql_conn():
    """Establece conexión a MySQL usando las variables de .env."""
    host = os.getenv('MYSQL_HOST')
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    db = os.getenv('MYSQL_DATABASE')
    
    if not password or not db or not host or not user:
        raise ValueError("FATAL: Credenciales de BD no encontradas. Asegúrese de que .env está configurado.")

    try:
        return pymysql.connect(
            host=host, user=user, password=password, database=db, 
            charset='utf8mb4', cursorclass=pymysql.cursors.Cursor, autocommit=False
        )
    except pymysql.err.OperationalError as e:
        # Captura el error si MySQL está apagado (Error 2003: Can't connect to MySQL server)
        if e.args[0] == 2003:
            raise ConnectionError("El servidor MySQL está apagado o inaccesible. Por favor, enciéndalo (Ubuntu 14.04).")
        raise # Relanza cualquier otro error operacional

# --- CONSULTA DE ESTADO ---
def get_last_update_time_efos() -> Optional[datetime]:
    """Obtiene la fecha y hora de la última actualización de EFOS."""
    conn = None
    try:
        conn = get_mysql_conn()
        with conn.cursor() as cur:
            # Intentar contar los registros. Si la tabla no existe, fallará ProgrammingError.
            cur.execute("SELECT COUNT(*) FROM EFOS_TAXPAYER")
            if cur.fetchone()[0] == 0:
                 return None

            # Obtiene la fecha más reciente de LAST_UPDATED
            cur.execute("SELECT LAST_UPDATED FROM EFOS_TAXPAYER ORDER BY LAST_UPDATED DESC LIMIT 1")
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            return None
    except pymysql.err.ProgrammingError:
        # Esto ocurre si la tabla EFOS_TAXPAYER no existe todavía o hay un error en la consulta
        return None
    except ConnectionError:
        # Si la conexión falla, no podemos obtener la hora
        return None
    finally:
        if conn:
            conn.close()

def get_last_update_time_ofac() -> Optional[datetime]:
    """Obtiene la fecha y hora de la última actualización de OFAC (usando la tabla de perfiles)."""
    conn = None
    try:
        conn = get_mysql_conn()
        with conn.cursor() as cur:
            # Usamos OFAC_PARTY_PROFILE para la fecha
            cur.execute("SELECT COUNT(*) FROM OFAC_PARTY_PROFILE")
            if cur.fetchone()[0] == 0:
                 return None
                 
            cur.execute("SELECT LAST_UPDATED FROM OFAC_PARTY_PROFILE ORDER BY LAST_UPDATED DESC LIMIT 1")
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            return None
    except pymysql.err.ProgrammingError:
        # Esto ocurre si la tabla OFAC_PARTY_PROFILE no existe todavía
        return None
    except ConnectionError:
        return None
    finally:
        if conn:
            conn.close()


# ----------------------------------------------------------------------
# LÓGICA DE EFOS (SAT) - Código Completo de import_efos_mysql.py
# ----------------------------------------------------------------------

# Constantes de EFOS (Ajustadas para la ubicación modular)
EFOS_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) LNofac_efos_ppe/1.0"
EFOS_BASE_CANDIDATES = [
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.csv",
    "https://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.zip",
]
RETRY_TOTAL     = _env_int("SAT_RETRIES", 1)
CONNECT_TIMEOUT = _env_int("SAT_CONNECT_TIMEOUT", 5)
READ_TIMEOUT    = _env_int("SAT_READ_TIMEOUT", 15)
TOTAL_DEADLINE  = _env_int("SAT_TOTAL_DEADLINE", 60)
SPANISH_MONTHS = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}


# --- Utilidades de Descarga EFOS ---

def mk_session() -> requests.Session:
    """Crea una sesión de requests con headers y adaptador de reintentos."""
    s = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL, connect=RETRY_TOTAL, read=RETRY_TOTAL,
        backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","HEAD"])
    )
    s.headers.update({"User-Agent": EFOS_USER_AGENT})
    adapter = HTTPAdapter(max_retries=retries)
    s.mount('https://', adapter) 
    s.mount("http://", adapter)
    return s

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _try_download_efos(session: requests.Session, url: str) -> tuple[bytes | None, str]:
    t0 = time.monotonic()
    try:
        verify_ssl = not ("sat.gob.mx" in url or "datos.gob.mx" in url)
        r = session.get(url, 
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), 
                        allow_redirects=True, 
                        verify=verify_ssl)
        r.raise_for_status()
        return r.content, url
    except Exception:
        return None, url

def _decode_csv_bytes_with_header(data: bytes) -> tuple[str, str]:
    try: txt = data.decode("utf-8-sig")
    except UnicodeDecodeError: txt = data.decode("latin-1")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    encabezado = txt[:30_000] 
    lines = txt.split("\n")

    header_idx = None
    for i, line in enumerate(lines[:200]):
        l = line.lower()
        if "rfc" in l and ("nombre" in l or "razon" in l or "razón" in l):
            header_idx = i
            break
            
    if header_idx is None: header_idx = 3 
        
    cleaned = "\n".join(lines[header_idx:])
    return cleaned, encabezado 

def _csv_buffer_from_zip(blob: bytes) -> tuple[io.StringIO, str]:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not name: raise RuntimeError("ZIP sin CSV")
        inner = zf.read(name)
        cleaned, encabezado = _decode_csv_bytes_with_header(inner)
        return io.StringIO(cleaned), encabezado 

def _mk_from_bytes(raw: bytes, src: str):
    cleaned, encabezado = _decode_csv_bytes_with_header(raw)
    vdate = _parse_global_header_date(encabezado)
    return io.StringIO(cleaned), sha256_bytes(raw), src, vdate

def descargar_csv_o_zip_efos(session: requests.Session) -> tuple[io.StringIO | bytes, str, str, date | None]:
    start = time.monotonic()
    def deadline_ok() -> bool: return (time.monotonic() - start) < TOTAL_DEADLINE

    urls = EFOS_BASE_CANDIDATES

    for url in urls:
        if not deadline_ok(): break
        blob, u = _try_download_efos(session, url)
        if not blob: continue
        
        if url.lower().endswith(".zip") or blob[:4] == b"PK\x03\x04":
            return blob, sha256_bytes(blob), u, date.today() 
        
        if b"RFC" in blob[:1024].upper():
             return _mk_from_bytes(blob, u)
        
    raise ConnectionError("No se pudo obtener el 69-B. (Revisa tus URLs y la conexión SSL/SAT)")

# --- Normalización y Parseo EFOS ---

def _parse_global_header_date(txt: str | bytes | list) -> date | None:
    if not isinstance(txt, str): return None 
    t = txt.lower().replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ", "n")
    m = re.search(r"actualiz\w+\s+al\s+(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})", t)
    if not m: return None
    day, month_str, year = m.groups()
    month = SPANISH_MONTHS.get(month_str, None)
    if month is None: return None
    try: return date(int(year), month, int(day))
    except (ValueError, TypeError): return None
    
def _parse_date_mx(s: Any) -> date | None:
    if not isinstance(s, str) or not s: return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%Y/%m/%d"):
        try: return datetime.strptime(s.split()[0], fmt).date()
        except Exception: pass
    return None

def normaliza_estatus(raw: Any) -> str:
    if isinstance(raw, (list, tuple)): raw = raw[0] if raw else ""
    elif raw is None: raw = ""
    try: t = str(raw).strip().lower()
    except Exception: t = "" 

    if "sentenc" in t: return "sentencia_favorable"
    if "desvirt" in t: return "desvirtuado"
    if "definitivo" in t: return "definitivo"
    if "presunto" in t: return "presunto"
    return "presunto"

def parse_csv_rows_efos(buf: io.StringIO) -> list[dict]:
    try:
        sample = buf.read(1024); buf.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters='|,\t') 
        reader = csv.reader(buf, dialect=dialect, quotechar='"')
    except csv.Error:
        buf.seek(0); reader = csv.reader(buf, delimiter=',', quotechar='"')
    except Exception: return []
    
    filas = []; headers_row = next(reader)
        
    IDX_RFC = 1; IDX_NOMBRE = 2; IDX_ESTATUS = 3; IDX_FECHA_DOF = 15
    if len(headers_row) <= IDX_FECHA_DOF: IDX_FECHA_DOF = -1 
         
    for row in reader:
        if not row: continue
        
        rfc_raw = row[IDX_RFC] if len(row) > IDX_RFC else ""; rfc = re.sub(r"[^A-Z0-9]", "", str(rfc_raw).upper().strip()) 
        if not rfc: continue
        
        nombre = str(row[IDX_NOMBRE]).strip() or None
        estatus_raw = row[IDX_ESTATUS] if len(row) > IDX_ESTATUS else ""; estatus = normaliza_estatus(estatus_raw)
        
        fecha_dof = None
        if IDX_FECHA_DOF != -1 and len(row) > IDX_FECHA_DOF:
            fecha_dof_raw = row[IDX_FECHA_DOF]; fecha_str = str(fecha_dof_raw).split(" - ")[0].split(" // ")[0].strip()
            fecha_dof = _parse_date_mx(fecha_str)

        filas.append(dict(
            rfc=rfc, fuente="69-B", tipo_evento=estatus, fecha_dof=fecha_dof, nombre=nombre, tipo_persona=None,
        ))
    return filas

# --- BD MySQL EFOS ---

def ensure_tables_efos(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EFOS_TAXPAYER ( 
                id BIGINT AUTO_INCREMENT PRIMARY KEY, 
                RFC VARCHAR(15) NOT NULL, 
                FULL_NAME TEXT, 
                ENTITY_TYPE VARCHAR(10), 
                SOURCE_NAME VARCHAR(10) NOT NULL DEFAULT '69-B', 
                CURRENT_STATUS VARCHAR(30), 
                LAST_DOF_DATE DATE, 
                LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
                UNIQUE KEY uk_rfc_source (RFC, SOURCE_NAME)
            );
        """)
        conn.commit()

def upsert_taxpayer(conn, filas: list[dict], corte: date) -> int:
    ensure_tables_efos(conn)
    data_to_load = []; latest_status = {}
    
    for f in filas:
        rfc = f['rfc']; current_dof = f.get('fecha_dof') or date.min
        stored_dof = latest_status[rfc].get('fecha_dof') or date.min if rfc in latest_status else date.min
        
        if rfc not in latest_status or current_dof > stored_dof:
            latest_status[rfc] = f
            
    for rfc, data in latest_status.items():
        data_to_load.append((data['rfc'], data['nombre'], data['tipo_persona'], data['fuente'], data['tipo_evento'], data['fecha_dof'], corte))
        
    UPSERT_SQL = """
        INSERT INTO EFOS_TAXPAYER 
        (RFC, FULL_NAME, ENTITY_TYPE, SOURCE_NAME, CURRENT_STATUS, LAST_DOF_DATE, LAST_UPDATED)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            FULL_NAME = VALUES(FULL_NAME), ENTITY_TYPE = VALUES(ENTITY_TYPE), CURRENT_STATUS = VALUES(CURRENT_STATUS), LAST_DOF_DATE = VALUES(LAST_DOF_DATE), LAST_UPDATED = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, data_to_load) 
    conn.commit()
    return len(data_to_load)

# --- FUNCIÓN PRINCIPAL DE EJECUCIÓN DE EFOS ---

def run_efos_update() -> int:
    """Función principal para ser llamada por el endpoint de Flask."""
    if not UPDATE_LOCK.acquire(blocking=False):
        raise RuntimeError("El proceso de actualización ya está en curso.")

    conn = None
    try:
        os.makedirs(DATA_DIR_EFOS, exist_ok=True)
        session = mk_session()
        buf_or_bytes, sha, src_url, vdate = descargar_csv_o_zip_efos(session)
        
        # 1. Descomprimir/Parsear
        if not isinstance(buf_or_bytes, io.StringIO):
             buf_or_bytes, encabezado = _csv_buffer_from_zip(buf_or_bytes)
             vdate = _parse_global_header_date(encabezado)

        filas = parse_csv_rows_efos(buf_or_bytes)
        
        if not filas:
            raise Exception(f"No se encontraron filas válidas en el archivo descargado de SAT ({src_url}).")
            
        # 2. Cargar a BD
        conn = get_mysql_conn()
        corte = vdate or date.today()
        
        total_loaded = upsert_taxpayer(conn, filas, corte)
        
        return total_loaded

    finally:
        if conn:
            conn.close()
        UPDATE_LOCK.release()

# ----------------------------------------------------------------------
# LÓGICA DE OFAC (TESORO DE EE. UU.) - Código Completo de import_ofac_mysql.py
# ----------------------------------------------------------------------

# Constantes de OFAC
OFAC_USER_AGENT = "busquedaLN/1.0 (+local)"
CONNECT_TIMEOUT = 5.0; READ_TIMEOUT = 10.0; TOTAL_DEADLINE = 45.0; CHUNK_SIZE = 64 * 1024
OFAC_URLS = {
    "sdn.csv": ["https://www.treasury.gov/ofac/downloads/sdn.csv", "https://home.treasury.gov/ofac/downloads/sdn.csv"],
    "alt.csv": ["https://www.treasury.gov/ofac/downloads/alt.csv", "https://home.treasury.gov/ofac/downloads/alt.csv"],
}
REF_CACHE = {} 
WEAK_AKA_REGEX = re.compile(r'(?:a\.k\.a\.|\sf\.k\.a\.|\sn\.k\.a\.)\s*[\'"]?([^\'"()\.;]+)[\'"]?', re.IGNORECASE)

# --- Descarga OFAC ---

def _http_get_first_ok_ofac(urls: Iterable[str]) -> bytes | None:
    sess = requests.Session(); sess.headers.update({"User-Agent": OFAC_USER_AGENT})
    for url in urls:
        start = time.monotonic()
        try:
            with sess.get(url, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True) as r:
                r.raise_for_status()
                buf = bytearray()
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk: buf.extend(chunk)
                    if (time.monotonic() - start) > TOTAL_DEADLINE: raise TimeoutError(f"deadline {TOTAL_DEADLINE:.0f}s excedido")
                return bytes(buf)
        except KeyboardInterrupt: raise
        except Exception: time.sleep(0.2); continue
    return None

def download_ofac_files(target_dir: Path) -> Tuple[Path, Optional[Path]]:
    """Descarga sdn.csv y alt.csv."""
    target_dir.mkdir(parents=True, exist_ok=True)
    sdn_path, alt_path = target_dir / "sdn.csv", target_dir / "alt.csv"
    
    content_sdn = _http_get_first_ok_ofac(OFAC_URLS["sdn.csv"])
    if not content_sdn:
        raise Exception("Fallo la descarga de sdn.csv.")
        
    sdn_path.write_bytes(content_sdn)
    
    content_alt = _http_get_first_ok_ofac(OFAC_URLS["alt.csv"])
    if content_alt:
        alt_path.write_bytes(content_alt)
        return sdn_path, alt_path
    
    return sdn_path, None

# --- Utilidades de Mapeo y RegEx OFAC ---

def load_reference_cache(conn):
    """Carga los IDs de las tablas de referencia una vez al inicio."""
    global REF_CACHE
    REF_CACHE['party_type'] = {}; REF_CACHE['alias_type'] = {}; REF_CACHE['program'] = {}
    with conn.cursor() as cur:
        # Nota: Estas tablas deben existir en tu base de datos
        # Implementación de OFAC completa
        try:
            cur.execute("SELECT ID, VALUE FROM OFAC_REF_PARTY_TYPE")
            for id, value in cur.fetchall(): REF_CACHE['party_type'][value.upper()] = id
            cur.execute("SELECT ID, CODE FROM OFAC_REF_ALIAS_TYPE")
            for id, code in cur.fetchall(): REF_CACHE['alias_type'][code.upper()] = id
            cur.execute("SELECT ID, CODE FROM OFAC_REF_SANCTION_PROG")
            for id, code in cur.fetchall(): REF_CACHE['program'][code.upper()] = id
        except pymysql.err.ProgrammingError as e:
            # Captura si faltan las tablas de referencia
            raise RuntimeError(f"Faltan tablas de referencia de OFAC (ej. OFAC_REF_PARTY_TYPE). Error: {e}")

def get_id(ref_type: str, value: str) -> int:
    if not value: return 999
    return REF_CACHE.get(ref_type, {}).get(value.upper().strip(), 999)

def extract_weak_akas(ent_num: int, remarks: str) -> List[Tuple]:
    if not remarks: return []
    weak_akas = []; weak_id = get_id('alias_type', 'WEAK')
    for match in WEAK_AKA_REGEX.finditer(remarks):
        alias = match.group(1).strip()
        if alias and alias != '-0-':
            weak_akas.append((0 - ent_num, ent_num, alias, weak_id, 0, 'weak'))
    return weak_akas

# --- Funciones de Parsing OFAC ---

def _read_text(path: Path) -> str:
    b = path.read_bytes().replace(b'\x1a', b'')
    return b.decode("latin-1", errors="ignore")

def parse_sdn(text: str) -> Tuple[List, List, List]:
    profiles, primary_names, weak_aliases = [], [], []
    rd = csv.reader(io.StringIO(text)); _ = next(rd, None) 
    for r in rd:
        if not r or r[0].strip() == '-0-': continue
        
        ent = int((r[0] or "0").strip()); name = (r[1] or "").strip() or None
        sdn_type = (r[2] or "").strip() or None; program_code = (r[3] or "").strip() or None
        remarks = (r[11] if len(r) > 11 else "").replace('-0-', '').strip() or None
        
        profiles.append((ent, get_id('party_type', sdn_type), get_id('program', program_code), name, remarks))
        if name: primary_names.append((ent, ent, name, get_id('alias_type', 'AKA'), 1, 'strong'))
        weak_aliases.extend(extract_weak_akas(ent, remarks))
        
    return profiles, primary_names, weak_aliases

def parse_alt(text: str) -> List[Tuple]:
    out = []; rd = csv.reader(io.StringIO(text)); _ = next(rd, None)
    for r in rd:
        if not r or r[0].strip() == '-0-': continue
        try:
            ent = int((r[0] or "0").strip()); alt_num = int((r[1] or "0").strip())
        except Exception: continue
        
        alt_type_code = (r[2] or "").strip().replace('-0-', '') or None
        alias = (r[3] or "").strip().replace('-0-', '')
        if alias: out.append((alt_num, ent, alias, get_id('alias_type', alt_type_code), 0, 'strong'))
            
    return out

# --- Lógica de Carga y Switch (Full Load con Tablas Temporales) OFAC ---

def truncate_and_load_ofac(conn, sdn_data: Tuple, alt_rows: List = None) -> int:
    profiles, primary_names, weak_aliases = sdn_data
    all_names_to_load = primary_names + (alt_rows or []) + weak_aliases
    
    TEMP_SUFFIX = "_NEW"
    # SQL IDÉNTICO a tu script
    PROFILE_INSERT_SQL = f"""
        INSERT INTO OFAC_PARTY_PROFILE{TEMP_SUFFIX} (ID, ENTITY_TYPE_ID, PROGRAM_ID, FULL_NAME, REMARKS, LAST_UPDATED) 
        VALUES (%s, %s, %s, %s, %s, NOW()) 
        ON DUPLICATE KEY UPDATE 
            ENTITY_TYPE_ID=VALUES(ENTITY_TYPE_ID), PROGRAM_ID=VALUES(PROGRAM_ID), 
            FULL_NAME=VALUES(FULL_NAME), REMARKS=VALUES(REMARKS), LAST_UPDATED=NOW();
    """
    NAME_INSERT_SQL = f"""
        INSERT INTO OFAC_NAME{TEMP_SUFFIX} (ID, PROFILE_ID, FULL_NAME, NAME_TYPE_ID, IS_PRIMARY, STRENGTH) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
            FULL_NAME=VALUES(FULL_NAME), NAME_TYPE_ID=VALUES(NAME_TYPE_ID), 
            IS_PRIMARY=VALUES(IS_PRIMARY), STRENGTH=VALUES(STRENGTH);
    """
    
    with conn.cursor() as cur:
        # 1. Creación de Tablas Temporales 
        # CORRECCIÓN: Usar f-strings para inyectar TEMP_SUFFIX
        cur.execute(f"DROP TABLE IF EXISTS OFAC_PARTY_PROFILE{TEMP_SUFFIX};")
        cur.execute(f"CREATE TABLE OFAC_PARTY_PROFILE{TEMP_SUFFIX} LIKE OFAC_PARTY_PROFILE;")
        cur.execute(f"DROP TABLE IF EXISTS OFAC_NAME{TEMP_SUFFIX};") 
        cur.execute(f"CREATE TABLE OFAC_NAME{TEMP_SUFFIX} LIKE OFAC_NAME;")
        
        # 2. Carga de datos
        cur.executemany(PROFILE_INSERT_SQL, profiles)
        cur.executemany(NAME_INSERT_SQL, all_names_to_load)
        
        # 3. RENAME RÁPIDO (Switch atómico)
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;") 
        # CORRECCIÓN: Usar f-strings para inyectar _OLD y _NEW
        cur.execute(f"DROP TABLE IF EXISTS OFAC_PARTY_PROFILE_OLD, OFAC_NAME_OLD;") 
        rename_sql = f"""
        RENAME TABLE 
            OFAC_PARTY_PROFILE TO OFAC_PARTY_PROFILE_OLD,
            OFAC_PARTY_PROFILE{TEMP_SUFFIX} TO OFAC_PARTY_PROFILE,
            OFAC_NAME TO OFAC_NAME_OLD,
            OFAC_NAME{TEMP_SUFFIX} TO OFAC_NAME;
        """
        cur.execute(rename_sql) 
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
    conn.commit()
    return len(profiles)

# --- FUNCIÓN PRINCIPAL DE EJECUCIÓN DE OFAC ---

def run_ofac_update() -> int:
    """Función principal para ser llamada por el endpoint de Flask."""
    if not UPDATE_LOCK.acquire(blocking=False):
        raise RuntimeError("El proceso de actualización ya está en curso.")

    conn = None
    try:
        os.makedirs(DATA_DIR_OFAC, exist_ok=True)
        conn = get_mysql_conn()
        
        # 1. Cargar caché de referencias (Necesario para el parseo)
        load_reference_cache(conn)

        # 2. DESCARGA ESTABLE
        sdn_path, alt_path = download_ofac_files(DATA_DIR_OFAC)
        
        # 3. Lectura y Parseo
        sdn_data = parse_sdn(_read_text(sdn_path))
        alt_rows = parse_alt(_read_text(alt_path)) if alt_path else None
        
        if not sdn_data[0]:
            raise Exception("No se encontraron perfiles de OFAC en el archivo SDN.")

        # 4. Ejecución de la Carga en MySQL (Doble Buffer)
        total_loaded = truncate_and_load_ofac(conn, sdn_data, alt_rows)
        
        return total_loaded
        
    finally:
        if conn:
            conn.close()
        UPDATE_LOCK.release()
