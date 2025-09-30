# Archivo: import_efos_mysql.py (Versión FINAL Y ROBUSTA con csv.reader)
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import re
import sys
import time
import zipfile
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin
from typing import List, Tuple, Any, Optional

# --- BIBLIOTECAS ---
import pymysql.cursors
from dotenv import load_dotenv
import requests
import requests.adapters
import ssl
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup as BS 

# <<< PARCHE FINAL PARA PROBLEMAS SSL >>>
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# --------------------------------------

# ───────────────────────────────────────────
# Configuración y Constantes
# ───────────────────────────────────────────
load_dotenv() 

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) LNofac_efos_ppe/1.0"
DATOS_ABIERTOS_HTML = "https://wwwmat.sat.gob.mx/consultas/76674/consulta-la-relacion-de-contribuyentes-con-operaciones-presuntamente-inexistentes"

BASE_CANDIDATES = [
    # <<< ESTA ES LA URL DE TU PROYECTO EXITOSO (HTTP, CSV) >>>
    "http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.csv",
    
    # El resto de las URLs que causaban problemas:
    "https://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.zip",
    "http://omawww.sat.gob.mx/cifras_sat/Paginas/datos/vinculo.html?page=ListCompleta69B.html",
]

TEMP_DIR = Path(os.getenv("TEMP_DIR", "/tmp/efos"))

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

RETRY_TOTAL     = _env_int("SAT_RETRIES", 1)
CONNECT_TIMEOUT = _env_int("SAT_CONNECT_TIMEOUT", 5)
READ_TIMEOUT    = _env_int("SAT_READ_TIMEOUT", 15)
TOTAL_DEADLINE  = _env_int("SAT_TOTAL_DEADLINE", 60)

# ───────────────────────────────────────────
# Utilidades de Conexión y Descarga
# ───────────────────────────────────────────

def mk_session() -> requests.Session:
    """Crea una sesión de requests con headers y adaptador de reintentos (sin conflicto SSL)."""
    s = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL, connect=RETRY_TOTAL, read=RETRY_TOTAL,
        backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET","HEAD"])
    )
    s.headers.update({"User-Agent": USER_AGENT})
    adapter = HTTPAdapter(max_retries=retries)
    s.mount('https://', adapter) 
    s.mount("http://", adapter)
    return s

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _try_download(session: requests.Session, url: str) -> tuple[bytes | None, str]:
    t0 = time.monotonic()
    try:
        print(f"[EFOS] GET {url}")
        
        # Desactivamos la verificación SSL para el SAT (La solución que tu entorno acepta)
        verify_ssl = not ("sat.gob.mx" in url or "datos.gob.mx" in url)
        
        r = session.get(url, 
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), 
                        allow_redirects=True, 
                        verify=verify_ssl)
        r.raise_for_status()
        dt = time.monotonic() - t0
        print(f"[EFOS]   OK {len(r.content) / 1024:.2f} KB en {dt:.1f}s")
        return r.content, url
    except Exception as e:
        dt = time.monotonic() - t0
        print(f"[EFOS]   fallo en {dt:.1f}s: {e.__class__.__name__} - {e}")
        return None, url

def _csv_buffer_from_zip(blob: bytes) -> tuple[io.StringIO, str]:
    """Descomprime un ZIP y devuelve el buffer CSV."""
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not name: raise RuntimeError("ZIP sin CSV")
        inner = zf.read(name)
        cleaned, encabezado = _decode_csv_bytes_with_header(inner)
        return io.StringIO(cleaned), encabezado 

def _decode_csv_bytes_with_header(data: bytes) -> tuple[str, str]:
    """Decodifica (UTF-8/Latin-1) y corta prefacios hasta encabezado real."""
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
            
    # LÓGICA CLAVE: Si la detección falla, forzamos la línea 4.
    if header_idx is None:
        print("[EFOS] ⚠️ Advertencia: No se detectó el encabezado. Forzando inicio en LÍNEA 4.")
        header_idx = 3 
        
    # DIAGNÓSTICO: Imprimimos el encabezado detectado/forzado
    if header_idx is not None:
        print(f"[EFOS]   HEADER USADO (Línea {header_idx+1}): {lines[header_idx].strip()}")


    cleaned = "\n".join(lines[header_idx:])
    return cleaned, encabezado 

def _mk_from_bytes(raw: bytes, src: str):
    """Convierte bytes a buffer CSV y extrae la fecha global."""
    cleaned, encabezado = _decode_csv_bytes_with_header(raw)
    vdate = _parse_global_header_date(encabezado)
    return io.StringIO(cleaned), sha256_bytes(raw), src, vdate

def _discover_from_html(session: requests.Session) -> list[str]:
    print(f"[EFOS] Buscando URLs en HTML de: {DATOS_ABIERTOS_HTML}")
    blob, _ = _try_download(session, DATOS_ABIERTOS_HTML)
    if not blob: return []
    try: soup = BS(blob.decode("utf-8", "replace"), 'html.parser')
    except Exception: print("[EFOS] No se pudo parsear el HTML con BS."); return []
    links = []
    for a in soup.find_all('a', href=True):
        href = a.get('href')
        if re.search(r'\.(csv|zip)$', href, re.IGNORECASE) and re.search(r'69-?B', href, re.I):
            links.append(urljoin(DATOS_ABIERTOS_HTML, href))
    return sorted(set(links), key=lambda u: (u.startswith("https://"), u))

def descargar_csv_o_zip(session: requests.Session,
                        prefer_local_csv: str | None,
                        prefer_local_zip: str | None,
                        fast: bool = False) -> tuple[io.StringIO | bytes, str, str, date | None]:
    start = time.monotonic()
    def deadline_ok() -> bool: return (time.monotonic() - start) < TOTAL_DEADLINE

    if prefer_local_csv and os.path.isfile(prefer_local_csv):
        raw = open(prefer_local_csv, "rb").read()
        return _mk_from_bytes(raw, prefer_local_csv)
    if prefer_local_zip and os.path.isfile(prefer_local_zip):
        raw = open(prefer_local_zip, "rb").read()
        return raw, sha256_bytes(raw), prefer_local_zip, date.today() # Devuelve bytes del ZIP

    urls = []
    # Note: La búsqueda en HTML está comentada en la práctica, forzando los candidatos base.
    # if deadline_ok(): urls.extend(_discover_from_html(session)) 
    urls.extend(BASE_CANDIDATES)

    for url in urls:
        if not deadline_ok(): break
        blob, u = _try_download(session, url)
        if not blob: continue
        
        if url.lower().endswith(".zip") or blob[:4] == b"PK\x03\x04":
            return blob, sha256_bytes(blob), u, date.today() # Devuelve bytes del ZIP
        
        if b"RFC" in blob[:1024].upper():
             return _mk_from_bytes(blob, u)
        
    raise ConnectionError("No se pudo obtener el 69-B. (Revisa tus URLs y la conexión SSL/SAT)")

# ───────────────────────────────────────────
# Normalización y parseo de filas
# ───────────────────────────────────────────
SPANISH_MONTHS = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}

# Función auxiliar de robustez para todas las cadenas de texto (Canon)
def canon(s: Any) -> str:
    """Convierte a minúsculas, quita acentos, y asegura que la entrada sea str."""
    # BLINDAJE FINAL: Forzamos a que 's' sea una cadena simple.
    if isinstance(s, (list, tuple)) and s:
        s = s[0]
    elif s is None:
        s = ""

    if not isinstance(s, str):
        try:
            s = str(s)
        except Exception:
            return "" 
            
    # Esta línea ahora es segura, ya que 's' es str
    return s.lower().strip().replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")


def _parse_global_header_date(txt: str | bytes | list) -> date | None:
    # Corregido: Asegura que el input sea una cadena de texto
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
    # Corregido: Verificar si la entrada es una cadena de texto antes de cualquier manipulación
    if not isinstance(s, str) or not s: return None
        
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%Y/%m/%d"):
        try: return datetime.strptime(s.split()[0], fmt).date()
        except Exception: pass
    return None

def normaliza_estatus(raw: Any) -> str:
    """Normaliza el estatus y blinda contra entradas tipo lista."""
    # BLINDAJE FINAL: Forzamos la conversión a str DE FORMA AGRESIVA.
    if isinstance(raw, (list, tuple)):
        raw = raw[0] if raw else ""
    elif raw is None:
        raw = ""
        
    try:
        t = str(raw).strip().lower() # <--- Blindado: str() y strip()
    except Exception:
        t = "" 

    if "sentenc" in t:     return "sentencia_favorable"
    if "desvirt" in t:     return "desvirtuado"
    if "definitivo" in t:  return "definitivo"
    if "presunto" in t:    return "presunto"
    return "presunto"

# ───────────────────────────────────────────
# FUNCIÓN CRÍTICA CON csv.reader
# ───────────────────────────────────────────
def parse_csv_rows(buf: io.StringIO) -> list[dict]:
    """
    Parsea filas usando csv.reader (acceso por índice) para evitar errores 
    con DictReader en entornos problemáticos.
    """
    try:
        # 1. Intento con Sniffer
        sample = buf.read(1024)
        buf.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters='|,\t') 
        reader = csv.reader(buf, dialect=dialect, quotechar='"') # <--- USAMOS csv.reader
        print(f"[EFOS]   Dialecto detectado: '{dialect.delimiter}'")

    except csv.Error:
        # 2. Fallback forzando delimitador de coma
        buf.seek(0)
        print("[EFOS]   Sniffer falló. Forzando delimitador ','...")
        reader = csv.reader(buf, delimiter=',', quotechar='"') # <--- USAMOS csv.reader
    
    except Exception as e:
        print(f"[EFOS]   Fallo al inicializar el lector CSV: {e}")
        return []
    
    filas = []
    
    # 1. Obtener los encabezados (headers_row es una lista de strings)
    try:
        headers_row = next(reader)
    except StopIteration:
        print("[EFOS] El archivo CSV está vacío o solo tiene prefacio.")
        return []
        
    # Definimos los índices Fijos basados en el 'HEADER USADO' del log:
    # No,RFC,Nombre del Contribuyente,Situación del contribuyente, ... (RFC es índice 1)
    IDX_RFC = 1
    IDX_NOMBRE = 2
    IDX_ESTATUS = 3
    IDX_FECHA_DOF = 15 # 'Publicación DOF definitivos'
    
    
    # Si la lista de encabezados es más corta que 16, ajustamos el índice de fecha DOF
    if len(headers_row) <= IDX_FECHA_DOF:
         print(f"[EFOS] ⚠️ Advertencia: Número de columnas insuficiente ({len(headers_row)}). Se omitirá la fecha DOF.")
         IDX_FECHA_DOF = -1 
         

    # 2. Iterar sobre el resto de las filas (como listas)
    for i, row in enumerate(reader):
        if not row: continue
        
        # 3. Extraer valores por índice, asegurando que el índice exista y blindando contra errores
        
        # RFC (Índice 1)
        rfc_raw = row[IDX_RFC] if len(row) > IDX_RFC else ""
        # str() asegura que es string antes de strip()
        rfc = re.sub(r"[^A-Z0-9]", "", str(rfc_raw).upper().strip()) 
        
        if not rfc: continue
        
        # Nombre (Índice 2)
        nombre_raw = row[IDX_NOMBRE] if len(row) > IDX_NOMBRE else ""
        nombre = str(nombre_raw).strip() or None
        
        # Estatus (Índice 3)
        estatus_raw = row[IDX_ESTATUS] if len(row) > IDX_ESTATUS else ""
        estatus = normaliza_estatus(estatus_raw) # Llama a la función blindada
        
        # Fecha DOF (Índice 15, si existe)
        fecha_dof = None
        if IDX_FECHA_DOF != -1 and len(row) > IDX_FECHA_DOF:
            fecha_dof_raw = row[IDX_FECHA_DOF]
            # La columna 15 puede tener múltiples fechas, tomamos la primera parte
            fecha_str = str(fecha_dof_raw).split(" - ")[0].split(" // ")[0].strip()
            fecha_dof = _parse_date_mx(fecha_str)

        # Tipo Persona (La columna no existe en el archivo del SAT, se deja como None)
        tipo_persona = None 
        
        filas.append(dict(
            rfc=rfc, 
            fuente="69-B", 
            tipo_evento=estatus, 
            fecha_dof=fecha_dof, 
            nombre=nombre, 
            tipo_persona=tipo_persona,
        ))
    return filas
# ----------------------------------------------------

# ───────────────────────────────────────────
# BD MySQL
# ───────────────────────────────────────────
# ───────────────────────────────────────────
# BD MySQL (VERSIÓN FINAL SEGURA Y COMPATIBLE)
# ───────────────────────────────────────────
def get_mysql_conn():
    # CLAVE: Buscamos las variables directamente desde el entorno (os.getenv).
    # No usamos valores por defecto, forzando a que las variables existan.
    host = os.getenv('MYSQL_HOST')
    user = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD') # Compatible con proyecto OFAC
    db = os.getenv('MYSQL_DATABASE')       # Compatible con proyecto OFAC

    # Verificación de Seguridad: Si falta cualquier credencial, el script FALLA.
    if not password or not db or not host or not user:
        raise ValueError(
            "FATAL: Credenciales de BD no encontradas. Asegúrese de que MYSQL_HOST, "
            "MYSQL_USER, MYSQL_PASSWORD y MYSQL_DATABASE estén definidas como "
            "variables de entorno o en su archivo .env."
        )

    return pymysql.connect(
        host=host, 
        user=user, 
        password=password, 
        database=db, 
        charset='utf8mb4', 
        cursorclass=pymysql.cursors.Cursor, 
        autocommit=False
    )
def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        # **ESTA ES LA VERSIÓN FINAL Y SEGURA**
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

# ───────────────────────────────────────────
# BD MySQL
# ───────────────────────────────────────────
# ... (get_mysql_conn y ensure_tables quedan igual)

def upsert_taxpayer(conn, filas: list[dict], corte: date):
    ensure_tables(conn)
    data_to_load = []
    latest_status = {}
    
    # 1. Consolidar el estado más reciente por RFC (usando fecha_dof)
    for f in filas:
        rfc = f['rfc']
        
        # current_dof: Asegura que la fecha actual sea date.min si es None
        current_dof = f.get('fecha_dof') or date.min
        
        # stored_dof: Si el RFC ya existe, obtenemos la fecha almacenada.
        # Si la fecha almacenada es None, usamos date.min para la comparación.
        if rfc in latest_status:
            stored_dof = latest_status[rfc].get('fecha_dof') or date.min
        else:
            # Si no existe, usamos date.min
            stored_dof = date.min
        
        # Comparamos solo si la entrada actual es MÁS nueva que la almacenada
        # o si el RFC es nuevo.
        if rfc not in latest_status or current_dof > stored_dof:
            latest_status[rfc] = f
            
    # 2. Formatear datos para la inserción
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
        print(f"[EFOS] Cargando {len(data_to_load)} registros consolidados (UPSERT)...")
        cur.executemany(UPSERT_SQL, data_to_load) 
    print(f"[EFOS] ✓ UPSERT de {len(data_to_load)} contribuyentes finalizado.")
    conn.commit()


# ───────────────────────────────────────────
# Main
# ───────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None, help="Ruta CSV local (offline)")
    ap.add_argument("--zip", default=None, help="Ruta ZIP local (offline)")
    ap.add_argument("--fast", action="store_true", help="Prioriza URLs del HTML de Datos Abiertos")
    ap.add_argument("--save-only", action="store_true", help="Descarga el ZIP/CSV y lo guarda en /tmp, sin insertar a BD.") 
    args = ap.parse_args()

    conn = None
    try:
        session = mk_session()
        print("[EFOS] ⬇️ Iniciando descarga...")
        
        buf_or_bytes, sha, src_url, vdate = descargar_csv_o_zip(session, args.csv, args.zip, fast=args.fast)
        
        # --- Lógica de Separación (Guardar) ---
        if args.save_only:
            is_zip = not isinstance(buf_or_bytes, io.StringIO)
            
            if is_zip:
                output_path = TEMP_DIR / f"efos_69b_{sha[:8]}.zip"
                output_path.write_bytes(buf_or_bytes)
                print(f"\n✓ Archivo ZIP descargado y guardado en: {output_path}")
                print(f"Para cargarlo, usa: python import_efos_mysql.py --zip {output_path}")
            else:
                # Si fue CSV directo
                output_path = TEMP_DIR / f"efos_69b_{date.today().isoformat()}.csv"
                output_path.write_text(buf_or_bytes.getvalue(), encoding='utf-8')
                print(f"\n✓ Archivo CSV descargado y guardado en: {output_path}")
                print(f"Para cargarlo, usa: python import_efos_mysql.py --csv {output_path}")
            return
            # ------------------------------------

        # --- Lógica de Inserción (Parsear y Cargar) ---
        
        # Si descargamos un ZIP o usamos --zip, necesitamos descomprimirlo a un buffer CSV.
        if not isinstance(buf_or_bytes, io.StringIO):
             print("[EFOS] Descomprimiendo ZIP a buffer CSV...")
             buf_or_bytes, encabezado = _csv_buffer_from_zip(buf_or_bytes)
             vdate = _parse_global_header_date(encabezado)

        # Ahora buf_or_bytes es definitivamente un io.StringIO
        print("[EFOS] 🧩 Iniciando parseo...")
        filas = parse_csv_rows(buf_or_bytes)
        
        if not filas:
            print("[EFOS] ❌ No se encontraron filas para importar. Finalizando.")
            
            # DIAGNÓSTICO: Imprimimos los encabezados detectados por el lector CSV simple
            buf_or_bytes.seek(0)
            print("\n[EFOS] --- DIAGNÓSTICO DE ENCABEZADOS ---")
            # Usamos csv.reader para imprimir el encabezado de la primera línea
            temp_reader = csv.reader(buf_or_bytes, delimiter=',', quotechar='"')
            try:
                headers = next(temp_reader)
                print(f"[EFOS] ENC: {headers}")
            except Exception as e:
                print(f"[EFOS] No se pudo leer el encabezado para diagnóstico: {e}")
                
            return
            # ---------------------------------------------
            
        print(f"[EFOS] Filas parseadas: {len(filas)}")
        
        conn = get_mysql_conn()
        corte = vdate or date.today()
        
        upsert_taxpayer(conn, filas, corte)
        
        print(f"\n✓ Carga completa de EFOS terminada exitosamente. ({len(filas)} registros)")

    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: El script ha fallado. Detalles: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    main()