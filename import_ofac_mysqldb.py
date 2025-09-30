import argparse
import csv
import io
import os
import re
import requests
import sys
import time
from pathlib import Path
from typing import List, Tuple, Iterable

import pymysql
from dotenv import load_dotenv

# ── Configuración y Conexión ──────────────────────────────────────────────────

try:
    # Carga .env desde la carpeta del script
    load_dotenv(Path(__file__).resolve().parents[0] / ".env")
except Exception:
    pass

def get_conn():
    """Establece conexión a MySQL usando las variables de .env."""
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        database=os.getenv("MYSQL_DATABASE", "compliance_db"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        cursorclass=pymysql.cursors.Cursor
    )

# ── Descarga (Lógica Robusta de tu Archivo ofac.py) ───────────────────────────

USER_AGENT = "busquedaLN/1.0 (+local)"
CONNECT_TIMEOUT = 5.0        # segundos (conexión)
READ_TIMEOUT = 10.0          # segundos (cada lectura de socket)
TOTAL_DEADLINE = 45.0        # segundos (tope por descarga)
CHUNK_SIZE = 64 * 1024       # 64 KiB

# Fuentes primarias y espejos (las que te funcionan)
URLS = {
    "sdn.csv": [
        "https://www.treasury.gov/ofac/downloads/sdn.csv",
        "https://home.treasury.gov/ofac/downloads/sdn.csv",
        "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV",
    ],
    "alt.csv": [
        "https://www.treasury.gov/ofac/downloads/alt.csv",
        "https://home.treasury.gov/ofac/downloads/alt.csv",
        "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/AKA.CSV",
    ],
}

def _http_get_first_ok(urls: Iterable[str]) -> bytes | None:
    """Prueba una lista de URLs y devuelve bytes del primer 200 OK."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})

    for url in urls:
        start = time.monotonic()
        try:
            print(f"  [OFAC] GET {url}", flush=True)
            with sess.get(url, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True) as r:
                r.raise_for_status()
                buf = bytearray()
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        buf.extend(chunk)
                    if (time.monotonic() - start) > TOTAL_DEADLINE:
                        raise TimeoutError(f"deadline {TOTAL_DEADLINE:.0f}s excedido")
                return bytes(buf)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"  [OFAC]   FAIL: {type(e).__name__}: {e}", flush=True)
            time.sleep(0.2)
    return None


def download_ofac_files(target_dir: Path) -> bool:
    """Descarga sdn.csv y alt.csv usando la lógica de reintentos y espejos."""
    target_dir.mkdir(parents=True, exist_ok=True)
    sdn_ok = False
    
    print("⬇️  Iniciando descarga de OFAC...", flush=True)

    # Descargar SDN.CSV
    out_path_sdn = target_dir / "sdn.csv"
    content_sdn = _http_get_first_ok(URLS["sdn.csv"])
    if content_sdn:
        out_path_sdn.write_bytes(content_sdn)
        print(f"  [OFAC] ✓ OK {len(content_sdn)} bytes → {out_path_sdn}", flush=True)
        sdn_ok = True
    else:
        print(f"  [OFAC] ❌ No se pudo descargar sdn.csv.", flush=True)
        return False
        
    # Descargar ALT.CSV (opcional pero necesario para los aliases)
    out_path_alt = target_dir / "alt.csv"
    content_alt = _http_get_first_ok(URLS["alt.csv"])
    if content_alt:
        out_path_alt.write_bytes(content_alt)
        print(f"  [OFAC] ✓ OK {len(content_alt)} bytes → {out_path_alt}", flush=True)
    else:
        print(f"  [OFAC] ⚠️ No se pudo descargar alt.csv. Continuando sin aliases fuertes.", flush=True)

    print("✓ Descarga de archivos de OFAC finalizada.")
    return sdn_ok

# ── Utilidades de Mapeo y RegEx ────────────────────────────────────────────────
# (Código de carga de caché, get_id, extract_weak_akas: sin cambios)
REF_CACHE = {} 
WEAK_AKA_REGEX = re.compile(r'(?:a\.k\.a\.|\sf\.k\.a\.|\sn\.k\.a\.)\s*[\'"]?([^\'"()\.;]+)[\'"]?', re.IGNORECASE)

def load_reference_cache(conn):
    """Carga los IDs de las tablas de referencia una vez al inicio."""
    global REF_CACHE
    REF_CACHE['party_type'] = {}
    REF_CACHE['alias_type'] = {}
    REF_CACHE['program'] = {}
    
    with conn.cursor() as cur:
        cur.execute("SELECT ID, VALUE FROM OFAC_REF_PARTY_TYPE")
        for id, value in cur.fetchall():
            REF_CACHE['party_type'][value.upper()] = id
        cur.execute("SELECT ID, CODE FROM OFAC_REF_ALIAS_TYPE")
        for id, code in cur.fetchall():
            REF_CACHE['alias_type'][code.upper()] = id
        cur.execute("SELECT ID, CODE FROM OFAC_REF_SANCTION_PROG")
        for id, code in cur.fetchall():
            REF_CACHE['program'][code.upper()] = id

def get_id(ref_type: str, value: str) -> int:
    """Busca el ID en el caché. Devuelve 999 si no se encuentra (ID de fallback)."""
    if not value: return 999
    return REF_CACHE.get(ref_type, {}).get(value.upper().strip(), 999)

def extract_weak_akas(ent_num: int, remarks: str) -> List[Tuple]:
    """Extrae alias débiles (Weak AKAs) del texto de Remarks usando RegEx."""
    if not remarks:
        return []
    
    weak_akas = []
    weak_id = get_id('alias_type', 'WEAK')
    
    for match in WEAK_AKA_REGEX.finditer(remarks):
        alias = match.group(1).strip()
        if alias and alias != '-0-':
            # Tuple: (ID, PROFILE_ID, FULL_NAME, NAME_TYPE_ID, IS_PRIMARY, STRENGTH)
            weak_akas.append((0 - ent_num, ent_num, alias, weak_id, 0, 'weak'))
    return weak_akas

# ── Funciones de Parsing ──────────────────────────────────────────────────────

def _read_text(path: Path) -> str:
    """
    Lee CSV forzando latin-1 para manejar la codificación de OFAC y limpia 
    el carácter de EOF/sustitución (\x1a).
    """
    b = path.read_bytes()
    
    # AÑADIDO: Elimina el byte 0x1a, que causa el error de conversión a int.
    b = b.replace(b'\x1a', b'')
    
    # Usar la decodificación que ya teníamos
    return b.decode("latin-1", errors="ignore")

def parse_sdn(text: str) -> Tuple[List, List, List]:
    """Parsea sdn.csv para perfiles, nombres primarios y weak AKAs."""
    profiles, primary_names, weak_aliases = [], [], []
    rd = csv.reader(io.StringIO(text))
    _ = next(rd, None) # header
    
    for r in rd:
        if not r or r[0].strip() == '-0-': continue
        
        ent = int((r[0] or "0").strip())
        name = (r[1] or "").strip() or None
        sdn_type = (r[2] or "").strip() or None
        program_code = (r[3] or "").strip() or None
        remarks = (r[11] if len(r) > 11 else "").replace('-0-', '').strip() or None
        
        profiles.append((ent, get_id('party_type', sdn_type), get_id('program', program_code), name, remarks))

        if name:
            primary_names.append((ent, ent, name, get_id('alias_type', 'AKA'), 1, 'strong'))

        weak_aliases.extend(extract_weak_akas(ent, remarks))
        
    return profiles, primary_names, weak_aliases

def parse_alt(text: str) -> List[Tuple]:
    """Parsea alt.csv para Strong Aliases."""
    out = []
    rd = csv.reader(io.StringIO(text))
    _ = next(rd, None)
    for r in rd:
        if not r or r[0].strip() == '-0-': continue
        
        try:
            ent = int((r[0] or "0").strip())
            alt_num = int((r[1] or "0").strip())
        except Exception: continue
        
        alt_type_code = (r[2] or "").strip().replace('-0-', '') or None
        alias = (r[3] or "").strip().replace('-0-', '')
        
        if alias:
            out.append((alt_num, ent, alias, get_id('alias_type', alt_type_code), 0, 'strong'))
            
    return out


# ── Lógica de Carga y Switch (Full Load con Tablas Temporales) ────────────────
# (Código de carga MySQL: sin cambios)
# ── Lógica de Carga y Switch (Full Load con Tablas Temporales) ────────────────

# ── Lógica de Carga y Switch (Full Load con Tablas Temporales) ────────────────

def truncate_and_load(conn, sdn_data: Tuple, alt_rows: List = None):
    """Implementa el método de Doble Buffer (Rename) para Full Load."""
    
    profiles, primary_names, weak_aliases = sdn_data
    all_names_to_load = primary_names + (alt_rows or []) + weak_aliases
    
    TEMP_SUFFIX = "_NEW"

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
        print("[1/3] Creando tablas temporales...")
        cur.execute(f"DROP TABLE IF EXISTS OFAC_PARTY_PROFILE{TEMP_SUFFIX};")
        cur.execute(f"CREATE TABLE OFAC_PARTY_PROFILE{TEMP_SUFFIX} LIKE OFAC_PARTY_PROFILE;")
        cur.execute(f"DROP TABLE IF EXISTS OFAC_NAME{TEMP_SUFFIX};")
        cur.execute(f"CREATE TABLE OFAC_NAME{TEMP_SUFFIX} LIKE OFAC_NAME;")
        
        # 2. Carga de datos en Tablas Temporales
        print(f"[2/3] Cargando {len(profiles)} perfiles y {len(all_names_to_load)} alias...")
        
        cur.executemany(PROFILE_INSERT_SQL, profiles)
        cur.executemany(NAME_INSERT_SQL, all_names_to_load)
        
        # 3. RENAME RÁPIDO (Switch atómico)
        print("[3/3] Realizando switch de tablas...")
        
        # DESHABILITAR FK CHECKS y LIMPIAR MESAS VIEJAS (para evitar 1217 y 1050)
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;") 
        
        # ELIMINAMOS cualquier tabla _OLD que haya quedado de un fallo anterior (FIX 1050)
        cur.execute("DROP TABLE IF EXISTS OFAC_PARTY_PROFILE_OLD, OFAC_NAME_OLD;") 
        
        rename_sql = f"""
        RENAME TABLE 
            OFAC_PARTY_PROFILE TO OFAC_PARTY_PROFILE_OLD,
            OFAC_PARTY_PROFILE{TEMP_SUFFIX} TO OFAC_PARTY_PROFILE,
            OFAC_NAME TO OFAC_NAME_OLD,
            OFAC_NAME{TEMP_SUFFIX} TO OFAC_NAME;
        """
        cur.execute(rename_sql) 
        
        # REHABILITAR FK CHECKS
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
    conn.commit()

# ── Punto de Entrada CLI ──────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Importador Full Load de OFAC SDN a MySQL.")
    ap.parse_args()
    
    # Target directory donde se guardan los CSV.
    target_dir = Path(__file__).resolve().parent / "data" / "ofac"
    sdn_path = target_dir / "sdn.csv"
    alt_path = target_dir / "alt.csv"
    
    try:
        conn = get_conn()
        load_reference_cache(conn)

        # 1. DESCARGA ESTABLE
        if not download_ofac_files(target_dir):
             print("❌ ERROR: La descarga de archivos ha fallado. El script se detiene.")
             return

        # 2. Lectura y Parseo
        if not sdn_path.exists():
             print("❌ ERROR FATAL: No se pudo encontrar sdn.csv después de la descarga.")
             return
             
        sdn_data = parse_sdn(_read_text(sdn_path))
        alt_rows = parse_alt(_read_text(alt_path)) if alt_path.exists() else None
        
        print(f"[OFAC] Perfiles: {len(sdn_data[0])} | Strong Aliases: {len(alt_rows or [])} | Weak Aliases (RegEx): {len(sdn_data[2])}")

        # 3. Ejecución de la Carga en MySQL
        truncate_and_load(conn, sdn_data, alt_rows)
        print(f"✓ Carga completa de OFAC terminada exitosamente. Datos disponibles en MySQL.")
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: El script ha fallado. Detalles: {e}")
        try:
            conn.rollback()
        except:
            pass
        raise
    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()