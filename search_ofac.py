# Ruta sugerida: ~/Público/proyectos/LNofac_efos_ppe/
# Archivo: search_ofac.py

import pymysql.cursors
import os
import sys
from dotenv import load_dotenv
from typing import List, Dict, Any

# Carga las variables de entorno (asegúrate de tener un archivo .env con tus credenciales)
load_dotenv()

# --- Configuración de Conexión ---
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASS', ''),
    'database': os.getenv('MYSQL_DB', 'notaria'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
# -----------------------------------------------------------------

def clean_search_term(name: str) -> str:
    """Limpia el término de búsqueda en Python para usar en la consulta SQL."""
    name = name.upper()
    name = name.replace(',', '')
    name = name.replace('  ', ' ')
    return f"%{name.strip()}%"

def search_ofac(search_name: str) -> List[Dict[str, Any]]:
    """Ejecuta la búsqueda en las tablas de OFAC y devuelve los resultados."""
    
    clean_name_sql = clean_search_term(search_name)
    
    # Consulta SQL optimizada para evitar errores de COLLATION (utf8mb4_unicode_ci)
    QUERY = """
    SELECT
        'OFAC' AS Fuente,
        p.ID AS ID_Entidad,
        p.FULL_NAME AS Nombre_Principal,
        n.FULL_NAME AS Nombre_Coincidente,
        p.REMARKS AS Observaciones
    FROM 
        OFAC_PARTY_PROFILE p
    JOIN 
        OFAC_NAME n ON p.ID = n.PROFILE_ID
    WHERE 
        UPPER(REPLACE(REPLACE(p.FULL_NAME, ',', ''), '  ', ' '))
        LIKE %s COLLATE utf8mb4_unicode_ci 

    UNION

    SELECT
        'OFAC (Alias)' AS Fuente,
        p.ID AS ID_Entidad,
        p.FULL_NAME AS Nombre_Principal,
        n.FULL_NAME AS Nombre_Coincidente,
        p.REMARKS AS Observaciones
    FROM 
        OFAC_PARTY_PROFILE p
    JOIN 
        OFAC_NAME n ON p.ID = n.PROFILE_ID
    WHERE 
        UPPER(REPLACE(REPLACE(n.FULL_NAME, ',', ''), '  ', ' '))
        LIKE %s COLLATE utf8mb4_unicode_ci 
    
    LIMIT 50; 
    """

    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # Ejecutamos la consulta, pasando el valor limpio dos veces (%s, %s)
            cursor.execute(QUERY, (clean_name_sql, clean_name_sql))
            results = cursor.fetchall()
            return results

    except pymysql.Error as e:
        print(f"❌ Error de MySQL: {e}")
        return []
    finally:
        if conn:
            conn.close()


def print_results(search_term: str, results: List[Dict[str, Any]]):
    """Imprime los resultados de forma legible en la consola."""
    print(f"\n--- Resultados de Búsqueda para: '{search_term}' ---")
    
    if not results:
        print("❌ No se encontraron coincidencias.")
    else:
        print(f"✅ Coincidencias encontradas: {len(results)}")
        for i, row in enumerate(results):
            print("-" * 50)
            print(f"| {i+1}. Fuente: {row['Fuente']} | ID: {row['ID_Entidad']}")
            print(f"| Nombre Principal: {row['Nombre_Principal']}")
            print(f"| Coincidencia: {row['Nombre_Coincidente']}")
            print(f"| Observaciones: {row['Observaciones'][:100]}...") # Limitamos a 100 caracteres

if __name__ == "__main__":
    # La aplicación debe aceptar un nombre como argumento de línea de comandos.
    if len(sys.argv) < 2:
        print("Uso: python search_ofac.py \"Nombre a buscar\"")
        sys.exit(1)
        
    search_term = sys.argv[1]
    
    results = search_ofac(search_term)
    
    print_results(search_term, results)