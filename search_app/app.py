# Ruta: search_app/app.py
# Nombre del archivo: app.py
import pymysql
pymysql.install_as_MySQLdb()
import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_mysqldb import MySQL
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from dotenv import load_dotenv
import traceback
from datetime import date
# IMPORTADO PARA LA BÚSQUEDA FUZZY (DEBES INSTALAR: pip install rapidfuzz)
from rapidfuzz import fuzz 

# Importamos las funciones de carga de datos y las de consulta de fecha del módulo modular
# ...
from search_app.data_importer import (
    run_efos_update, 
    run_ofac_update, 
    get_last_update_time_efos, 
    get_last_update_time_ofac
)

# ----------------
# 1. Configuración
# ----------------

# Cargar variables de entorno desde el archivo .env
load_dotenv()

app = Flask(__name__)

# Configuración de la clave secreta para la sesión (Usando os.urandom es más seguro)
app.secret_key = os.urandom(24)

# Configuración de MySQL usando variables de entorno
app.config['MYSQL_HOST'] = os.getenv('MYSQL_HOST')
app.config['MYSQL_USER'] = os.getenv('MYSQL_USER')
app.config['MYSQL_PASSWORD'] = os.getenv('MYSQL_PASSWORD')
app.config['MYSQL_DB'] = os.getenv('MYSQL_DATABASE')
app.config['MYSQL_PORT'] = int(os.getenv('MYSQL_PORT', 3306)) # Usar 3306 como defecto

mysql = MySQL(app)

# Configuración de Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# -------------------
# 2. Modelo de Usuario
# -------------------

# Clase simple para manejar usuarios con Flask-Login
class User(UserMixin):
    def __init__(self, user_id):
        # El user_id es el emp_ID de la tabla m_empleados
        self.id = user_id

# Función de carga de usuario para Flask-Login
@login_manager.user_loader
def load_user(user_id):
    try:
        # Se asume que el user_id es el emp_ID de la tabla m_empleados
        cur = mysql.connection.cursor()
        cur.execute("SELECT emp_ID FROM m_empleados WHERE emp_ID = %s", (user_id,))
        user_record = cur.fetchone()
        cur.close()
        if user_record:
            # Retorna el objeto User con el emp_ID
            return User(user_record[0])
        return None
    except Exception as e:
        print(f"Error al cargar usuario: {e}")
        return None

# -----------------
# 3. Rutas de la App
# -----------------

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('search'))

    if request.method == 'POST':
        # Los campos del formulario se mapean a los campos de la tabla m_empleados
        emp_clave = request.form['username']
        emp_pwd = request.form['password']

        try:
            cur = mysql.connection.cursor() 
            # Búsqueda en la tabla m_empleados por clave y contraseña
            cur.execute("SELECT emp_ID FROM m_empleados WHERE emp_Clave = %s AND emp_Pwd = %s", (emp_clave, emp_pwd))
            user_record = cur.fetchone()
            cur.close()

            if user_record:
                user_id = user_record[0]
                user = User(user_id)
                login_user(user)
                flash('Inicio de sesión exitoso.', 'success')
                return redirect(url_for('search'))
            else:
                flash('Credenciales inválidas. Por favor, inténtelo de nuevo.', 'danger')
        except Exception as e:
            flash(f'Error de base de datos durante el inicio de sesión.', 'danger')
            print("--- ERROR DE CONEXIÓN A MYSQL EN LOGIN ---")
            print(f"Error de DB al intentar login: {e}") 
            traceback.print_exc()

    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Has cerrado sesión correctamente.', 'info')
    return redirect(url_for('login'))

@app.route('/', methods=['GET', 'POST'])
@app.route('/search', methods=['GET', 'POST'])
@login_required
def search():
    
    PAGE_SIZE = 20 # Tamaño de página fijo

    # 1. Manejar la entrada de búsqueda y el parámetro de página
    if request.method == 'POST':
        query_text = request.form.get('query_text', '').strip()
        current_page = 1
    else:
        query_text = request.args.get('query_text', '').strip()
        try:
            current_page = int(request.args.get('page', 1))
        except ValueError:
            current_page = 1

    final_results = []
    total_results = 0
    results_for_template = []
    
    if query_text:
        # Umbral bajo (50) para capturar alias cortos que no están en el nombre principal
        FUZZY_THRESHOLD = 50 
        search_filter = f"%{query_text.lower()}%"
        
        raw_results = []
        flash_warnings = []

        try:
            cur = mysql.connection.cursor()
            
            # --- CONSULTA 1: EFOS_TAXPAYER ---
            efos_query = f"""
            SELECT 
                'EFOS' AS fuente,
                FULL_NAME AS encontrados,       
                RFC AS rfc,
                CURRENT_STATUS AS comentarios_status, 
                LAST_DOF_DATE AS fecha             
            FROM EFOS_TAXPAYER
            WHERE 
                LOWER(FULL_NAME) LIKE %s OR        
                LOWER(RFC) LIKE %s
            """
            cur.execute(efos_query, (search_filter, search_filter))
            raw_results.extend(cur.fetchall())
            
            # --- CONSULTA 2: OFAC_LIST (BUSCA EN 3 TABLAS PARA CAPTURAR EL ALIAS OCULTO) ---
            try:
                ofac_query = f"""
                SELECT DISTINCT
                    'OFAC' AS fuente,
                    T1.name AS encontrados,      
                    T1.rfc AS rfc,
                    /* Usamos GROUP_CONCAT para mostrar todos los alias y remarks relevantes */
                    CONCAT(
                        'Tipos: ', T1.alias_of, 
                        ' / Aliases: ', GROUP_CONCAT(DISTINCT T2.FULL_NAME SEPARATOR ' | '),
                        ' / Remarks: ', T3.REMARKS
                    ) AS comentarios_status, 
                    T1.date AS fecha
                FROM OFAC_LIST T1
                /* JOIN 1: Tabla de Aliases (FULL_NAME) */
                LEFT JOIN OFAC_NAME T2 ON T1.profile_id = T2.PROFILE_ID 
                /* JOIN 2: Tabla de Perfiles (REMARKS) - ¡Donde está "El Mencho"! */
                LEFT JOIN OFAC_PARTY_PROFILE T3 ON T1.profile_id = T3.ID 
                
                WHERE 
                    LOWER(T1.name) LIKE %s OR       
                    LOWER(T1.rfc) LIKE %s OR        
                    LOWER(T2.FULL_NAME) LIKE %s OR  
                    LOWER(T3.REMARKS) LIKE %s       
                GROUP BY T1.profile_id, T1.name, T1.rfc, T1.alias_of, T1.date, T3.REMARKS
                """
                
                # Se pasan 4 parámetros para los cuatro %s del WHERE
                cur.execute(ofac_query, (search_filter, search_filter, search_filter, search_filter))
                raw_results.extend(cur.fetchall()) 
            
            except Exception as ofac_e:
                flash_warnings.append("Advertencia: La búsqueda en listas OFAC falló. Se usaron solo los resultados de EFOS. Revise las tablas OFAC_LIST, OFAC_NAME, OFAC_PARTY_PROFILE.")
                print("!!! ADVERTENCIA: La consulta a OFAC falló. Revise si las tablas OFAC_LIST, OFAC_NAME, OFAC_PARTY_PROFILE y sus columnas clave (profile_id, ID) existen y están pobladas. !!!")
                print(f"Error de OFAC: {ofac_e}")
                traceback.print_exc()
                
            cur.close()
            
            # 3. Aplicar el Filtro Fuzzy y Procesar Fechas
            for row in raw_results:
                if len(row) != 5:
                    print(f"Error en el formato de datos: El resultado tiene {len(row)} columnas en lugar de 5.")
                    continue

                fuente, encontrados, rfc, comentarios_status, fecha = row
                
                # --- LÓGICA FUZZY ---
                norm_encontrados = str(encontrados).lower()
                norm_rfc = str(rfc).lower()
                norm_query = query_text.lower() 
                
                score_name = fuzz.partial_ratio(norm_query, norm_encontrados) 
                score_rfc = fuzz.partial_ratio(norm_query, norm_rfc)         
                
                # Usamos score_total para ordenar los resultados
                score_total = max(score_name, score_rfc)

                is_fuzzy_match = (score_name >= FUZZY_THRESHOLD) or (score_rfc >= FUZZY_THRESHOLD)

                if is_fuzzy_match:
                    row_list = list(row)
                    
                    # Añadir Score al campo de comentarios
                    if comentarios_status:
                        row_list[3] = f"{comentarios_status} | Score: {score_total:.2f}%" 
                    else:
                        row_list[3] = f"Score: {score_total:.2f}%" 

                    # Formatear fecha
                    if isinstance(row_list[4], date):
                        row_list[4] = row_list[4].strftime('%Y-%m-%d')
                    
                    # Añadimos el score_total como elemento 5 para ordenar después
                    final_results.append(tuple(row_list + [score_total])) 
            
            total_results = len(final_results)
            
            # ORDENAR: Ordenamos los resultados por score total (el elemento 5) de mayor a menor
            # Esto debería poner a Nemesio Oseguera (con 100%) por encima del registro de HAYDAR (66%)
            final_results.sort(key=lambda x: x[5], reverse=True)
            
            # 4. Aplicar Paginación (Slicing) - Lógica estable
            start_index = (current_page - 1) * PAGE_SIZE
            end_index = start_index + PAGE_SIZE
            
            total_pages = (total_results // PAGE_SIZE) + (1 if total_results % PAGE_SIZE else 0)
            
            # Corrección de límites
            if current_page > total_pages and total_results > 0:
                current_page = total_pages
                start_index = (current_page - 1) * PAGE_SIZE
                end_index = start_index + PAGE_SIZE
            if current_page < 1:
                current_page = 1
                start_index = 0
                end_index = PAGE_SIZE

            if start_index >= total_results and total_results > 0:
                start_index = max(0, total_results - PAGE_SIZE)
                current_page = (start_index // PAGE_SIZE) + 1

            paginated_results = final_results[start_index:end_index]
            # Solo devolvemos las primeras 5 columnas (sin el score temporal de ordenación)
            results_for_template = [row[:5] for row in paginated_results] 

            # Mostrar advertencias no fatales (como el fallo de OFAC)
            for warning in flash_warnings:
                flash(warning, 'warning')

            # Mensaje Flash para la búsqueda exitosa
            if total_results > 0:
                if request.method == 'POST':
                    flash(f"Búsqueda finalizada. Se encontraron {total_results} coincidencias por similitud.", 'success')
            elif total_results == 0 and request.method == 'POST':
                 flash(f"No se encontraron coincidencias para la búsqueda: '{query_text}'.", 'info')

        except Exception as e:
            flash('Error crítico al ejecutar la búsqueda. Verifique la conexión a la base de datos o la consulta EFOS.', 'danger')
            print("!!! ERROR CRÍTICO AL EJECUTAR LA BÚSQUEDA !!!")
            print(f"Error detallado: {e}")
            traceback.print_exc()

    # --- LÓGICA DE FECHAS DE ACTUALIZACIÓN ---
    # Llamamos a las funciones del importador para obtener las fechas.
    # El try/except es para atrapar fallas de conexión o si las tablas no existen.
    last_efos_update = None
    last_ofac_update = None
    
    try:
        last_efos_update = get_last_update_time_efos()
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo obtener la fecha de EFOS. Error: {e}")
        
    try:
        last_ofac_update = get_last_update_time_ofac()
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo obtener la fecha de OFAC. Error: {e}")
    # ------------------------------------------

    # Si no hay resultados o es la carga inicial, total_results es 0.
    return render_template('search.html', 
                           results=results_for_template, 
                           query_text=query_text, 
                           total_results=total_results,
                           page_size=PAGE_SIZE,
                           current_page=current_page,
                           # PASAMOS LAS FECHAS AL TEMPLATE
                           last_efos_update=last_efos_update,
                           last_ofac_update=last_ofac_update)


# RUTA MODIFICADA: Eliminada la ruta '/update_lists' que usaba flash/redirect

# NUEVA RUTA: Para actualizar solo EFOS
@app.route('/update_efos', methods=['POST'])
@login_required
def update_efos():
    try:
        # Llama a la función de carga de datos que ya verificamos que funciona
        total_loaded = run_efos_update() 
        # Devuelve una respuesta JSON
        return jsonify({'status': 'success', 'message': f'Base de datos de EFOS actualizada correctamente. {total_loaded} registros consolidados.'})

    except RuntimeError as e:
        # Error capturado por el Locking (proceso ya en curso)
        return jsonify({'status': 'locked', 'message': str(e)}), 423 # 423 Locked
    except ConnectionError as e:
        # Error capturado si MySQL está apagado
        return jsonify({'status': 'error', 'message': f'ERROR DE CONEXIÓN: {str(e)}'}), 500
    except Exception as e:
        # Cualquier otro error crítico
        print(f"Error en la actualización de EFOS: {e}")
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': f'Error crítico al actualizar EFOS: {str(e)}'}), 500


# NUEVA RUTA: Para actualizar solo OFAC
@app.route('/update_ofac', methods=['POST'])
@login_required
def update_ofac():
    try:
        # Llama a la función de carga de datos que ya verificamos que funciona
        total_loaded = run_ofac_update()
        # Devuelve una respuesta JSON
        return jsonify({'status': 'success', 'message': f'Base de datos de OFAC actualizada correctamente. {total_loaded} perfiles cargados.'})

    except RuntimeError as e:
        # Error capturado por el Locking (proceso ya en curso)
        return jsonify({'status': 'locked', 'message': str(e)}), 423 # 423 Locked
    except ConnectionError as e:
        # Error capturado si MySQL está apagado
        return jsonify({'status': 'error', 'message': f'ERROR DE CONEXIÓN: {str(e)}'}), 500
    except Exception as e:
        # Cualquier otro error crítico
        print(f"Error en la actualización de OFAC: {e}")
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': f'Error crítico al actualizar OFAC: {str(e)}'}), 500


# -----------------
# 4. Inicialización
# -----------------
if __name__ == '__main__':
    # Usamos host='0.0.0.0' y port=8080 para asegurar el acceso estable.
    try:
        # Intenta iniciar la aplicación
        print(f"[*] Servidor Flask iniciando en http://0.0.0.0:8080/login (Accesible vía 127.0.0.1:8080)")
        app.run(debug=True, host='0.0.0.0', port=8080) # <-- Puerto 8080
    except Exception as e:
        # Captura y muestra cualquier error crítico al inicio (como fallos de MySQL o de Flask)
        print("!!! ERROR CRÍTICO al iniciar la aplicación:")
        print(e)
        traceback.print_exc()
