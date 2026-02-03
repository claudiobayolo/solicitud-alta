from flask import Flask, request, jsonify, send_from_directory
import sqlite3
from datetime import datetime
import os
import pyodbc
import logging
import requests
from functools import wraps
from typing import Optional, Dict, List, Tuple

# ==================== CONFIGURACIÓN ====================
DB_PATH = "/home/cfbayolo/mysite/alta.db"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = "/home/cfbayolo/mysite/logs"

# Crear directorio de logs si no existe
os.makedirs(LOG_PATH, exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_PATH, 'flask_app.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

SQL_CONN_STR = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=NOTDELL191114\\SQLEXPRESS;'
    'DATABASE=SolicitudAlta;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)

# ==================== UTILIDADES ====================
def normalizar_rut(rut: str) -> str:
    """Normaliza RUT: elimina puntos, espacios y convierte a minúsculas."""
    return rut.replace(".", "").replace("-", "").strip().lower()

def validar_rut(rut: str) -> bool:
    """Valida formato básico de RUT chileno."""
    rut_clean = normalizar_rut(rut)
    return len(rut_clean) >= 8 and len(rut_clean) <= 10

def validar_datos_solicitud(data: Dict) -> Tuple[bool, str]:
    """Valida datos obligatorios de solicitud."""
    campos_obligatorios = [
        'fechaIngreso', 'rutCliente', 'cliente', 'nroSAM', 'razonSocial',
        'ejecutivoComercial', 'fonoEjecutivo', 'contactoCliente', 
        'fonoContactoCliente', 'contactoTecnico', 'fonoContactoTecnico',
        'jefeProyecto', 'fonoJefeProyecto', 'proyecto', 'pepGasto',
        'proveedor', 'actividad', 'tipoDireccion', 'conceptoOtrosCostos',
        'monedaOtrosCostos', 'monedaInstalacion', 'monedaRenta', 'plazoMeses'
    ]
    
    for campo in campos_obligatorios:
        if campo not in data or not str(data.get(campo, '')).strip():
            return False, f"Campo obligatorio faltante: {campo}"
    
    # Validar RUT
    if not validar_rut(data['rutCliente']):
        return False, "Formato de RUT inválido"
    
    # Validar valores numéricos
    try:
        float(data.get('montoOtrosCostos', 0))
        float(data.get('costoInstalacion', 0))
        float(data.get('valorRenta', 0))
        int(data['plazoMeses'])
    except ValueError:
        return False, "Valores numéricos inválidos"
    
    # Validar direcciones
    direcciones = data.get('direcciones', [])
    if not isinstance(direcciones, list) or len(direcciones) == 0:
        return False, "Debe incluir al menos una dirección"
    
    return True, "OK"

def handle_errors(f):
    """Decorador para manejo centralizado de errores."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error en {f.__name__}: {str(e)}", exc_info=True)
            return jsonify({
                "status": "ERROR",
                "mensaje": f"Error interno del servidor: {str(e)}"
            }), 500
    return decorated_function

# ==================== BASE DE DATOS ====================
def get_db() -> sqlite3.Connection:
    """Obtiene conexión a SQLite con mejor manejo de errores."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Permite acceso por columna
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error conectando a SQLite: {e}")
        raise

def init_db():
    """Inicializa las tablas de SQLite."""
    db = get_db()
    cur = db.cursor()
    
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS solicitud (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          fechaingreso TEXT NOT NULL,
          rutcliente TEXT NOT NULL,
          cliente TEXT NOT NULL,
          nrosam TEXT NOT NULL,
          razonsocial TEXT NOT NULL,
          ejecutivocomercial TEXT NOT NULL,
          fonoejecutivo TEXT NOT NULL,
          contactocliente TEXT NOT NULL,
          fonocontactocliente TEXT NOT NULL,
          contactotecnico TEXT NOT NULL,
          fonocontactotecnico TEXT NOT NULL,
          jefeproyecto TEXT NOT NULL,
          fonojefeproyecto TEXT NOT NULL,
          proyecto TEXT NOT NULL,
          pepgasto TEXT NOT NULL,
          proveedor TEXT NOT NULL,
          actividad TEXT NOT NULL,
          tipodireccion TEXT NOT NULL,
          conceptootroscostos TEXT NOT NULL,
          monedaotros_costos TEXT NOT NULL,
          montootros_costos REAL DEFAULT 0,
          monedainstalacion TEXT NOT NULL,
          costoinstalacion REAL DEFAULT 0,
          monedarenta TEXT NOT NULL,
          valorrenta REAL DEFAULT 0,
          plazomeses INTEGER NOT NULL,
          fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          estado TEXT DEFAULT 'PENDIENTE'
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS direccion (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          solicitudid INTEGER NOT NULL,
          numero INTEGER NOT NULL,
          direccion TEXT NOT NULL,
          servicio TEXT NOT NULL,
          capacidad TEXT NOT NULL,
          FOREIGN KEY(solicitudid) REFERENCES solicitud(id)
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_estado (
          solicitud_id INTEGER PRIMARY KEY,
          estado_sync TEXT NOT NULL,
          fecha_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(solicitud_id) REFERENCES solicitud(id)
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS error_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          solicitud_id INTEGER,
          tipo_error TEXT NOT NULL,
          mensaje_error TEXT NOT NULL,
          detalle TEXT,
          fecha_error TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(solicitud_id) REFERENCES solicitud(id)
        )
        """)
        
        # Crear índices para mejorar búsquedas
        cur.execute("CREATE INDEX IF NOT EXISTS idx_solicitud_rut ON solicitud(rutcliente)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_estado ON sync_estado(estado_sync)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_direccion_solicitud ON direccion(solicitudid)")
        
        db.commit()
        logger.info("Base de datos inicializada correctamente")
    except sqlite3.Error as e:
        logger.error(f"Error inicializando BD: {e}")
        db.close()
        raise
    finally:
        db.close()

# Inicializar BD
init_db()

# ==================== RUTAS ====================
@app.route("/")
def index():
    """Sirve el archivo HTML principal."""
    try:
        return send_from_directory(BASE_DIR, "index.html")
    except Exception as e:
        logger.error(f"Error sirviendo index.html: {e}")
        return jsonify({"status": "ERROR", "mensaje": "Archivo no encontrado"}), 404

@app.route("/init", methods=["GET"])
@handle_errors
def init_form():
    """Obtiene número de solicitud y fecha actual."""
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute("SELECT IFNULL(MAX(id), 0) + 1 FROM solicitud")
        nro = cur.fetchone()[0]
        
        return jsonify({
            "solicitudNro": nro,
            "fechaIngreso": datetime.now().strftime("%d-%m-%Y"),
            "status": "OK"
        })
    finally:
        db.close()

@app.route('/guardarsolicitud', methods=['POST'])
@handle_errors
def guardar():
    """Guarda solicitud en SQLite y SQL Server."""
    data = request.json
    
    # Validar datos
    valido, mensaje = validar_datos_solicitud(data)
    if not valido:
        logger.warning(f"Validación fallida: {mensaje}")
        return jsonify({"status": "ERROR", "mensaje": mensaje}), 400
    
    sqlite_id = None
    sql_id = None
    sql_sync_status = "pendiente"
    
    # ===== GUARDAR EN SQLITE (BUFFER LOCAL) =====
    db_sqlite = get_db()
    try:
        cur_sqlite = db_sqlite.cursor()
        
        cur_sqlite.execute("""
            INSERT INTO solicitud (
                fechaingreso, rutcliente, cliente, nrosam, razonsocial, 
                ejecutivocomercial, fonoejecutivo, contactocliente, fonocontactocliente,
                contactotecnico, fonocontactotecnico, jefeproyecto, fonojefeproyecto,
                proyecto, pepgasto, proveedor, actividad, tipodireccion,
                conceptootroscostos, monedaotros_costos, montootros_costos,
                monedainstalacion, costoinstalacion, monedarenta, valorrenta, plazomeses
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['fechaIngreso'], normalizar_rut(data['rutCliente']), data['cliente'], 
            data['nroSAM'], data['razonSocial'], data['ejecutivoComercial'],
            data['fonoEjecutivo'], data['contactoCliente'], data['fonoContactoCliente'],
            data['contactoTecnico'], data['fonoContactoTecnico'], data['jefeProyecto'],
            data['fonoJefeProyecto'], data['proyecto'], data['pepGasto'],
            data['proveedor'], data['actividad'], data['tipoDireccion'],
            data['conceptoOtrosCostos'], data['monedaOtrosCostos'],
            float(data.get('montoOtrosCostos', 0)), data['monedaInstalacion'],
            float(data.get('costoInstalacion', 0)), data['monedaRenta'],
            float(data.get('valorRenta', 0)), int(data['plazoMeses'])
        ))
        
        sqlite_id = cur_sqlite.lastrowid
        
        # Guardar direcciones en SQLite
        direcciones = data.get('direcciones', [])
        for d in direcciones:
            cur_sqlite.execute("""
                INSERT INTO direccion (solicitudid, numero, direccion, servicio, capacidad)
                VALUES (?, ?, ?, ?, ?)
            """, (sqlite_id, d['numero'], d['direccion'], d['servicio'], d['capacidad']))
        
        db_sqlite.commit()
        logger.info(f"Solicitud guardada en SQLite con ID: {sqlite_id}")
    
    except sqlite3.Error as e:
        logger.error(f"Error guardando en SQLite: {e}")
        db_sqlite.rollback()
        return jsonify({"status": "ERROR", "mensaje": f"Error en BD local: {str(e)}"}), 500
    finally:
        db_sqlite.close()
    
    # ===== GUARDAR EN SQL SERVER =====
    try:
        conn_sql = pyodbc.connect(SQL_CONN_STR)
        cur_sql = conn_sql.cursor()
        
        cur_sql.execute("""
            INSERT INTO solicitud (
                fechaingreso, rutcliente, cliente, nrosam, razonsocial,
                ejecutivocomercial, fonoejecutivo, contactocliente, fonocontactocliente,
                contactotecnico, fonocontactotecnico, jefeproyecto, fonojefeproyecto,
                proyecto, pepgasto, proveedor, actividad, tipodireccion,
                conceptootroscostos, monedaotros_costos, montootros_costos,
                monedainstalacion, costoinstalacion, monedarenta, valorrenta, plazomeses
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['fechaIngreso'], normalizar_rut(data['rutCliente']), data['cliente'],
            data['nroSAM'], data['razonSocial'], data['ejecutivoComercial'],
            data['fonoEjecutivo'], data['contactoCliente'], data['fonoContactoCliente'],
            data['contactoTecnico'], data['fonoContactoTecnico'], data['jefeProyecto'],
            data['fonoJefeProyecto'], data['proyecto'], data['pepGasto'],
            data['proveedor'], data['actividad'], data['tipoDireccion'],
            data['conceptoOtrosCostos'], data['monedaOtrosCostos'],
            float(data.get('montoOtrosCostos', 0)), data['monedaInstalacion'],
            float(data.get('costoInstalacion', 0)), data['monedaRenta'],
            float(data.get('valorRenta', 0)), int(data['plazoMeses'])
        ))
        
        cur_sql.execute("SELECT @@IDENTITY")
        sql_id = int(cur_sql.fetchone()[0])
        
        # Guardar direcciones en SQL Server
        for d in direcciones:
            cur_sql.execute("""
                INSERT INTO direccion (solicitudid, numero, direccion, servicio, capacidad)
                VALUES (?, ?, ?, ?, ?)
            """, (sql_id, d['numero'], d['direccion'], d['servicio'], d['capacidad']))
        
        conn_sql.commit()
        conn_sql.close()
        
        sql_sync_status = f"OK (ID: {sql_id})"
        logger.info(f"Solicitud sincronizada a SQL Server con ID: {sql_id}")
        
        # Marcar como sincronizado
        db = get_db()
        try:
            cur = db.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO sync_estado (solicitud_id, estado_sync) VALUES (?, 'SINCRONIZADO')",
                (sqlite_id,)
            )
            db.commit()
        finally:
            db.close()
    
    except pyodbc.Error as e:
        sql_sync_status = f"Error: {str(e)}"
        logger.error(f"Error sincronizando a SQL Server: {e}")
        
        # Registrar error en tabla de logs
        db = get_db()
        try:
            cur = db.cursor()
            cur.execute(
                "INSERT INTO error_log (solicitud_id, tipo_error, mensaje_error) VALUES (?, ?, ?)",
                (sqlite_id, "SQL_SERVER_SYNC", str(e))
            )
            db.commit()
        finally:
            db.close()
    
    except Exception as e:
        sql_sync_status = f"Error inesperado: {str(e)}"
        logger.error(f"Error inesperado sincronizando: {e}")
    
    return jsonify({
        "status": "OK",
        "sqlite_id": sqlite_id,
        "sql_server_sync": sql_sync_status
    }), 201

@app.route("/buscar_cliente")
@handle_errors
def buscar_cliente():
    """Busca nombre de cliente por RUT en archivo compartido de Google Drive."""
    rut = normalizar_rut(request.args.get("rut", ""))
    
    if not rut or not validar_rut(rut):
        logger.warning(f"RUT inválido o vacío: {rut}")
        return jsonify({"cliente": "", "validacion": False, "error": "RUT inválido"}), 400
    
    url = "https://drive.google.com/uc?export=download&id=10EUZK61nkiZ90IbNOYLjAtnWKz-9IKPx"
    
    try:
        logger.info(f"🔍 Buscando RUT: {rut}")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = requests.get(url, stream=True, timeout=15, headers=headers, allow_redirects=True)
        resp.raise_for_status()
        
        logger.info(f"✅ Archivo descargado. Status: {resp.status_code}")
        
        cliente_encontrado = None
        ruts_encontrados = []  # Para debug
        lineas_procesadas = 0
        lineas_con_datos = 0
        
        for linea_raw in resp.iter_lines(decode_unicode=True):
            lineas_procesadas += 1
            
            if isinstance(linea_raw, bytes):
                linea = linea_raw.decode('utf-8', errors='ignore')
            else:
                linea = linea_raw
            
            linea = linea.strip()
            
            # Saltar línea vacía o cabecera
            if not linea or linea.lower().startswith('rut'):
                continue
            
            # Detectar separador (puede ser ; , o |)
            separadores = [';', ',', '|', '\t']
            separador_usado = None
            
            for sep in separadores:
                if sep in linea:
                    separador_usado = sep
                    break
            
            if not separador_usado:
                logger.debug(f"Línea {lineas_procesadas} sin separador: {linea[:50]}")
                continue
            
            try:
                partes = linea.split(separador_usado)
                if len(partes) < 2:
                    continue
                
                rut_txt = partes[0].strip()
                cliente = partes[1].strip()
                
                if not rut_txt or not cliente:
                    continue
                
                rut_normalizado = normalizar_rut(rut_txt)
                lineas_con_datos += 1
                
                # Guardar primeros 5 RUT para debug
                if len(ruts_encontrados) < 5:
                    ruts_encontrados.append(f"{rut_txt} → {rut_normalizado}")
                
                # Buscar coincidencia
                if rut_normalizado == rut:
                    cliente_encontrado = cliente
                    logger.info(f"✅ ENCONTRADO: RUT {rut} = {cliente}")
                    break
            
            except Exception as e:
                logger.debug(f"Error en línea {lineas_procesadas}: {e}")
                continue
        
        # Log detallado
        logger.info(f"📊 RESUMEN: Lineas={lineas_procesadas}, Con datos={lineas_con_datos}")
        if ruts_encontrados:
            logger.info(f"📋 Primeros RUT encontrados: {ruts_encontrados}")
        
        if cliente_encontrado:
            return jsonify({
                "cliente": cliente_encontrado,
                "validacion": True,
                "encontrado": True
            }), 200
        else:
            logger.warning(f"❌ RUT {rut} no encontrado después de procesar {lineas_con_datos} registros")
            return jsonify({
                "cliente": "",
                "validacion": False,
                "encontrado": False,
                "mensaje": f"RUT {rut} no encontrado"
            }), 404
    
    except Exception as e:
        logger.error(f"❌ ERROR en buscar_cliente: {type(e).__name__}: {str(e)}", exc_info=True)
        return jsonify({
            "cliente": "",
            "validacion": False,
            "error": str(e)
        }), 500


@app.route("/api/obtener_pendientes", methods=["GET"])
@handle_errors
def obtener_pendientes():
    """Obtiene todas las solicitudes pendientes de sincronización."""
    db = get_db()
    try:
        cur = db.cursor()
        
        # Solicitudes pendientes o sin sincronizar
        cur.execute("""
            SELECT s.*, 
                   COALESCE(se.estado_sync, 'PENDIENTE') as estado_sync,
                   se.fecha_sync
            FROM solicitud s
            LEFT JOIN sync_estado se ON s.id = se.solicitud_id
            WHERE se.estado_sync IS NULL OR se.estado_sync != 'SINCRONIZADO'
            ORDER BY s.fecha_creacion DESC
        """)
        
        solicitudes = []
        for row in cur.fetchall():
            sol = dict(row)
            
            # Obtener direcciones
            cur.execute("SELECT * FROM direccion WHERE solicitudid = ? ORDER BY numero", (sol['id'],))
            sol['direcciones'] = [dict(r) for r in cur.fetchall()]
            
            solicitudes.append(sol)
        
        logger.info(f"Se retornaron {len(solicitudes)} solicitudes pendientes")
        return jsonify(solicitudes), 200
    
    finally:
        db.close()

@app.route("/api/marcar_sincronizado", methods=["POST"])
@handle_errors
def marcar_sincronizado():
    """Marca una solicitud como sincronizada."""
    data = request.json
    
    if not data or 'solicitud_id' not in data:
        return jsonify({"status": "ERROR", "mensaje": "Parámetro solicitud_id requerido"}), 400
    
    solicitud_id = data['solicitud_id']
    
    db = get_db()
    try:
        cur = db.cursor()
        
        # Verificar que la solicitud existe
        cur.execute("SELECT id FROM solicitud WHERE id = ?", (solicitud_id,))
        if not cur.fetchone():
            return jsonify({"status": "ERROR", "mensaje": f"Solicitud {solicitud_id} no existe"}), 404
        
        # Marcar como sincronizado
        cur.execute(
            "INSERT OR REPLACE INTO sync_estado (solicitud_id, estado_sync) VALUES (?, 'SINCRONIZADO')",
            (solicitud_id,)
        )
        db.commit()
        
        logger.info(f"Solicitud {solicitud_id} marcada como SINCRONIZADO")
        return jsonify({"status": "OK", "solicitud_id": solicitud_id}), 200
    
    finally:
        db.close()

@app.route("/api/obtener_solicitud/<int:solicitud_id>", methods=["GET"])
@handle_errors
def obtener_solicitud(solicitud_id):
    """Obtiene detalle de una solicitud específica."""
    db = get_db()
    try:
        cur = db.cursor()
        
        cur.execute("SELECT * FROM solicitud WHERE id = ?", (solicitud_id,))
        row = cur.fetchone()
        
        if not row:
            return jsonify({"status": "ERROR", "mensaje": "Solicitud no encontrada"}), 404
        
        solicitud = dict(row)
        
        cur.execute("SELECT * FROM direccion WHERE solicitudid = ? ORDER BY numero", (solicitud_id,))
        solicitud['direcciones'] = [dict(r) for r in cur.fetchall()]
        
        # Obtener estado de sincronización
        cur.execute("SELECT estado_sync, fecha_sync FROM sync_estado WHERE solicitud_id = ?", (solicitud_id,))
        sync_row = cur.fetchone()
        if sync_row:
            solicitud['sync_estado'] = dict(sync_row)
        
        return jsonify(solicitud), 200
    
    finally:
        db.close()

# ==================== MANEJO DE ERRORES GLOBAL ====================
@app.errorhandler(404)
def not_found(error):
    logger.warning(f"Ruta no encontrada: {request.path}")
    return jsonify({"status": "ERROR", "mensaje": "Ruta no encontrada"}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Error interno del servidor: {error}")
    return jsonify({"status": "ERROR", "mensaje": "Error interno del servidor"}), 500

# ==================== MAIN ====================
if __name__ == "__main__":
    logger.info("Iniciando aplicación Flask")
    app.run(debug=True, host='127.0.0.1', port=5000)
