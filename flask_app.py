from flask import Flask, request, jsonify, send_from_directory
import sqlite3
from datetime import datetime
import os
import pyodbc



DB_PATH = "/home/cfbayolo/mysite/alta.db"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

def normalizar_rut(rut):
    return rut.replace(".", "").strip().lower()

def get_db():
    return sqlite3.connect(DB_PATH)

def init_db():
    db = get_db()
    cur = db.cursor()

    cur.execute("""
CREATE TABLE IF NOT EXISTS solicitud (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fechaingreso TEXT,        -- sin guión bajo
  rutcliente TEXT,
  cliente TEXT,
  nrosam TEXT,
  razonsocial TEXT,
  ejecutivocomercial TEXT,
  fonoejecutivo TEXT,
  contactocliente TEXT,
  fonocontactocliente TEXT,
  contactotecnico TEXT,
  fonocontactotecnico TEXT,
  jefeproyecto TEXT,
  fonojefeproyecto TEXT,
  proyecto TEXT,
  pepgasto TEXT,
  proveedor TEXT,
  actividad TEXT,
  tipodireccion TEXT,
  conceptootroscostos TEXT,
  monedaotros_costos TEXT,  -- sin guión
  montootros_costos REAL,
  monedainstalacion TEXT,
  costoinstalacion REAL,
  monedarenta TEXT,
  valorrenta REAL,
  plazomeses INTEGER        -- sin guión
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS direccion (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  solicitudid INTEGER,      -- sin guión bajo
  numero INTEGER,
  direccion TEXT,
  servicio TEXT,
  capacidad TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS sync_estado (
  solicitud_id INTEGER PRIMARY KEY,
  estado_sync TEXT
)
""")

db.commit()
db.close()

# Inicializa BBDD
init_db()

@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")

@app.route("/init", methods=["GET"])
def init_form():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT IFNULL(MAX(id),0)+1 FROM solicitud")
    nro = cur.fetchone()[0]
    db.close()
    return jsonify({
        "solicitudNro": nro,
        "fechaIngreso": datetime.now().strftime("%d-%m-%Y")
    })


SQL_CONN_STR = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=NOTDELL191114\SQLEXPRESS;'  # o localhost
    'DATABASE=SolicitudAlta;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)


@app.route('/guardarsolicitud', methods=['POST'])
def guardar():
    data = request.json

    # SQLite (buffer)
    db_sqlite = get_db()
    cur_sqlite = db_sqlite.cursor()
    cur_sqlite.execute("""
        INSERT INTO solicitud (fechaingreso, rutcliente, cliente, nrosam, razonsocial, ejecutivocomercial,
                               fonoejecutivo, contactocliente, fonocontactocliente, contactotecnico,
                               fonocontactotecnico, jefeproyecto, fonojefeproyecto, proyecto, pepgasto,
                               proveedor, actividad, tipodireccion, conceptootroscostos, monedaotros_costos,
                               montootros_costos, monedainstalacion, costoinstalacion, monedarenta,
                               valorrenta, plazomeses)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data['fechaIngreso'], data['rutCliente'], data['cliente'], data['nroSAM'], data['razonSocial'],
        data['ejecutivoComercial'], data['fonoEjecutivo'], data['contactoCliente'], data['fonoContactoCliente'],
        data['contactoTecnico'], data['fonoContactoTecnico'], data['jefeProyecto'], data['fonoJefeProyecto'],
        data['proyecto'], data['pepGasto'], data['proveedor'], data['actividad'], data['tipoDireccion'],
        data['conceptoOtrosCostos'], data['monedaOtrosCostos'], float(data.get('montoOtrosCostos', 0)),
        data['monedaInstalacion'], float(data.get('costoInstalacion', 0)), data['monedaRenta'],
        float(data.get('valorRenta', 0)), int(data['plazoMeses'])
    ))
    sqlite_id = cur_sqlite.lastrowid

    direcciones = data.get('direcciones', [])
    for d in direcciones:
        cur_sqlite.execute("""
            INSERT INTO direccion (solicitudid, numero, direccion, servicio, capacidad)
            VALUES (?, ?, ?, ?, ?)
        """, (sqlite_id, d['numero'], d['direccion'], d['servicio'], d['capacidad']))

    db_sqlite.commit()
    db_sqlite.close()

    sql_sync = "pendiente"

    # SQL Server directo
    try:        
    conn_sql = pyodbc.connect(SQL_CONN_STR)
    cur_sql = conn_sql.cursor()
    cur_sql.execute("""
            INSERT INTO solicitud (fechaingreso, rutcliente, cliente, nrosam, razonsocial, ejecutivocomercial,
                                   fonoejecutivo, contactocliente, fonocontactocliente, contactotecnico,
                                   fonocontactotecnico, jefeproyecto, fonojefeproyecto, proyecto, pepgasto,
                                   proveedor, actividad, tipodireccion, conceptootroscostos, monedaotros_costos,
                                   montootros_costos, monedainstalacion, costoinstalacion, monedarenta,
                                   valorrenta, plazomeses)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['fechaIngreso'], data['rutCliente'], data['cliente'], data['nroSAM'], data['razonSocial'],
            data['ejecutivoComercial'], data['fonoEjecutivo'], data['contactoCliente'], data['fonoContactoCliente'],
            data['contactoTecnico'], data['fonoContactoTecnico'], data['jefeProyecto'], data['fonoJefeProyecto'],
            data['proyecto'], data['pepGasto'], data['proveedor'], data['actividad'], data['tipoDireccion'],
            data['conceptoOtrosCostos'], data['monedaOtrosCostos'], float(data.get('montoOtrosCostos', 0)),
            data['monedaInstalacion'], float(data.get('costoInstalacion', 0)), data['monedaRenta'],
            float(data.get('valorRenta', 0)), int(data['plazoMeses'])
        ))
        cur_sql.execute("SELECT @@IDENTITY")
        sql_id = cur_sql.fetchone()[0]

        for d in direcciones:
            cur_sql.execute("""
                INSERT INTO direccion (solicitudid, numero, direccion, servicio, capacidad)
                VALUES (?, ?, ?, ?, ?)
            """, (sql_id, d['numero'], d['direccion'], d['servicio'], d['capacidad']))

        conn_sql.commit()
        conn_sql.close()
        sql_sync = f"OK (ID: {sql_id})"
        print(f"Guardado SQL ID: {sql_id}")
    except Exception as e:
        sql_sync = f"Error: {str(e)}"
        print(f"Error SQL: {e}")

    return jsonify({
        "status": "OK",
        "sqlite_id": sqlite_id,
        "sql_server_sync": sql_sync
    })

@app.route("/buscar_cliente")
def buscar_cliente():
    rut = normalizar_rut(request.args.get("rut", ""))
    if len(rut) < 8:
        return jsonify({"cliente": ""})
    
    import requests
    url = "https://drive.google.com/uc?export=download&id=10EUZK61nkiZ90IbNOYLjAtnWKz-9IKPx"
    
    try:
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        for linea in resp.iter_lines(decode_unicode=True):
            linea = linea.decode('utf-8').strip()
            if ';' in linea and not linea.startswith('RUT_Cliente'):
                rut_txt, cliente = linea.split(';', 1)
                if normalizar_rut(rut_txt.strip()) == rut:
                    return jsonify({"cliente": cliente.strip()})
    except Exception as e:
        print(f"Error clientes: {e}")
    
    return jsonify({"cliente": ""})

@app.route("/api/obtener_pendientes", methods=["GET"])
def obtener_pendientes():
    db = get_db()
    cur = db.cursor()
cur.execute("""
    SELECT * FROM solicitud s
    LEFT JOIN sync_estado se ON s.id = se.solicitud_id
    WHERE se.estado_sync IS NULL OR se.estado_sync != 'SINCRONIZADO'
    ORDER BY s.id DESC
""")
    solicitudes = []
    columnas = [desc[0] for desc in cur.description]
    for row in cur.fetchall():
        sol = dict(zip(columnas, row))
        cur.execute("SELECT * FROM direccion WHERE solicitudid = ?", (sol['id'],))
        dir_cols = [desc[0] for desc in cur.description]
        sol['direcciones'] = [dict(zip(dir_cols, row)) for row in cur.fetchall()]
        solicitudes.append(sol)
    db.close()
    return jsonify(solicitudes)

@app.route("/api/marcar_sincronizado", methods=["POST"])
def marcar_sincronizado():
    data = request.json
    db = get_db()
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO sync_estado (solicitud_id, estado_sync) VALUES (?, 'SINCRONIZADO')",
                (data['solicitud_id'],))
    db.commit()
    db.close()
    return jsonify({"status": "OK"})

if __name__ == "__main__":
    app.run(debug=True)



