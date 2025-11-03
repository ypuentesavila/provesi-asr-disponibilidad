import time
import requests
import mysql.connector

ERP_URL = "http://54.226.55.197:5000/sync"

DB_CONFIG = {
    "host": "wms-db.cn2k0uwymh6v.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "admin1234...",
    "database": "wms-db"
}

def procesar_cola():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        cursor.execute("SELECT * FROM cola_local WHERE estado='pendiente'")
        pedidos = cursor.fetchall()

        for pedido in pedidos:
            try:
                r = requests.get(ERP_URL, timeout=0.9)
                if r.status_code == 200:
                    cursor.execute("DELETE FROM cola_local WHERE id=%s", (pedido["id"],))
                    db.commit()
                    print(f"[WORKER] Pedido {pedido['id']} reenviado con éxito.")
                else:
                    print(f"[WORKER] ERP respondió error para pedido {pedido['id']}.")
            except Exception as e:
                print(f"[WORKER] ERP sigue caído, reintentará más tarde.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Fallo de conexión a la base de datos: {err}")
    finally:
        if 'db' in locals() and db.is_connected():
            db.close()

if __name__ == "__main__":
    print("[WORKER] Iniciando proceso de sincronización...")
    while True:
        procesar_cola()
        time.sleep(5)
