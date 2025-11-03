import time
import requests
import pymysql

ERP_URL = "http://54.226.55.197:5000/sync"

DB_CONFIG = {
    "host": "wms-db.cn2k0uwymh6v.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "admin1234...",
    "database": "wms-db",
    "connect_timeout": 5
}

def procesar_cola():
    try:
        db = pymysql.connect(**DB_CONFIG)
        cursor = db.cursor(pymysql.cursors.DictCursor)

        cursor.execute("SELECT * FROM cola_local WHERE estado='pendiente'")
        pedidos = cursor.fetchall()

        if not pedidos:
            print("[WORKER] No hay pedidos pendientes.")
            return

        for pedido in pedidos:
            try:
                r = requests.get(ERP_URL, timeout=0.9)
                if r.status_code == 200:
                    cursor.execute("DELETE FROM cola_local WHERE id=%s", (pedido["id"],))
                    db.commit()
                    print(f"[WORKER] Pedido {pedido['id']} reenviado con éxito.")
                else:
                    print(f"[WORKER] ERP respondió error para pedido {pedido['id']}.")
            except Exception:
                print(f"[WORKER] ERP sigue caído, reintentará más tarde.")
    except Exception as e:
        print(f"[DB ERROR] No se pudo conectar a la base de datos: {e}")
    finally:
        try:
            db.close()
        except:
            pass


if __name__ == "__main__":
    print("[WORKER] Iniciando proceso de sincronización con ERP...")
    while True:
        procesar_cola()
        time.sleep(5)
