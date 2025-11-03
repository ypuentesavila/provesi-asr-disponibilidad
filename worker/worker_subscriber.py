import time, requests, mysql.connector

ERP_URL = "http://<IP_ERP>:5000/sync"
DB_CONFIG = {
    "host": "<IP_DB>",
    "user": "admin",
    "password": "admin123",
    "database": "wms"
}

def procesar_cola():
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM cola_local WHERE estado='pendiente'")
    pedidos = cursor.fetchall()
    for pedido in pedidos:
        try:
            r = requests.get(ERP_URL, timeout=1)
            if r.status_code == 200:
                cursor.execute("DELETE FROM cola_local WHERE id=%s", (pedido["id"],))
                db.commit()
                print(f"[WORKER] Pedido {pedido['id']} reenviado con éxito.")
            else:
                print(f"[WORKER] ERP respondió error para {pedido['id']}.")
        except Exception as e:
            print("[WORKER] ERP sigue caído, reintentará más tarde.")
    db.close()

if __name__ == "__main__":
    while True:
        procesar_cola()
        time.sleep(5)
