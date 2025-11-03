import requests
import time
import pymysql

ERP_URL = "http://54.226.55.197:5000/sync"

DB_CONFIG = {
    "host": "wms-db.cn2k0uwymh6v.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "admin1234...",
    "database": "wms-db",
    "connect_timeout": 5,
    "ssl": {"ssl": False}  # fuerza sin SSL
}

def enviar_pedido(pedido_id):
    inicio = time.time()
    try:
        r = requests.get(ERP_URL, timeout=0.9)
        duracion = time.time() - inicio
        print(f"[OK] ERP respondió en {round(duracion,3)} s → {r.status_code}")

        if r.status_code == 200:
            print(f"[WMS] Pedido {pedido_id} procesado correctamente en el ERP.")
        else:
            print(f"[ERROR] ERP devolvió {r.status_code}. Encolando pedido {pedido_id}.")
            guardar_en_cola(pedido_id)

    except Exception as e:
        duracion = time.time() - inicio
        print(f"[FALLO] ERP no respondió en 0.9 s ({round(duracion,3)} s). Encolando pedido {pedido_id}...")
        guardar_en_cola(pedido_id)


def guardar_en_cola(pedido_id):
    try:
        db = pymysql.connect(**DB_CONFIG)
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO cola_local (pedido_id, estado) VALUES (%s, %s)",
            (pedido_id, 'pendiente')
        )
        db.commit()
        db.close()
        print(f"[QUEUE] Pedido {pedido_id} guardado en cola_local correctamente.")
    except Exception as e:
        print(f"[DB ERROR] No se pudo guardar el pedido {pedido_id}: {e}")


if __name__ == "__main__":
    print("=== WMS Producer iniciado ===")
    print("Enviando pedidos al ERP cada 3 segundos...\n")
    while True:
        pedido_id = int(time.time())
        enviar_pedido(pedido_id)
        time.sleep(3)
