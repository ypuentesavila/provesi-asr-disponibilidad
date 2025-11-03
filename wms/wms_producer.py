import requests
import time
import mysql.connector

ERP_URL = "http://54.226.55.197:5000/sync"
DB_CONFIG = {
    "host": "wms-db.cn2k0uwymh6v.us-east-1.rds.amazonaws.com",
    "user": "admin",
    "password": "admin1234...",
    "database": "wms-db",
    "ssl_disabled": True 
}

def enviar_pedido(pedido_id):
    """Envía un pedido al ERP. Si falla o se demora >0.9 s, lo guarda en la cola local."""
    inicio = time.time()
    try:
        r = requests.get(ERP_URL, timeout=0.9)
        duracion = time.time() - inicio
        print(f"[OK] ERP respondió en {round(duracion,3)} s → {r.status_code}")

        if r.status_code == 200:
            print(f"[WMS] Pedido {pedido_id} procesado correctamente en el ERP.")
        else:
            print(f"[ERROR] ERP devolvió estado {r.status_code}, encolando pedido {pedido_id}.")
            guardar_en_cola(pedido_id)

    except Exception as e:
        duracion = time.time() - inicio
        print(f"[FALLO] ERP no respondió en 0.9 s ({round(duracion,3)} s). Encolando pedido {pedido_id}...")
        guardar_en_cola(pedido_id)

def guardar_en_cola(pedido_id):
    """Guarda un pedido en la base de datos local (cola_local) cuando el ERP no responde."""
    try:
        db = mysql.connector.connect(**DB_CONFIG)
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
