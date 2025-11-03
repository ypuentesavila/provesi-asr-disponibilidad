import requests, time, mysql.connector

ERP_URL = "http://<IP_ERP>:5000/sync"  # Reemplaza con la IP pública del ERP
DB_CONFIG = {
    "host": "<IP_DB>",  # o 'localhost' si usas MySQL local
    "user": "admin",
    "password": "admin123",
    "database": "wms"
}

def enviar_pedido(pedido_id):
    inicio = time.time()
    try:
        r = requests.get(ERP_URL, timeout=0.9)
        duracion = time.time() - inicio
        print(f"[OK] ERP respondió en {round(duracion,3)} s: {r.json()}")
    except Exception as e:
        print(f"[FALLO] ERP no respondió en 0.9 s. Guardando pedido {pedido_id} en cola local...")
        guardar_en_cola(pedido_id)

def guardar_en_cola(pedido_id):
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()
    cursor.execute("INSERT INTO cola_local (pedido_id, estado) VALUES (%s, %s)", (pedido_id, 'pendiente'))
    db.commit()
    db.close()

if __name__ == "__main__":
    while True:
        pedido_id = int(time.time())  # ID único
        enviar_pedido(pedido_id)
        time.sleep(3)
