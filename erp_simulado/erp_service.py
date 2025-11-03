from flask import Flask, jsonify
import random, time

app = Flask(__name__)

@app.route("/sync")
def sync():
    falla = random.choice([True, False, False]) 
    if falla:
        print("[ERP] Simulando falla o demora...")
        time.sleep(2)  
        return jsonify({"status": "error", "msg": "ERP no disponible"}), 500
    else:
        print("[ERP] Procesó pedido correctamente.")
        return jsonify({"status": "ok", "msg": "Pedido sincronizado"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
