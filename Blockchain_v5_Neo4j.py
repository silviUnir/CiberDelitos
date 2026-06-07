# Blockchain_v5.py

# Programa que solicita una transacción y muestra Input y Output
# https://mempool.space/api/tx/<txid>
# Versión 4 : Recursiva, buscando en el historial de cada monedero origen y destino.(ANALIZADOR FORENSE REAL)
# Version 5 : Integracion Neo4j para almacenar el grafo de transacciones y relaciones entre wallets.

#Dada una transacción 0:
#- Para cada input, obtenemos la dirección origen (monedero origen).
#Luego buscamos todas las transacciones anteriores donde esa dirección aparece como output.
#Eso nos permite ir hacia atrás.
#- Para cada output, obtenemos la dirección destino (monedero destino).
#Luego buscamos todas las transacciones posteriores donde esa dirección aparece como input.
#Eso nos permite ir hacia adelante.

# Transacción ejemplo para pruebas
# txid = 9af60e0adab4aff07cf14451a3071f74d841ed42c200853e624c25fe155590e5


import requests
import time
import os
from neo4j import GraphDatabase
import sys

# DEFINICIÓN DE VARIABLES
# Nombre instancia analisisBlockchain
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASS")

if not NEO4J_PASS:
    print("[ERROR] Variable de entorno NEO4J_PASS no definida.")
    print("  Ejecuta: export NEO4J_PASS='tu_contraseña' antes de lanzar el script.")
    sys.exit(1)

driver = None

API_TIMEOUT = 15
API_MAX_RETRIES = 3
API_RETRY_DELAY = 2

def connect_neo4j():
# CONEXIÓN ROBUSTA A NEO4J
    global driver
    print("[INFO] Intentando conectar con Neo4j...")

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

        # Probar conexión real
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            value = result.single()["test"]

        if value == 1:
            print("[OK] Conexión establecida correctamente con Neo4j.")
        else:
            print("[ERROR] Neo4j respondió, pero el resultado no es válido.")
            sys.exit(1)

    except Exception as e:
        print("\n[ERROR] No se pudo conectar a Neo4j.")
        print("Causas más comunes:")
        print("  1) Neo4j Desktop no está arrancado")
        print("  2) El puerto Bolt (7687) está cerrado o cambiado")
        print("  3) Contraseña incorrecta")
        print("  4) Firewall bloqueando la conexión\n")
        print("Detalle técnico del error:")
        print(e)
        sys.exit(1)




def close_neo4j():
    if driver:
        driver.close()
        print("[INFO] Conexión con Neo4j cerrada.")



BASE = "https://mempool.space/api"


def _api_request(url, default=None):
    """Petición GET con timeout, reintentos y manejo de rate-limit (429)."""
    delay = API_RETRY_DELAY
    for intento in range(1, API_MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=API_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", delay))
                print(f"[WARN] Rate-limit (429). Reintentando en {retry_after}s (intento {intento}/{API_MAX_RETRIES})")
                time.sleep(retry_after)
                delay *= 2
                continue
            if r.status_code >= 500:
                print(f"[WARN] Error servidor ({r.status_code}). Reintentando en {delay}s (intento {intento}/{API_MAX_RETRIES})")
                time.sleep(delay)
                delay *= 2
                continue
            print(f"[WARN] HTTP {r.status_code} en {url}")
            return default
        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout. Reintentando en {delay}s (intento {intento}/{API_MAX_RETRIES})")
            time.sleep(delay)
            delay *= 2
        except requests.exceptions.RequestException as e:
            print(f"[WARN] Error de red: {e}. Reintentando en {delay}s (intento {intento}/{API_MAX_RETRIES})")
            time.sleep(delay)
            delay *= 2
    print(f"[ERROR] No se pudo obtener {url} tras {API_MAX_RETRIES} intentos.")
    return default


def get_tx(txid):
    # Dado un txid, obtenemos el JSON de la transacción desde la API de mempool.space
    return _api_request(f"{BASE}/tx/{txid}")


def get_address_txs(address):
    # Dada una dirección, obtenemos todas las transacciones asociadas a esa dirección desde la API de mempool.space
    return _api_request(f"{BASE}/address/{address}/txs", default=[])



# ============================
#   BACKWARD TRACING
# ============================

def trace_backward(address, visited=None, depth=0, max_depth=5):
    # Función recursiva para seguir hacia atrás las transacciones a partir de una dirección dada.

    if visited is None:
        visited = set()

    if depth > max_depth:
        return

    print("  " * depth + f"[BACKWARD] Buscando transacciones donde {address} recibe dinero...")

    txs = get_address_txs(address)

    for tx in txs:
        txid = tx["txid"]

        # Evitar bucles
        if txid in visited:
            continue
        visited.add(txid)

        # Guardar address en Neo4j
        upsert_tx_and_relations(tx)


        # Buscar outputs donde esta address recibe
        for o in tx.get("vout", []):
            if o.get("scriptpubkey_address") == address:
                print("  " * depth + f" ← Recibió en TX: {txid} . Wallet : {address}")

                # Ahora seguimos hacia atrás con los inputs de esa transacción
                for i in tx.get("vin", []):
                    prev = i.get("prevout", {})
                    prev_addr = prev.get("scriptpubkey_address")
                    if prev_addr:
                        trace_backward(prev_addr, visited, depth + 1, max_depth)





# ============================
#   FORWARD TRACING
# ============================

def trace_forward(address, visited=None, depth=0, max_depth=5):
# Función recursiva para seguir hacia adelante las transacciones a partir de una dirección dada.
    if visited is None:
        visited = set()

    if depth > max_depth:
        return

    print("  " * depth + f"[FORWARD] Buscando transacciones donde {address} envía dinero...")

    txs = get_address_txs(address)

    for tx in txs:
        txid = tx["txid"]

        if txid in visited:
            continue
        visited.add(txid)

        # Guardar address en Neo4j
        upsert_tx_and_relations(tx)
        
        # Buscar inputs donde esta address envía
        for i in tx.get("vin", []):
            prev = i.get("prevout", {})
            prev_addr = prev.get("scriptpubkey_address")
            if prev_addr == address:
                print("  " * depth + f" → Envió en TX: {txid}")

                # Ahora seguimos hacia adelante con los outputs
                for o in tx.get("vout", []):
                    next_addr = o.get("scriptpubkey_address")
                    if next_addr:
                        trace_forward(next_addr, visited, depth + 1, max_depth)





def upsert_tx_and_relations(tx_json):
    # Dado el JSON de una transacción, creamos nodos y relaciones en Neo4j
    """ 
    Inserta/actualiza en Neo4j:
      - nodo (:Tx {txid})
      - nodos (:Address {address})
      - relaciones (:Address)-[:INPUT_OF]->(:Tx)
      - relaciones (:Tx)-[:OUTPUT_TO {value}]->(:Address)
    """

    txid = tx_json["txid"]
    vout = tx_json.get("vout", [])
    vin  = tx_json.get("vin", [])

    with driver.session() as session:
        # Crear nodo de transacción
        session.run("""
            MERGE (t:Tx {txid: $txid})
        """, txid=txid)

        # INPUTS: Address -> Tx
        for i in vin:
            prev = i.get("prevout", {})
            addr = prev.get("scriptpubkey_address")
            if not addr:
                continue
            session.run("""
                MERGE (a:Address {address: $addr})
                WITH a
                MATCH (t:Tx {txid: $txid})
                MERGE (a)-[:INPUT_OF]->(t)
            """, addr=addr, txid=txid)

        # OUTPUTS: Tx -> Address
        for o in vout:
            addr = o.get("scriptpubkey_address")
            value = o.get("value", 0)
            if not addr:
                continue
            session.run("""
                MERGE (a:Address {address: $addr})
                WITH a
                MATCH (t:Tx {txid: $txid})
                MERGE (t)-[r:OUTPUT_TO {value: $value}]->(a)
            """, addr=addr, txid=txid, value=value)











# ============================
#   PROGRAMA PRINCIPAL
# ============================

if __name__ == "__main__":
    connect_neo4j()   

    try:
        txid = input("Introduce TXID inicial: ").strip()
        tx = get_tx(txid)

        if not tx:
            print("Transacción no encontrada.")
            close_neo4j()
            exit()

        upsert_tx_and_relations(tx)

        print("\n=== INPUTS (origen) ===")
        for i in tx.get("vin", []):
            addr = i.get("prevout", {}).get("scriptpubkey_address")
            if addr:
                print(f"Origen: {addr}")
                trace_backward(addr)

        print("\n=== OUTPUTS (destino) ===")
        for o in tx.get("vout", []):
            addr = o.get("scriptpubkey_address")
            if addr:
                print(f"Destino: {addr}")
                trace_forward(addr)

    finally:
        close_neo4j()