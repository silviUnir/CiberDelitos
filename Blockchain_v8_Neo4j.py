# Blockchain_v5.py

# Programa que solicita una transacción y muestra Input y Output
# https://mempool.space/api/tx/<txid>
# Versión 4 : Recursiva, buscando en el historial de cada monedero origen y destino.(ANALIZADOR FORENSE REAL)
# Version 5 : Integracion Neo4j para almacenar el grafo de transacciones y relaciones entre wallets.
# Version 6 : Introduce csv recursivo, para almacenar en un csv el historial de transacciones, con sus inputs y outputs, para cada wallet analizada. (ANALIZADOR FORENSE REAL + EXPORTACION DE DATOS PARA ANALISIS EXTERNO)
# Version 7 : Introduce detección de patrones forenses (OP_RETURN, cambio interno, peeling chain, consolidación, batching) y los etiqueta en Neo4j. (ANALIZADOR FORENSE REAL + EXPORTACION DE DATOS PARA ANALISIS EXTERNO + DETECCIÓN DE PATRONES FORENSES)
# Version 8 : Corrección backward tracing para incluir transacciones previas en las que la dirección aparece como output, y no solo como input. (ANALIZADOR FORENSE REAL + EXPORTACION DE DATOS PARA ANALISIS EXTERNO + DETECCIÓN DE PATRONES FORENSES + BACKWARD TRACING COMPLETO) 

#Dada una transacción 0:
#- Para cada input, obtenemos la dirección origen (monedero origen).
#Luego buscamos todas las transacciones anteriores en el tiempo donde esa dirección aparece como output.
#Eso nos permite ir hacia atrás.
#- Para cada output, obtenemos la dirección destino (monedero destino).
#Luego buscamos todas las transacciones posteriores en el tiempo donde esa dirección aparece como input.
#Eso nos permite ir hacia adelante.

# fecha_tx_actual = fecha de la transacción que estamos evaluando en esta iteración
# backward: buscamos transacciones con fecha < fecha_tx_actual
# forward: buscamos transacciones con fecha > fecha_tx_actual

# Transacción ejemplo para pruebas
# txid1 = 9af60e0adab4aff07cf14451a3071f74d841ed42c200853e624c25fe155590e5
# txid2 mismo grafo txid1 = 428bd0e9f6e3bbe881fbec5bf3af387d1afc4c6a4c77662b69aa74f3e1014e93


import requests
print(requests.__version__)
from neo4j import GraphDatabase
import sys
import csv
from datetime import datetime, timezone
import os
from analisisForense import analizar_csv




# DEFINICIÓN DE VARIABLES
# Nombre instancia analisisBlockchain
NEO4J_URI = "bolt://localhost:7687"
#neo4j://127.0.0.1:7687
NEO4J_USER = "neo4j"
NEO4J_PASS = "SRFParaku26!"   # <-- Ver para encriptarla

driver = None
csv_registrados = set()  # Para evitar duplicados en el CSV



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
            print("✅ 100 - Conexión establecida correctamente con Neo4j.")
        else:
            print("❌ 200 - Neo4j respondió, pero el resultado no es válido.")
            sys.exit(1)

    except Exception as e:
        print("\n❌ 200 - No se pudo conectar a Neo4j.")
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
        print("[INFO] ✅ 100 - Conexión con Neo4j cerrada.")



BASE = "https://mempool.space/api"




def get_tx(txid):
    # Dado un txid, obtenemos el JSON de la transacción desde la API de mempool.space
    r = requests.get(f"{BASE}/tx/{txid}?include_prevout=true")
    if r.status_code != 200:
        return None
    return r.json()



def get_address_txs(address):
    # Dada una dirección, obtenemos todas las transacciones asociadas a esa dirección desde la API de mempool.space
    r = requests.get(f"{BASE}/address/{address}/txs")
    if r.status_code != 200:
        return []
    return r.json()



# ============================
#   BACKWARD TRACING
# ============================

def trace_backward(tx_actual, visitadas=None, profundidad=0, max_profundidad=10):
    if visitadas is None:
        visitadas = set()

    if not isinstance(tx_actual, dict) or "txid" not in tx_actual:
        print("ERROR: trace_backward() recibió un valor no válido.")
        return

    txid_actual = tx_actual["txid"]

    # Evitar ciclos
    if txid_actual in visitadas:
        return
    visitadas.add(txid_actual)

    fecha_tx_actual = tx_actual["status"]["block_time"]

    print(f"\n[BACKWARD] Analizando tx {txid_actual} (fecha {fecha_tx_actual})")

    if profundidad > max_profundidad:
        print("Máxima profundidad alcanzada en backward.")
        return

    for vin in tx_actual.get("vin", []):
        # Tu API SIEMPRE tiene vin.txid
        prev_txid = vin.get("txid")
        direccion_input = vin.get("prevout", {}).get("scriptpubkey_address")

        print(f"  → Retrocediendo: input {direccion_input} viene de {prev_txid}")

        if not prev_txid:
            continue

        tx_previa = get_tx(prev_txid)
        if not tx_previa:
            print("    No se pudo obtener la transacción previa.")
            continue

        print(f"    ✔ Transacción previa encontrada: {prev_txid}")

        guardar_csv_recursivo(tx_previa, direccion_input)
        upsert_tx_and_relations(tx_previa)

        trace_backward(tx_previa, visitadas, profundidad + 1, max_profundidad)




# ============================
#   FORWARD TRACING
# ============================

def trace_forward(tx_actual, visitadas=None, profundidad=0, max_profundidad=10):
    if visitadas is None:
        visitadas = set()

    if not isinstance(tx_actual, dict) or "txid" not in tx_actual:
        print("ERROR: trace_forward() recibió un valor no válido.")
        return

    txid_actual = tx_actual["txid"]

    # Evitar ciclos
    if txid_actual in visitadas:
        return
    visitadas.add(txid_actual)

    fecha_tx_actual = tx_actual["status"]["block_time"]

    print(f"\n[FORWARD] Analizando tx {txid_actual} (fecha {fecha_tx_actual})")

    if profundidad > max_profundidad:
        print("Máxima profundidad alcanzada en forward.")
        return

    for vout in tx_actual.get("vout", []):
        direccion_output = vout.get("scriptpubkey_address")
        if not direccion_output:
            continue

        print(f"  → Buscando transacciones posteriores donde {direccion_output} es INPUT")

        txs_direccion = get_address_txs(direccion_output)
        if not txs_direccion:
            continue

        txs_posteriores = []

        for tx in txs_direccion:
            for vin in tx.get("vin", []):
                # Caso API con prevout
                if "prevout" in vin and vin["prevout"]:
                    addr_in = vin["prevout"].get("scriptpubkey_address")
                else:
                    addr_in = vin.get("scriptpubkey_address")

                if addr_in == direccion_output and tx["status"]["block_time"] > fecha_tx_actual:
                    txs_posteriores.append(tx)
                    break

        if not txs_posteriores:
            continue

        txs_posteriores.sort(key=lambda t: t["status"]["block_time"])

        for tx_posterior in txs_posteriores:
            print(f"    ✔ Posterior: {tx_posterior['txid']}")

            guardar_csv_recursivo(tx_posterior, direccion_output)
            upsert_tx_and_relations(tx_posterior)

            trace_forward(tx_posterior, visitadas, profundidad + 1, max_profundidad)





def upsert_tx_and_relations(tx_json):
    """
    Inserta/actualiza en Neo4j:
      - nodo (:Tx {txid})
      - nodos (:Address {address})
      - nodos (:Output {id})
      - relaciones (:Address)-[:INPUT_OF]->(:Tx)
      - relaciones (:Tx)-[:HAS_OUTPUT]->(:Output)-[:TO_ADDRESS]->(:Address)
      - etiquetas forenses en Tx y Output
    Guarda en CSV cada combinación input-output de la transacción.
    """

    # Guardar datos en CSV recursivo
    guardar_csv_recursivo(tx_json)
    
    txid = tx_json["txid"]
    vin  = tx_json.get("vin", [])
    vout = tx_json.get("vout", [])

    # === PREPARAR DATOS PARA DETECCIÓN DE PATRONES ===
    input_addresses = {
        i.get("prevout", {}).get("scriptpubkey_address")
        for i in vin
        if i.get("prevout", {}).get("scriptpubkey_address")
    }

    output_addresses = {
        o.get("scriptpubkey_address")
        for o in vout
        if o.get("scriptpubkey_address")
    }

    num_inputs = len(input_addresses)
    num_outputs = len(vout)

    op_return_present = any(o.get("scriptpubkey_address") is None for o in vout)
    cambio_interno = len(input_addresses & output_addresses) > 0
    peeling_chain = (num_inputs == 1 and num_outputs == 2 and cambio_interno)
    consolidacion = (num_inputs > 1 and num_outputs == 1)
    batching = (num_inputs == 1 and num_outputs > 2)

    # === INSERTAR EN NEO4J ===
    with driver.session() as session:

        # Crear nodo de transacción
        session.run("""
            MERGE (t:Tx {txid: $txid})
        """, txid=txid)

        # === INPUTS ===
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

        # === OUTPUTS ===
        for idx, o in enumerate(vout):
            addr = o.get("scriptpubkey_address")
            value = o.get("value", 0)

            output_id = f"{txid}_out_{idx}"

            # Crear nodo Output
            session.run("""
                MERGE (o:Output {id: $oid})
                SET o.value = $value
            """, oid=output_id, value=value)

            # === OP_RETURN ===
            if addr is None:
                session.run("""
                    MATCH (o:Output {id: $oid})
                    SET o:OP_RETURN
                """, oid=output_id)
                continue

            # Crear Address
            session.run("""
                MERGE (a:Address {address: $addr})
            """, addr=addr)

            # Relación Tx → Output → Address
            session.run("""
                MATCH (t:Tx {txid: $txid})
                MATCH (o:Output {id: $oid})
                MATCH (a:Address {address: $addr})
                MERGE (t)-[:HAS_OUTPUT]->(o)
                MERGE (o)-[:TO_ADDRESS]->(a)
            """, txid=txid, oid=output_id, addr=addr)

            # === CAMBIO INTERNO ===
            if addr in input_addresses:
                session.run("""
                    MATCH (o:Output {id: $oid})
                    SET o:Cambio
                """, oid=output_id)

        # === ETIQUETAS EN LA TRANSACCIÓN ===
        if op_return_present:
            session.run("""
                MATCH (t:Tx {txid: $txid})
                SET t:OP_RETURN_TX
            """, txid=txid)

        if cambio_interno:
            session.run("""
                MATCH (t:Tx {txid: $txid})
                SET t:Cambio
            """, txid=txid)

        if peeling_chain:
            session.run("""
                MATCH (t:Tx {txid: $txid})
                SET t:PeelingChain
            """, txid=txid)

        if consolidacion:
            session.run("""
                MATCH (t:Tx {txid: $txid})
                SET t:Consolidacion
            """, txid=txid)

        if batching:
            session.run("""
                MATCH (t:Tx {txid: $txid})
                SET t:Batching
            """, txid=txid)







def guardar_csv_recursivo(tx_json, dirección=None, ruta_csv="transacciones.csv"):
    txid = tx_json["txid"]

    # === FECHA REAL DE LA TRANSACCIÓN ===
    block_time = tx_json.get("status", {}).get("block_time")
    if block_time:
        fecha_tx = datetime.fromtimestamp(block_time, tz=timezone.utc).isoformat()
    else:
        fecha_tx = "UNCONFIRMED"

    # === INPUTS ===
    inputs = []
    for i in tx_json.get("vin", []):
        prev = i.get("prevout", {})
        addr = prev.get("scriptpubkey_address")
        value = prev.get("value", 0)
        if addr:
            inputs.append((addr, value))
    if not inputs:
        inputs = [("COINBASE", 0)]

    # === OUTPUTS ===
    outputs = []
    for o in tx_json.get("vout", []):
        addr = o.get("scriptpubkey_address")
        value = o.get("value", 0)
        if addr:
            outputs.append((addr, value))
        else:
            outputs.append(("OP_RETURN", value))

    # === CSV HEADER ===
    encabezado = [
        "txid",
        "fecha_transaccion_utc",
        "input_address",
        "cantidad_input_btc",
        "output_address",
        "cantidad_output_btc"
    ]

    # === CHECK IF FILE EXISTS ===
    archivo_existe = False
    try:
        with open(ruta_csv, "r", encoding="utf-8") as f:
            archivo_existe = True
    except FileNotFoundError:
        archivo_existe = False

    # === WRITE ROWS ===
    with open(ruta_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if not archivo_existe:
            writer.writerow(encabezado)

        for out_addr, out_value in outputs:
            for in_addr, in_value in inputs:
                writer.writerow([
                    txid,
                    fecha_tx,
                    in_addr,
                    in_value / 100_000_000,   # satoshis → BTC
                    out_addr,
                    out_value / 100_000_000   # satoshis → BTC
                ])

    print(f"[CSV] Registrada TX {txid} con {len(outputs)} outputs")



def eliminar_csv_recursivo():
    # Eliminar CSV antes de cada ejecución
    csv_path = "transacciones.csv"
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print("[INFO] CSV anterior eliminado.")
    else:
        print("[INFO] No existe CSV previo, se creará uno nuevo.")




# ============================
#   PROGRAMA PRINCIPAL
# ============================

if __name__ == "__main__":
    eliminar_csv_recursivo()
    connect_neo4j()

    try:
        txid = input("Introduce TXID inicial: ").strip()
        tx = get_tx(txid)

        if not tx:
            print("Transacción no encontrada.")
            close_neo4j()
            exit()

        # Guardamos la transacción inicial
        #guardar_csv_recursivo(tx)
        upsert_tx_and_relations(tx)

        print("\n=== INPUTS (origen) ===")
        for i in tx.get("vin", []):
            addr = i.get("prevout", {}).get("scriptpubkey_address")
            if addr:
                print(f"Origen: {addr}")

        # Backward
        trace_backward(tx)

        print("\n=== OUTPUTS (destino) ===")
        for o in tx.get("vout", []):
            addr = o.get("scriptpubkey_address")
            if addr:
                print(f"Destino: {addr}")

        # Forward
       # trace_forward(tx)

        analizar_csv("transacciones.csv", "transacciones_analizadas.csv")

    finally:
        close_neo4j()

