# Blockchain_v4.py

# Programa que solicita una transacción y muestra Input y Output
# https://mempool.space/api/tx/<txid>
# Versión 4 : Recursiva, buscando en el historial de cada monedero origen y destino.(ANALIZADOR FORENSE REAL)

#Dada una transacción 0:
#- Para cada input, obtenemos la dirección origen (monedero origen).
#Luego buscamos todas las transacciones anteriores donde esa dirección aparece como output.
#Eso nos permite ir hacia atrás.
#- Para cada output, obtenemos la dirección destino (monedero destino).
#Luego buscamos todas las transacciones posteriores donde esa dirección aparece como input.
#Eso nos permite ir hacia adelante.


import requests

BASE = "https://mempool.space/api"

def get_tx(txid):
    r = requests.get(f"{BASE}/tx/{txid}")
    if r.status_code != 200:
        return None
    return r.json()

def get_address_txs(address):
    r = requests.get(f"{BASE}/address/{address}/txs")
    if r.status_code != 200:
        return []
    return r.json()

# ============================
#   BACKWARD TRACING
# ============================

def trace_backward(address, visited=None, depth=0, max_depth=5):
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

# ============================
#   PROGRAMA PRINCIPAL
# ============================

txid = input("Introduce TXID inicial: ").strip()
tx = get_tx(txid)

if not tx:
    print("Transacción no encontrada.")
    exit()

print("\n=== INPUTS (origen) ===")
for i in tx["vin"]:
    addr = i.get("prevout", {}).get("scriptpubkey_address")
    if addr:
        print(f"Origen: {addr}")
        trace_backward(addr)

print("\n=== OUTPUTS (destino) ===")
for o in tx["vout"]:
    addr = o.get("scriptpubkey_address")
    if addr:
        print(f"Destino: {addr}")
        trace_forward(addr)