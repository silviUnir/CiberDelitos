import pandas as pd

def analizar_csv(ruta_csv, ruta_salida="transacciones_analizadas.csv", tx_inicial=None):    
    df = pd.read_csv(ruta_csv)

    # ============================
    # 1. Normalizar fechas
    # ============================
    df["fecha_transaccion_utc"] = pd.to_datetime(
        df["fecha_transaccion_utc"], errors="coerce"
    )

    # ============================
    # 2. Calcular tx_order_id
    # ============================
    fechas = df.groupby("txid")["fecha_transaccion_utc"].first()
    fechas_ordenadas = fechas.sort_values()

    if tx_inicial is None:
        tx_inicial = fechas_ordenadas.index[0]

    idx_inicial = fechas_ordenadas.index.get_loc(tx_inicial)

    tx_order_map = {}
    for i, txid in enumerate(fechas_ordenadas.index):
        offset = i - idx_inicial
        tx_order_map[txid] = f"{offset:04d}" if offset >= 0 else f"-{abs(offset):04d}"

    df["tx_order_id"] = df["txid"].map(tx_order_map)

    # ============================
    # 3. Análisis forense
    # ============================
    tx_groups = df.groupby("txid")
    patrones = {}

    # === NUEVO: contador global de patrones ===
    resumen_global = {
        "op_return": 0,
        "cambio": 0,
        "peeling_chain": 0,
        "consolidacion": 0,
        "batching": 0,
        "actividad_circular": 0
    }

    for txid, grupo in tx_groups:

        input_addrs = set(grupo["input_address"].dropna())
        output_addrs = list(grupo["output_address"])

        num_inputs = len(input_addrs)
        num_outputs = len(output_addrs)

        op_return = any(addr == "OP_RETURN" for addr in output_addrs)
        cambio = any(addr in input_addrs for addr in output_addrs if addr != "OP_RETURN")
        peeling_chain = (num_inputs == 1 and num_outputs == 2 and cambio)
        consolidacion = (num_inputs > 1 and num_outputs == 1)
        batching = (num_inputs == 1 and num_outputs > 2)
        actividad_circular = cambio

        patrones[txid] = {
            "op_return": op_return,
            "cambio": cambio,
            "peeling_chain": peeling_chain,
            "consolidacion": consolidacion,
            "batching": batching,
            "actividad_circular": actividad_circular,
            "num_inputs": num_inputs,
            "num_outputs": num_outputs
        }

        # === NUEVO: actualizar resumen global ===
        for p, valor in patrones[txid].items():
            if p in resumen_global and valor is True:
                resumen_global[p] += 1

        # Mostrar por pantalla
        print(f"\n=== TX {txid} (orden {tx_order_map[txid]}) ===")
        encontrados = [k for k, v in patrones[txid].items() if v is True]

        if encontrados:
            for p in encontrados:
                motivo = ""
                if p == "op_return":
                    motivo = "→ contiene un output OP_RETURN"
                elif p == "cambio":
                    motivo = "→ output vuelve a la misma dirección que el input"
                elif p == "peeling_chain":
                    motivo = "→ 1 input, 2 outputs (uno es cambio)"
                elif p == "actividad_circular":
                    motivo = "→ input y output coinciden"
                elif p == "consolidacion":
                    motivo = "→ varios inputs, un solo output"
                elif p == "batching":
                    motivo = "→ un input, múltiples outputs"

                print(f"Patrón detectado: {p} {motivo}")

        else:
            print("No localizado ningún patrón.")

    # Añadir patrones al CSV
    for col in patrones[next(iter(patrones))].keys():
        df[col] = df["txid"].map(lambda x: patrones[x][col])

    df.to_csv(ruta_salida, index=False)
    print(f"\n[OK] Análisis completado. Archivo generado: {ruta_salida}")

    # ============================
    # 4. RESUMEN GLOBAL FINAL
    # ============================
    print("\n\n===== RESUMEN GLOBAL DE PATRONES =====")

    # === NUEVO: total de transacciones analizadas ===
    total_transacciones = df["txid"].nunique()
    print(f"Transacciones analizadas: {total_transacciones}\n")

    total = sum(resumen_global.values())

    if total == 0:
        print("No se han localizado patrones en esta trazabilidad.")
    else:
        print("Patrones detectados en toda la trazabilidad:\n")
        for p, count in resumen_global.items():
            print(f"- {p}: {count}")
        print(f"\nTOTAL patrones detectados: {total}")

    return df
