from pathlib import Path
import csv
from typing import Dict, Any, Optional

# ya tienes load_stop_names en este archivo; esta función la usa.
# Pega esto al final de src/transport_opt/utils.py

def save_edge_load_named(sim_result: Dict[str, Any],
                         out_dir: Path,
                         processed_run_dir: Path,
                         bpt: Optional[object] = None,
                         filename: str = "edge_load_named.csv") -> Path:
    """
    Guarda un CSV legible con cargas por arista usando nombres de parada.

    Args:
      sim_result: resultado devuelto por Simulator.run(), debe contener "edge_load" dict.
      out_dir: carpeta donde se guarda el CSV (Path o str).
      processed_run_dir: carpeta data/processed/<run> para cargar nombres (Path o str).
      bpt: si tienes el B+Tree, pásalo para obtener nombres más fiables (opcional).
      filename: nombre del archivo CSV de salida.

    Retorna:
      Path al archivo guardado.
    """
    out_p = Path(out_dir)
    out_p.mkdir(parents=True, exist_ok=True)

    # intentar cargar pandas para salvar con mayor comodidad; si no, usa csv.writer
    try:
        import pandas as pd
        use_pandas = True
    except Exception:
        use_pandas = False

    # obtener mapa stop_id -> stop_name (usa la función existente)
    try:
        stop_names = load_stop_names(Path(processed_run_dir), bpt)
    except Exception:
        stop_names = {}

    rows = []
    edge_load = sim_result.get("edge_load", {})
    for key, cnt in edge_load.items():
        # key puede ser tupla (u,v) o string; normalizamos
        if isinstance(key, (list, tuple)) and len(key) >= 2:
            u, v = key[0], key[1]
        else:
            # si se guarda como "u|v" u otro formato, intentar parsear
            ks = str(key)
            if "|" in ks:
                u, v = ks.split("|", 1)
            elif "->" in ks:
                u, v = ks.split("->", 1)
            else:
                # fallback: todo en u, v vacío
                u, v = ks, ""
        rows.append({
            "u_id": u,
            "u_name": stop_names.get(u, u),
            "v_id": v,
            "v_name": stop_names.get(v, v) if v else "",
            "count": int(cnt)
        })

    out_file = out_p / filename

    if use_pandas:
        df = pd.DataFrame(rows, columns=["u_id","u_name","v_id","v_name","count"])
        df.to_csv(out_file, index=False)
    else:
        # escritura CSV sin pandas
        with open(out_file, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["u_id","u_name","v_id","v_name","count"])
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    return out_file
