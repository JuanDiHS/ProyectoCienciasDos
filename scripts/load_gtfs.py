# scripts/load_gtfs.py
"""
Script para cargar un GTFS ZIP, construir el grafo y persistir artefactos procesados.
Uso: desde la raíz del proyecto (la carpeta que contiene src/, data/, etc.)
    python .\scripts\load_gtfs.py
"""
from pathlib import Path
import pickle
import sys

# intentamos importar el loader y la función de guardado
try:
    from transport_opt.graph.loaders import build_graph_from_gtfs
except Exception as e:
    print("ERROR: No se pudo importar build_graph_from_gtfs. Asegúrate de estar ejecutando desde la raíz del proyecto")
    print("y de que el paquete transport_opt esté accesible (usa python -m pip install pandas numpy si falta).")
    raise

try:
    from transport_opt.io import save_processed_artifacts
except Exception:
    # si no existe el módulo io, intentamos import desde utils o simplemente saltamos el guardado modular
    save_processed_artifacts = None

def main():
    ROOT = Path(__file__).resolve().parents[1]  # proyecto root
    zip_path = ROOT / "data" / "raw" / "sitp_gtfs.zip"

    if not zip_path.exists():
        print("ERROR: no encontré el archivo GTFS en:", zip_path)
        print("Coloca el zip en data/raw/ y vuelve a ejecutar.")
        sys.exit(1)

    print("GTFS zip path:", zip_path)
    print("Construyendo grafo desde GTFS (esto puede tardar unos segundos/minutos según el tamaño)...")
    g, bpt = build_graph_from_gtfs(str(zip_path))

    print("Grafo construido. Nodos (stops):", len(g.nodes))
    # imprimir una muestra de aristas para sanity-check
    sample_edges = []
    for u in list(g.adj.keys())[:10]:
        for v,w in g.adj[u][:5]:
            sample_edges.append((u, v, w))
    print("Muestra de aristas (hasta 50):", sample_edges[:50])

    # Guardado: si existe save_processed_artifacts, lo usamos; si no, guardamos manualmente en data/processed/run_...
    if save_processed_artifacts is not None:
        out_dir = save_processed_artifacts(g, bpt, ROOT)
        print("Artifacts guardados en (via transport_opt.io):", out_dir)
    else:
        # guardado manual (compatibilidad si no existe transport_opt.io)
        processed_dir = ROOT / "data" / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)
        run_name = "run_auto"
        out_dir = processed_dir / run_name
        out_dir.mkdir(parents=True, exist_ok=True)

        # grafo pickle
        with open(out_dir / "gtfs_graph.pkl", "wb") as fh:
            pickle.dump(g, fh)

        # edges CSV
        try:
            import pandas as pd
        except Exception:
            print("AVISO: pandas no instalado. Instala pandas para exportar CSV (python -m pip install pandas).")
            print("Se guardó solo el pickle del grafo:", out_dir / "gtfs_graph.pkl")
        else:
            edges = []
            for u in g.adj:
                for v,w in g.adj[u]:
                    edges.append({"u": u, "v": v, "w": w})
            pd.DataFrame(edges).to_csv(out_dir / "gtfs_edges.csv", index=False)

            if bpt:
                pd.DataFrame(bpt.traverse_leaves(), columns=["stop_id", "metadata"]).to_csv(out_dir / "gtfs_stops.csv", index=False)

        print("Artifacts guardados en (manual):", out_dir)

    print("Carga GTFS completada.")

if __name__ == "__main__":
    main()
