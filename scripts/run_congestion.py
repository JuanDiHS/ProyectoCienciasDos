# scripts/run_congestion.py
"""
Analizador de congestión simple:
  - carga grafo procesado (data/processed/<run>)
  - construye capacidades por arista (desde GTFS si está disponible, si no heurística)
  - construye super-source (SRC) y super-sink (SNK)
  - ejecuta edmonds_karp y muestra top-N aristas por utilización
Uso:
  python scripts/run_congestion.py --run run_auto --top 30 --root .
"""
import argparse
from pathlib import Path
from collections import defaultdict
import math
import time
from transport_opt.utils import load_stop_names

from transport_opt.io import load_graph_from_processed
from transport_opt.graph.algorithms import edmonds_karp
from transport_opt.graph.gtfs_utils import build_capacity_from_gtfs

def build_capacity_from_graph(graph, base_capacity_per_minute=30):
    capacity = {}
    for u in graph.nodes:
        capacity[u] = {}
    for u in graph.adj:
        for v, w in graph.adj[u]:
            cap = base_capacity_per_minute
            if str(u).startswith("M") or str(v).startswith("M"):
                cap = int(base_capacity_per_minute * 3)
            elif str(u).startswith("T") or str(v).startswith("T"):
                cap = int(base_capacity_per_minute * 2)
            capacity[u][v] = cap
    return capacity

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", default="run_auto", help="Nombre del run en data/processed")
    p.add_argument("--root", default=".", help="Ruta raíz del proyecto (contiene data/)")
    p.add_argument("--top", type=int, default=20, help="Número de aristas top a mostrar")
    p.add_argument("--gtfs", default=None, help="Ruta al zip GTFS (opcional). Si no se provee, se buscará data/raw/sitp_gtfs.zip")
    args = p.parse_args()

    ROOT = Path(args.root).resolve()
    run_name = args.run
    top_n = args.top

    # Cargar grafo desde artifacts procesados
    graph, bpt = load_graph_from_processed(ROOT, run_name=run_name)
    print("Grafo cargado. Nodos:", len(graph.nodes))
    graph, bpt = load_graph_from_processed(Path(args.root), run_name=args.run)
    processed_dir = Path(args.root) / "data" / "processed" / args.run
    stop_names = load_stop_names(processed_dir, bpt=bpt)
    def pretty_stop(sid):
        return f"{stop_names.get(sid, sid)} ({sid})"


    # Intentar crear capacity desde GTFS si existe
    gtfs_path = Path(args.gtfs) if args.gtfs else (ROOT / "data" / "raw" / "sitp_gtfs.zip")
    if gtfs_path.exists():
        print("Construyendo capacidades desde GTFS:", gtfs_path)
        t0 = time.time()
        try:
            capacity = build_capacity_from_gtfs(str(gtfs_path), graph)
            print("Capacity built from GTFS (took {:.1f}s)".format(time.time() - t0))
        except Exception as ex:
            print("Warning: fallo al construir capacidad desde GTFS:", ex)
            print("Usando heurística por prefijo de nodo.")
            capacity = build_capacity_from_graph(graph)
    else:
        print("GTFS no encontrado en:", gtfs_path)
        print("Usando heurística por prefijo de nodo.")
        capacity = build_capacity_from_graph(graph)

    # elegir sink: nodo con mayor grado (hub)
    degs = [(len(graph.adj[n]), n) for n in graph.nodes]
    degs.sort(reverse=True)
    sink = degs[0][1]
    print("Seleccionado sink (hub):", pretty_stop(sink))


    # construir super-source SRC, conectarlo a todos nodos como orígenes
    capacity.setdefault("SRC", {})
    for n in graph.nodes:
        cap = max(1, int(len(graph.adj[n]) * 2))
        capacity["SRC"][n] = cap

    # asegurar que cada nodo tiene dict en capacity y conectar sink a SNK
    for n in graph.nodes:
        capacity.setdefault(n, {})
    capacity[sink]["SNK"] = 10**9  # sink con gran capacidad

    print("Ejecutando Edmonds-Karp (esto puede tardar un poco)...")
    t0 = time.time()
    maxflow, flow = edmonds_karp(capacity, 'SRC', 'SNK')
    print("Flujo máximo encontrado (pasajeros/min aprox):", maxflow, f"(took {time.time()-t0:.1f}s)")

    # calcular utilización por arista u->v
    utiliz = []
    for u in flow:
        for v in flow[u]:
            f = flow[u][v]
            cap = capacity.get(u, {}).get(v, None)
            if cap is None or cap == 0:
                continue
            if (u == "SRC") or (v == "SNK"):
                continue
            util = f / cap
            utiliz.append((util, u, v, f, cap))
    utiliz.sort(reverse=True)

    print(f"Top {top_n} aristas por utilización:")
    for util, u, v, f, cap in utiliz[:top_n]:
        print(f"{pretty_stop(u)} -> {pretty_stop(v)} | utilization: {util:.2f} | flow={f} cap={cap}")


if __name__ == "__main__":
    main()
