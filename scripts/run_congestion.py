# scripts/run_congestion.py
"""
Analizador de congestión simple:
  - carga grafo procesado (data/processed/run_auto)
  - construye capacidades por arista (heurística por tipo de parada)
  - construye super-source (SRC) y super-sink (SNK)
  - ejecuta edmonds_karp y muestra top-N aristas por utilización
Uso:
  python scripts/run_congestion.py --run run_auto --top 30
"""
import argparse
from pathlib import Path
from collections import defaultdict
import math

from transport_opt.io import load_graph_from_processed
from transport_opt.graph.algorithms import edmonds_karp

def build_capacity_from_graph(graph, base_capacity_per_minute=30):
    capacity = {}
    for u in graph.nodes:
        capacity[u] = {}
    for u in graph.adj:
        for v, w in graph.adj[u]:
            # heurística: metro (M) > tm (T) > sitp (S)
            cap = base_capacity_per_minute
            if str(u).startswith("M") or str(v).startswith("M"):
                cap = int(base_capacity_per_minute * 3)
            elif str(u).startswith("T") or str(v).startswith("T"):
                cap = int(base_capacity_per_minute * 2)
            capacity[u][v] = cap
    return capacity

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", default="run_auto")
    p.add_argument("--root", default=".")
    p.add_argument("--top", type=int, default=20)
    args = p.parse_args()

    graph, bpt = load_graph_from_processed(Path(args.root), run_name=args.run)
    print("Grafo cargado. Nodos:", len(graph.nodes))

    capacity = build_capacity_from_graph(graph, base_capacity_per_minute=30)

    # elegir sink: nodo con mayor grado (hub)
    degs = [(len(graph.adj[n]), n) for n in graph.nodes]
    degs.sort(reverse=True)
    sink = degs[0][1]
    print("Seleccionado sink (hub):", sink)

    # construir super-source SRC, conectarlo a todos nodos como orígenes con capacidad proporcional al grado
    capacity["SRC"] = {}
    for n in graph.nodes:
        # aporte de origen; pequeño, ejemplo: degree passengers per minute
        cap = max(1, int(len(graph.adj[n]) * 2))
        capacity["SRC"][n] = cap

    # conectar sinks al super-sink SNK
    for n in graph.nodes:
        capacity.setdefault(n, {})
    capacity[sink]["SNK"] = 100000  # sink con gran capacidad

    print("Ejecutando Edmonds-Karp (esto puede tardar un poco)...")
    maxflow, flow = edmonds_karp(capacity, 'SRC', 'SNK')
    print("Flujo máximo encontrado (pasajeros/min aprox):", maxflow)

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
    print("Top aristas por utilización:")
    for util, u, v, f, cap in utiliz[:args.top]:
        print(f"{u} -> {v} | utilization: {util:.2f} | flow={f} cap={cap}")

if __name__ == "__main__":
    main()
