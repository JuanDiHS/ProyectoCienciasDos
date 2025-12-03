# src/transport_opt/cli.py
import argparse
import pickle
import os
import sys

from transport_opt.graph.algorithms import shortest_path, astar, default_heuristic_factory, build_stops_info_from_bpt

# visualización opcional
try:
    from transport_opt.viz.visualizer import visualize_path
    HAS_VIS = True
except Exception:
    HAS_VIS = False

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_GRAPH_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "gtfs_graph.pkl")
DEFAULT_BPT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "gtfs_bpt.pkl")


def load_graph(path: str = DEFAULT_GRAPH_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró el grafo en {path}. Genera el grafo con scripts/load_gtfs.py")
    with open(path, "rb") as f:
        return pickle.load(f)


def load_bpt(path: str = DEFAULT_BPT_PATH):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def cmd_route(args):
    g = load_graph(args.graph) if args.graph else load_graph()
    bpt = load_bpt(args.bpt) if args.bpt else load_bpt()

    if args.method == "dijkstra":
        path, cost = shortest_path(g, args.src, args.dst)
    else:  # astar
        heuristic = None
        if args.use_heuristic:
            # construir stops_info desde bpt (fallback a {})
            stops_info = build_stops_info_from_bpt(bpt) if bpt else {}
            heuristic = default_heuristic_factory(stops_info)
        path, cost = astar(g, args.src, args.dst, heuristic=heuristic)

    print("=== RESULTADO ===")
    print("Origen:", args.src)
    print("Destino:", args.dst)
    print("Método:", args.method, "| Usar heurística:", args.use_heuristic)
    print("Coste (min):", cost)
    print("Path length:", len(path) if path else 0)
    print("Path:", path)

    if args.plot:
        if not HAS_VIS:
            print("Visualización no disponible. Instala networkx y matplotlib.")
        else:
            visualize_path(g, path, title=f"Ruta: {args.src} → {args.dst} ({args.method})")


def main():
    p = argparse.ArgumentParser(prog="transport_opt")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("route", help="Buscar ruta entre dos paradas")
    pr.add_argument("--from", dest="src", required=True, help="stop_id origen")
    pr.add_argument("--to", dest="dst", required=True, help="stop_id destino")
    pr.add_argument("--method", choices=["dijkstra", "astar"], default="dijkstra")
    pr.add_argument("--use-heuristic", action="store_true", help="Usar heurística (solo para astar)")
    pr.add_argument("--graph", help="Ruta al archivo pkl del grafo (opcional)")
    pr.add_argument("--bpt", help="Ruta al archivo pkl del B+Tree con stops (opcional)")
    pr.add_argument("--plot", action="store_true", help="Mostrar gráfica de la ruta (si hay dependencias)")

    args = p.parse_args()
    if args.cmd == "route":
        cmd_route(args)


if __name__ == "__main__":
    main()

