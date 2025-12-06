# scripts/run_simulation.py
"""
Ejecuta una simulación de demanda contra el grafo procesado y guarda resultados legibles.
Uso (ejemplo):
  # activar el venv (Windows PowerShell)
  .\.venv\Scripts\Activate

  # ejecutar (desde la raíz del proyecto)
  python scripts/run_simulation.py --run run_auto --out results/run1
  python scripts/run_simulation.py --run run_auto --demand-file examples/demand.csv --out results/run2 --window 3600 --seed 0

Formato demand-file (CSV): columnas origin,dest,rate_h
  ejemplo:
    origin,dest,rate_h
    Z_52031_STOP,Z_52029_STOP,300
    Z_52031_STOP,Z_53303_STOP,120
"""
from pathlib import Path
import argparse
import json
import csv
import sys
from typing import List, Tuple
import random
import numpy as np

# imports del paquete del proyecto
try:
    from transport_opt.io import load_graph_from_processed
except Exception as e:
    print("ERROR: no se pudo importar transport_opt.io. Ejecuta desde la raíz del proyecto y verifica el package.")
    raise

try:
    from transport_opt.sim.simulator import Simulator
except Exception as e:
    print("ERROR: no se pudo importar Simulator (transport_opt.sim.simulator).")
    raise

try:
    from transport_opt.utils import load_stop_names
except Exception:
    # si no existe utils, definimos un stub que devuelve {} (se usará stop_id como nombre)
    def load_stop_names(*args, **kwargs):
        return {}

def read_demand_csv(path: Path) -> List[Tuple[str, str, float]]:
    """Lee CSV con columnas origin,dest,rate_h -> devuelve lista (origin,dest,rate_h)"""
    demand = []
    with open(path, newline='', encoding='utf-8') as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            try:
                origin = (r.get('origin') or r.get('origin_id') or r.get('from') or "").strip()
                dest = (r.get('dest') or r.get('destination') or r.get('to') or "").strip()
                rate_h_raw = r.get('rate_h') or r.get('rate') or r.get('rate_per_hour') or "0"
                rate_h = float(rate_h_raw)
                if origin and dest and rate_h > 0:
                    demand.append((origin, dest, rate_h))
            except Exception:
                # ignorar líneas malformadas
                continue
    return demand

def write_edge_load_named(out_path: Path, edge_load: dict, stop_names: dict):
    """Guarda edge_load (dict (u,v)->count) a CSV legible"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=["u_id","u_name","v_id","v_name","count"])
        writer.writeheader()
        for (u,v), count in edge_load.items():
            writer.writerow({
                "u_id": u,
                "u_name": stop_names.get(u, u),
                "v_id": v,
                "v_name": stop_names.get(v, v),
                "count": count
            })

def write_passenger_results(out_path: Path, passenger_results: list, stop_names: dict):
    """Guarda la lista de resultados por pasajero (pid,path,time) a CSV"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=["pid","origin_id","origin_name","dest_id","dest_name","path","travel_time"])
        writer.writeheader()
        for pid, path, t in passenger_results:
            origin = path[0] if path else ''
            dest = path[-1] if path else ''
            writer.writerow({
                "pid": pid,
                "origin_id": origin,
                "origin_name": stop_names.get(origin, origin),
                "dest_id": dest,
                "dest_name": stop_names.get(dest, dest),
                "path": " -> ".join(path) if path else "",
                "travel_time": t if t is not None else ""
            })

def main(argv=None):
    argv = argv or sys.argv[1:]
    p = argparse.ArgumentParser(description="Run passenger flow simulation and export readable results")
    p.add_argument("--run", "-r", default="run_auto", help="Nombre del run en data/processed")
    p.add_argument("--root", "-R", default=".", help="Directorio raíz del proyecto")
    p.add_argument("--out", "-o", default="results/run_sim", help="Directorio de salida para resultados")
    p.add_argument("--demand-file", "-d", default=None, help="CSV con columnas origin,dest,rate_h")
    p.add_argument("--window", "-w", type=int, default=3600, help="Ventana de simulación en segundos")
    p.add_argument("--sampling-factor", "-s", type=int, default=10, help="Factor de muestreo interno del simulador (si lo soporta)")
    p.add_argument("--seed", type=int, default=None, help="Semilla aleatoria para reproducibilidad (random + numpy)")
    args = p.parse_args(argv)

    # reproducibilidad (si se pasó --seed)
    if args.seed is not None:
        random.seed(int(args.seed))
        try:
            np.random.seed(int(args.seed))
        except Exception:
            pass
        print(f"Semilla fija a {args.seed} (random + numpy)")

    ROOT = Path(args.root)
    processed_dir = ROOT / "data" / "processed" / args.run

    # 1) cargar grafo y bpt
    try:
        graph, bpt = load_graph_from_processed(ROOT, run_name=args.run)
    except Exception as e:
        print("ERROR: no se pudo cargar grafo procesado con load_graph_from_processed:", e)
        raise

    print("Grafo cargado. Nodos:", len(graph.nodes))

    # 2) cargar nombres legibles de paradas (si están)
    stop_names = {}
    try:
        stop_names = load_stop_names(processed_dir, bpt=bpt)
    except Exception:
        stop_names = {}

    # 3) leer demanda (o usar ejemplo por defecto)
    demand = []
    if args.demand_file:
        demand_path = Path(args.demand_file)
        if not demand_path.exists():
            print("ERROR: demand-file no encontrado:", demand_path)
            return 1
        demand = read_demand_csv(demand_path)
        if not demand:
            print("AVISO: demanda vacía o malformada en", demand_path, "- se usará demanda por defecto.")

    if not args.demand_file or not demand:
        # demanda por defecto simple (ajústala a tu gusto)
        sample_nodes = list(graph.nodes)[:6]
        demand = []
        for i in range(0, len(sample_nodes), 2):
            if i+1 < len(sample_nodes):
                demand.append((sample_nodes[i], sample_nodes[i+1], 200))

    print("Demanda (ejemplo):", demand[:10])

    # 4) ejecutar simulador
    sim = Simulator(graph)
    print("Ejecutando simulador (window_seconds=%s sampling_factor=%s) ..." % (args.window, args.sampling_factor))

    try:
        # Intentamos pasar sampling_factor sólo si el método lo acepta
        try:
            sim_out = sim.run(demand, window_seconds=args.window, sampling_factor=args.sampling_factor)  # type: ignore
        except TypeError:
            # firma antigua sin sampling_factor
            sim_out = sim.run(demand, window_seconds=args.window)
    except Exception as ex:
        print("ERROR durante la ejecución del simulador:", ex)
        raise

    # 5) guardar resultados legibles
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # summary JSON
    summary = {
        "total_passengers": sim_out.get('total_passengers'),
        "avg_travel_time": sim_out.get('avg_travel_time'),
        "total_results": len(sim_out.get('passenger_results', []))
    }
    with open(out_dir / "summary.json", "w", encoding='utf-8') as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)

    # edge load named CSV
    try:
        write_edge_load_named(out_dir / "edge_load_named.csv", sim_out.get('edge_load', {}), stop_names or {})
    except Exception as ex:
        print("Warning: fallo guardando edge_load_named:", ex)

    # passenger results CSV (puede ser grande, guardamos muestra si es muy largo)
    pr = sim_out.get('passenger_results', [])
    try:
        if len(pr) > 10000:
            write_passenger_results(out_dir / "passenger_results_head.csv", pr[:10000], stop_names or {})
        else:
            write_passenger_results(out_dir / "passenger_results.csv", pr, stop_names or {})
    except Exception as ex:
        print("Warning: fallo guardando passenger results:", ex)

    print("Resultados guardados en:", out_dir)
    print("Resumen:", summary)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

