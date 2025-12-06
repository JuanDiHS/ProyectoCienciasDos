#!/usr/bin/env python3
# scripts/list_stations.py
"""
Lista estaciones legibles (stop_id, stop_name).
Uso:
  python scripts/list_stations.py --run run_auto --limit 50
  python scripts/list_stations.py --run run_auto --format csv --out outputs/stops.csv
"""
from pathlib import Path
import argparse
import csv
import gzip
import io
import sys

# Intentamos usar utilidades del paquete si están disponibles
try:
    from transport_opt.utils import load_stop_names
except Exception:
    load_stop_names = None

try:
    from transport_opt.io import load_graph_from_processed
except Exception:
    load_graph_from_processed = None

def read_stops_csv_any(path: Path):
    """
    Intentar leer csv o csv.gz con columnas stop_id, stop_name — devuelve dict.
    Detecta gzip comprobando los primeros bytes (magic header).
    """
    names = {}
    p = Path(path)
    if not p.exists():
        return names

    # detectar gzip por magic bytes
    try:
        with open(p, "rb") as fh:
            head = fh.read(2)
        is_gz = head == b"\x1f\x8b"
    except Exception:
        is_gz = False

    open_fn = gzip.open if is_gz else open
    try:
        # usar text mode con replacement de errores para no fallar por encoding raro
        with open_fn(p, mode="rt", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            # si el archivo no es CSV válido, DictReader puede fallar al acceder a fieldnames
            for r in reader:
                sid = r.get("stop_id") or r.get("id") or r.get("stop_id".lower())
                nm = r.get("stop_name") or r.get("name") or r.get("metadata")
                if sid:
                    names[str(sid)] = nm or str(sid)
    except Exception:
        # último recurso: intentar con pandas si está instalado (más tolerante)
        try:
            import pandas as pd
            df = pd.read_csv(p, dtype=str, compression='gzip' if is_gz else None)
            for _, r in df.iterrows():
                sid = r.get("stop_id") or r.get("id")
                nm = r.get("stop_name") or r.get("name") or r.get("metadata")
                if pd.notna(sid):
                    names[str(sid)] = (nm if (nm is not None and not pd.isna(nm)) else str(sid))
        except Exception:
            pass
    return names

def pretty_print(names: dict, limit: int = None):
    items = list(names.items())
    if limit:
        items = items[:limit]
    # calcular anchos
    idw = max((len(k) for k, _ in items), default=6)
    namew = max((len(v) for _, v in items), default=4)
    idw = max(idw, len("stop_id"))
    namew = max(namew, len("stop_name"))
    sep = "-" * (idw + 3 + namew)
    print(f"{'stop_id'.ljust(idw)}   {'stop_name'.ljust(namew)}")
    print(sep)
    for sid, name in items:
        print(f"{sid.ljust(idw)}   {str(name).ljust(namew)}")

def main(argv=None):
    argv = argv or sys.argv[1:]
    p = argparse.ArgumentParser(description="List readable GTFS stops (stop_id, stop_name)")
    p.add_argument("--run", "-r", default="run_auto", help="Nombre del run en data/processed")
    p.add_argument("--root", "-R", default=".", help="Directorio raíz del proyecto")
    p.add_argument("--format", "-f", choices=("pretty","csv"), default="pretty", help="Formato de salida")
    p.add_argument("--out", "-o", default=None, help="Ruta de salida si --format csv")
    p.add_argument("--limit", "-n", type=int, default=None, help="Mostrar solo primeras N filas (pretty)")
    args = p.parse_args(argv)

    ROOT = Path(args.root)
    processed_dir = ROOT / "data" / "processed" / args.run

    names = {}
    # 1) Intentar cargar usando transport_opt.utils (si existe)
    if load_stop_names is not None:
        try:
            names = load_stop_names(processed_dir, bpt=None)
        except Exception:
            names = {}

    # 2) Si load_stop_names no dio nada, intentar load_graph_from_processed para obtener bpt
    if not names and load_graph_from_processed is not None:
        try:
            graph, bpt = load_graph_from_processed(ROOT, run_name=args.run)
            if load_stop_names is not None:
                names = load_stop_names(processed_dir, bpt=bpt)
        except Exception:
            names = {}

    # 3) Si todavía vacío: intentar leer gtfs_stops CSV en processed_dir (detecta gz)
    if not names:
        candidate_csv = processed_dir / "gtfs_stops.csv"
        candidate_csv_gz = processed_dir / "gtfs_stops.csv.gz"
        # preferimos el .gz si existe
        for c in (candidate_csv_gz, candidate_csv):
            if c.exists():
                names = read_stops_csv_any(c)
                if names:
                    break

    # 4) Si aun vacío: intentar leer stops.txt del zip raw
    if not names:
        raw_zip = ROOT / "data" / "raw"
        found = None
        if raw_zip.exists():
            for pth in raw_zip.iterdir():
                if pth.is_file() and "gtfs" in pth.name.lower() and pth.suffix.lower() in (".zip",):
                    found = pth
                    break
        if found:
            try:
                import zipfile, io
                with zipfile.ZipFile(found) as zf:
                    if "stops.txt" in zf.namelist():
                        with zf.open("stops.txt") as fh:
                            txt = io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
                            reader = csv.DictReader(txt)
                            for r in reader:
                                sid = r.get("stop_id")
                                nm = r.get("stop_name") or r.get("name")
                                if sid:
                                    names[str(sid)] = nm or str(sid)
            except Exception:
                pass

    if not names:
        print("No pude encontrar nombres de paradas. Asegúrate de haber ejecutado scripts/load_gtfs.py y de que exista data/processed/{run}/gtfs_stops.csv(.gz) o el B+Tree.", file=sys.stderr)
        return 2

    # salida
    if args.format == "csv":
        out_path = Path(args.out) if args.out else Path("stations.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["stop_id","stop_name"])
            for sid, name in sorted(names.items()):
                writer.writerow([sid, name])
        print("Saved:", out_path)
    else:
        pretty_print({k:v for k,v in sorted(names.items())}, limit=args.limit)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
