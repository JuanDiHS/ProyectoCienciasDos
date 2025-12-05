# scripts/load_gtfs.py
"""
Script mejorado para cargar GTFS ZIP, construir el grafo y persistir artefactos procesados.
Uso (ejemplo):
    python .\scripts\load_gtfs.py \
        --zip data/raw/sitp_gtfs.zip \
        --out data/processed --run run_auto \
        --compress --save-bpt

Por defecto intenta guardar en data/processed/run_auto.
"""
from pathlib import Path
import pickle
import sys
import argparse
import gzip
import math

# import loader
try:
    from transport_opt.graph.loaders import build_graph_from_gtfs
except Exception as e:
    print("ERROR: No se pudo importar build_graph_from_gtfs. Asegúrate de estar ejecutando desde la raíz del proyecto")
    raise

# optional helper save function (no obligatorio)
try:
    from transport_opt.io import save_processed_artifacts  # si lo implementaste
except Exception:
    save_processed_artifacts = None

def save_pickle_gz(obj, path: Path):
    """Guarda objeto por pickle comprimido (gzip)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as fh:
        pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)

def write_edges_csv_gz(graph, path: Path, dedupe=True):
    try:
        import pandas as pd
    except Exception:
        print("AVISO: pandas no instalado. No se exportará CSV. Instala pandas si quieres CSV.")
        return False

    seen = set()
    rows = []
    for u in graph.adj:
        for v, w in graph.adj[u]:
            if dedupe and not graph.directed:
                key = (u, v) if u <= v else (v, u)
                if key in seen:
                    continue
                seen.add(key)

            meta = {}
            if hasattr(graph, 'edge_meta'):
                meta = graph.edge_meta.get((u, v), {}) or graph.edge_meta.get((v, u), {}) or {}

            trip_ids = list(meta.get('trip_ids', []))
            route_ids = list(meta.get('route_ids', []))
            modes = list(meta.get('modes', []))
            headways = [h for h in meta.get('headways', []) if h is not None]

            row = {
                "u": u,
                "v": v,
                "w": float(w),
                "trip_ids_count": len(trip_ids),
                "trip_ids_sample": ",".join(trip_ids[:5]) if trip_ids else "",
                "route_ids": ",".join(route_ids[:5]) if route_ids else "",
                "mode": ",".join(modes[:2]) if modes else "",
                "headway_min_secs": min(headways) if headways else None,
                "headway_mean_secs": (sum(headways)/len(headways)) if headways else None
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, compression="gzip")
    return True



def main(argv=None):
    argv = argv or sys.argv[1:]
    p = argparse.ArgumentParser(description="Construye grafo desde GTFS y guarda artefactos procesados")
    p.add_argument("--zip", "-z", type=str, default="data/raw/sitp_gtfs.zip", help="Ruta al zip GTFS")
    p.add_argument("--out", "-o", type=str, default="data/processed", help="Directorio base de salida")
    p.add_argument("--run", "-r", type=str, default="run_auto", help="Nombre de ejecución (subcarpeta)")
    p.add_argument("--no-save", action="store_true", help="No guardar artefactos (solo construir y mostrar resumen)")
    p.add_argument("--compress", action="store_true", help="Guardar pickle en gzip (.pkl.gz)")
    p.add_argument("--save-bpt", action="store_true", help="Guardar B+Tree como pickle (comprimido si --compress)")
    p.add_argument("--dedupe-edges", action="store_true", help="Eliminar duplicados en export CSV (útil para grafos no dirigidos)")
    args = p.parse_args(argv)

    zip_path = Path(args.zip)
    if not zip_path.exists():
        print("ERROR: no encontré el archivo GTFS en:", zip_path)
        sys.exit(1)

    out_dir = Path(args.out) / args.run
    out_dir.mkdir(parents=True, exist_ok=True)

    print("GTFS zip path:", zip_path)
    print("Construyendo grafo desde GTFS (esto puede tardar varios minutos según el tamaño)...")

    g, bpt = build_graph_from_gtfs(str(zip_path))
    print("Grafo construido. Nodos (stops):", len(g.nodes))

    # muestra de aristas (sanity-check)
    sample_edges = []
    cnt = 0
    for u in list(g.adj.keys())[:20]:
        for v,w in g.adj[u][:5]:
            sample_edges.append((u, v, w))
            cnt += 1
            if cnt >= 50:
                break
        if cnt >= 50:
            break
    print("Muestra de aristas (hasta 50):", sample_edges[:50])

    if args.no_save:
        print("No se guardaron artefactos por --no-save. Fin.")
        return 0

    # If there's a modular saver use it (custom transport_opt.io)
    if save_processed_artifacts is not None:
        try:
            saved = save_processed_artifacts(g, bpt, out_dir.parent)
            print("Artifacts guardados via transport_opt.io ->", saved)
            return 0
        except Exception as ex:
            print("Warning: save_processed_artifacts falló:", ex)
            print("Procediendo a guardado manual...")

    # Guardado manual
    print("Guardando artefactos en:", out_dir)
    # 1) grafo pickle (comprimido si --compress)
    graph_path = out_dir / "gtfs_graph.pkl.gz" if args.compress else out_dir / "gtfs_graph.pkl"
    try:
        if args.compress:
            save_pickle_gz(g, graph_path)
        else:
            with open(graph_path, "wb") as fh:
                pickle.dump(g, fh, protocol=pickle.HIGHEST_PROTOCOL)
        print("Guardado grafo en:", graph_path)
    except Exception as ex:
        print("ERROR guardando grafo:", ex)

    # 2) edges CSV (gzip) dedupe si se pidió
    csv_path = out_dir / "gtfs_edges_with_meta.csv.gz"
    dedupe = bool(args.dedupe_edges) or (not g.directed)
    csv_written = write_edges_csv_gz(g, csv_path, dedupe=dedupe)


    if csv_written:
        print("Guardado edges CSV (comprimido):", csv_path)
    else:
        print("No se generó CSV de edges (pandas no disponible)")

    # 3) stops (B+Tree) -> guardar como CSV y/o pickle si se solicitó
    if bpt is not None:
        # CSV of stops (use traverse_leaves if available)
        try:
            leaves = bpt.traverse_leaves()
            try:
                import pandas as pd
            except Exception:
                # fallback plain text file
                stops_txt = out_dir / "gtfs_stops.txt"
                with open(stops_txt, "w", encoding="utf-8") as fh:
                    for k, v in leaves:
                        fh.write(f"{k}\t{v}\n")
                print("Guardado stops en texto:", stops_txt)
            else:
                df = pd.DataFrame(leaves, columns=["stop_id", "metadata"])
                df.to_csv(out_dir / "gtfs_stops.csv", index=False, compression="gzip")
                print("Guardado stops CSV (gzip):", out_dir / "gtfs_stops.csv")
        except Exception as ex:
            print("Warning: no se pudo extraer leaves de B+Tree:", ex)

        if args.save_bpt:
            bpt_path = out_dir / ("gtfs_bpt.pkl.gz" if args.compress else "gtfs_bpt.pkl")
            try:
                if args.compress:
                    save_pickle_gz(bpt, bpt_path)
                else:
                    with open(bpt_path, "wb") as fh:
                        pickle.dump(bpt, fh, protocol=pickle.HIGHEST_PROTOCOL)
                print("Guardado B+Tree en:", bpt_path)
            except Exception as ex:
                print("ERROR guardando B+Tree:", ex)
    else:
        print("No se generó B+Tree (bpt is None)")

    print("Carga GTFS completada.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

