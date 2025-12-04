# src/transport_opt/io.py
import pickle
import gzip
from pathlib import Path
from typing import Tuple, Optional, Any  # <-- asegurate que 'Any' esté importado
import os
from pathlib import Path
from typing import Tuple, Optional
import pandas as _pd

def _ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def save_pickle(obj, path: Path, compress: bool = False):
    _ensure_parent(path)
    if compress:
        with gzip.open(str(path), "wb") as fh:
            pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(path, "wb") as fh:
            pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return path

class _CompatUnpickler(pickle.Unpickler):
    """
    Unpickler que intercepta intentos de importar la clase 'Edge' desde cualquier módulo
    y la reemplaza por la Edge actual definida en transport_opt.graph.model.
    """
    def find_class(self, module: str, name: str):
        if name == "Edge":
            try:
                from transport_opt.graph.model import Edge as CurrentEdge
                return CurrentEdge
            except Exception:
                pass
        return super().find_class(module, name)

def load_pickle(path: Path) -> Any:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    # gzip o .pkl.gz
    if p.suffix == ".gz" or str(p).endswith(".pkl.gz"):
        with gzip.open(str(p), "rb") as fh:
            return _CompatUnpickler(fh).load()
    else:
        with open(p, "rb") as fh:
            return _CompatUnpickler(fh).load()
    def find_class(self, module: str, name: str):
        if name == "Edge":
            try:
                from transport_opt.graph.model import Edge as CurrentEdge
                return CurrentEdge
            except Exception:
                pass
        return super().find_class(module, name)

def load_pickle(path: Path) -> Any:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    # gzip o .pkl.gz
    if p.suffix == ".gz" or str(p).endswith(".pkl.gz"):
        with gzip.open(str(p), "rb") as fh:
            return _CompatUnpickler(fh).load()
    else:
        with open(p, "rb") as fh:
            return _CompatUnpickler(fh).load()

def save_processed_artifacts(graph, bpt, root: Path, run_name: str = "run_auto",
                             compress: bool = True, dedupe_edges: bool = True):
    """
    Guarda artifacts en root/data/processed/<run_name>/:
      - gtfs_graph.pkl.gz  (o .pkl)
      - gtfs_bpt.pkl.gz    (si bpt)
      - gtfs_edges.csv.gz  (si pandas disponible)
      - gtfs_stops.csv.gz  (si pandas disponible)
    Devuelve la path del out_dir.
    """
    from pathlib import Path
    import pandas as pd
    out_dir = Path(root) / "data" / "processed" / run_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # graph
    graph_name = "gtfs_graph.pkl.gz" if compress else "gtfs_graph.pkl"
    save_pickle(graph, out_dir / graph_name, compress=compress)

    # edges
    rows = []
    seen = set()
    for u in graph.adj:
        for v, w in graph.adj[u]:
            if dedupe_edges and not graph.directed:
                key = (u, v) if u <= v else (v, u)
                if key in seen:
                    continue
                seen.add(key)
            rows.append({"u": u, "v": v, "w": float(w)})
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(out_dir / "gtfs_edges.csv.gz", index=False, compression="gzip")

    # bpt -> stops
    if bpt is not None:
        # try traverse_leaves
        try:
            leaves = bpt.traverse_leaves()
        except Exception:
            leaves = None
        if leaves:
            df2 = pd.DataFrame(leaves, columns=["stop_id", "metadata"])
            df2.to_csv(out_dir / "gtfs_stops.csv.gz", index=False, compression="gzip")
        bpt_name = "gtfs_bpt.pkl.gz" if compress else "gtfs_bpt.pkl"
        save_pickle(bpt, out_dir / bpt_name, compress=compress)

    return out_dir

def load_graph_from_processed(root: Path, run_name: str = "run_auto") -> Tuple[object, Optional[object]]:
    """
    Intenta cargar (graph, bpt) desde data/processed/run_name.
    - Primero intenta deserializar los pickles con load_pickle (compat unpickler).
    - Si la deserialización del grafo falla (por TypeError u otros),
      reconstruye el grafo desde gtfs_edges.csv.gz (o gtfs_edges.csv).
    - Para bpt: intenta cargar pickle; si falla, lo deja como None.
    """
    base = Path(root) / "data" / "processed" / run_name

    # ---- Intentar cargar grafo desde pickle (preferido) ----
    gp_gz = base / "gtfs_graph.pkl.gz"
    gp = base / "gtfs_graph.pkl"
    graph = None

    try:
        if gp_gz.exists():
            try:
                graph = load_pickle(gp_gz)
            except Exception as e:
                # fallo al deserializar; lo manejamos abajo
                print("Warning: fallo al deserializar gtfs_graph.pkl.gz:", e)
                graph = None
        elif gp.exists():
            try:
                graph = load_pickle(gp)
            except Exception as e:
                print("Warning: fallo al deserializar gtfs_graph.pkl:", e)
                graph = None
    except Exception as e:
        # cualquier otro fallo no esperado
        print("Warning: error inesperado intentando cargar pickle:", e)
        graph = None

    # ---- Si no se pudo cargar pickle, reconstruir desde CSV de aristas ----
    if graph is None:
        edges_csv_gz = base / "gtfs_edges.csv.gz"
        edges_csv = base / "gtfs_edges.csv"
        edges_path = None
        if edges_csv_gz.exists():
            edges_path = edges_csv_gz
            comp = "gzip"
        elif edges_csv.exists():
            edges_path = edges_csv
            comp = None
        else:
            raise FileNotFoundError(f"No encontré ni gtfs_graph.pkl(.gz) ni gtfs_edges.csv(.gz) en {base}")

        print("Reconstruyendo Graph a partir de", edges_path)
        try:
            # lee con pandas (usa compression si .gz)
            if comp == "gzip":
                df = _pd.read_csv(edges_path, compression="gzip")
            else:
                df = _pd.read_csv(edges_path)
        except Exception as e:
            raise RuntimeError(f"Error leyendo {edges_path}: {e}")

        # construir Graph vacío acorde a src/transport_opt/graph/model.py
        from transport_opt.graph.model import Graph
        g = Graph(directed=False)
        # añadir nodos implícitamente por aristas
        for _, row in df.iterrows():
            u = str(row["u"])
            v = str(row["v"])
            try:
                w = float(row["w"])
            except Exception:
                w = 1.0
            g.add_edge(u, v, weight=w)
        graph = g
        print("Graph reconstruido desde CSV. Nodos:", len(graph.nodes), "Aristas (muestra):", list(df.head(10).itertuples(index=False, name=None)))

    # ---- Intentar cargar B+Tree (bpt) ----
    bpt = None
    bpt_gz = base / "gtfs_bpt.pkl.gz"
    bpt_p = base / "gtfs_bpt.pkl"
    if bpt_gz.exists():
        try:
            bpt = load_pickle(bpt_gz)
        except Exception as e:
            print("Warning: fallo al deserializar gtfs_bpt.pkl.gz:", e)
            bpt = None
    elif bpt_p.exists():
        try:
            bpt = load_pickle(bpt_p)
        except Exception as e:
            print("Warning: fallo al deserializar gtfs_bpt.pkl:", e)
            bpt = None
    else:
        # no hay bpt; no es fatal
        bpt = None

    return graph, bpt