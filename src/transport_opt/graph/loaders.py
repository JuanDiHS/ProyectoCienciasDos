# src/transport_opt/graph/loaders.py
import pandas as pd
import numpy as np
import zipfile
from math import radians, cos, sin, asin, sqrt
from typing import Tuple, Optional
from collections import defaultdict
from transport_opt.graph.model import Graph
from transport_opt.db.bplustree import BPlusTree

# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def _hms_to_seconds(t: Optional[str]):
    """Convierte HH:MM:SS a segundos. Devuelve None si no es válido."""
    if t is None or pd.isna(t):
        return None
    try:
        h, m, s = map(int, str(t).split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return None

def haversine(lat1, lon1, lat2, lon2) -> float:
    """Distancia Haversine en kilómetros. Si faltan coordenadas devuelve 0.0"""
    try:
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            return 0.0
    except Exception:
        return 0.0
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return 6371.0 * c

# ============================================================
# LECTOR DE GTFS (LEE COLUMNAS NECESARIAS)
# ============================================================

def read_gtfs_tables(gtfs_zip_path: str):
    with zipfile.ZipFile(gtfs_zip_path) as zf:
        def read_file(name, usecols=None):
            try:
                with zf.open(name) as fh:
                    return pd.read_csv(fh, dtype=str, usecols=usecols)
            except KeyError:
                return None

        return {
            "stops": read_file("stops.txt", usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"]),
            "stop_times": read_file("stop_times.txt", usecols=[
                "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"
            ]),
            "trips": read_file("trips.txt", usecols=["trip_id", "route_id"]),
            "routes": read_file("routes.txt", usecols=["route_id", "route_short_name", "route_type"]),
            "frequencies": read_file("frequencies.txt")
        }

# ============================================================
# FUNCIÓN PRINCIPAL — ARMADO DEL GRAFO (CON METADATA)
# ============================================================

def build_graph_from_gtfs(gtfs_zip_path: str, insert_to_bpt: bool = True, bpt_order: int = 32) -> Tuple[Graph, Optional[BPlusTree]]:
    """
    Construye un Graph a partir de un GTFS zip.
    Devuelve (graph, bplustree_or_None). El grafo contiene g.edge_meta: dict[(u,v)] -> metadata.
    """
    print("Leyendo archivos GTFS...")
    data = read_gtfs_tables(gtfs_zip_path)

    stops = data["stops"]
    stop_times = data["stop_times"]
    trips = data["trips"]
    routes = data["routes"]
    frequencies = data.get("frequencies", None)

    if stops is None or stop_times is None or trips is None or routes is None:
        raise ValueError("GTFS missing required files (stops, stop_times, trips, routes required)")

    # Convertir tipos mínimos y limpiar
    stops["stop_id"] = stops["stop_id"].astype(str)
    stop_times["trip_id"] = stop_times["trip_id"].astype(str)
    stop_times["stop_id"] = stop_times["stop_id"].astype(str)
    stop_times["stop_sequence"] = stop_times["stop_sequence"].astype(float)
    trips["trip_id"] = trips["trip_id"].astype(str)
    trips["route_id"] = trips["route_id"].astype(str)

    # Merge stop_times <- trips <- routes (rápido)
    print("Fusionando tablas (merge)...")
    stop_times = stop_times.merge(trips, on="trip_id", how="left")
    stop_times = stop_times.merge(routes, on="route_id", how="left")

    # Preparar mapping trip->route (si se necesita)
    trip_to_route = {}
    if trips is not None and not trips.empty:
        trip_to_route = dict(zip(trips["trip_id"].astype(str), trips["route_id"].astype(str)))

    # Preparar headways por trip (si frequencies tiene trip_id)
    trip_headways = {}
    if frequencies is not None and not frequencies.empty:
        if "trip_id" in frequencies.columns and "headway_secs" in frequencies.columns:
            for _, r in frequencies.iterrows():
                tid = str(r.get("trip_id")) if pd.notna(r.get("trip_id")) else None
                try:
                    secs = int(float(r.get("headway_secs"))) if pd.notna(r.get("headway_secs")) else None
                except Exception:
                    secs = None
                if tid:
                    trip_headways.setdefault(tid, []).append(secs)

    # Crear grafo y B+Tree
    print("Inicializando estructuras...")
    g = Graph(directed=False)
    bpt = BPlusTree(order=bpt_order) if insert_to_bpt else None

    # Insertar nodos (stops)
    print("Insertando nodos...")
    for _, row in stops.iterrows():
        sid = str(row["stop_id"])
        info = {
            "id": sid,
            "name": row.get("stop_name", "") if pd.notna(row.get("stop_name")) else "",
            "lat": float(row["stop_lat"]) if pd.notna(row.get("stop_lat")) else None,
            "lon": float(row["stop_lon"]) if pd.notna(row.get("stop_lon")) else None,
        }
        g.add_node(sid)
        if bpt:
            bpt.insert(sid, info)

    # Index rápido de coordenadas (puede contener NaN)
    coords_lat = stops.set_index("stop_id")["stop_lat"].astype(float)
    coords_lon = stops.set_index("stop_id")["stop_lon"].astype(float)

    # Preparar arrays ordenados para iteración rápida
    print("Ordenando secuencias de viaje...")
    stop_times_sorted = stop_times.sort_values(["trip_id", "stop_sequence"]).reset_index(drop=True)
    trip_ids = stop_times_sorted["trip_id"].to_numpy()
    stop_ids = stop_times_sorted["stop_id"].to_numpy()
    arr = stop_times_sorted["arrival_time"].to_numpy()
    dep = stop_times_sorted["departure_time"].to_numpy()

    # Si están, sacar route arrays para heurísticas de modo
    route_short_arr = stop_times_sorted["route_short_name"].to_numpy() if "route_short_name" in stop_times_sorted.columns else None
    route_type_arr = stop_times_sorted["route_type"].to_numpy() if "route_type" in stop_times_sorted.columns else None

    default_speed_kmh = 18.0

    # metadata container: canonical key -> metadata
    # canonical key: (u,v) ordenado para grafo no dirigido
    g.edge_meta = {}

    print("Construyendo aristas y metadata (optimizado)...")
    n = len(stop_times_sorted)
    for i in range(1, n):
        # solo conectar si es el mismo viaje
        if trip_ids[i] != trip_ids[i - 1]:
            continue

        u = str(stop_ids[i - 1])
        v = str(stop_ids[i])
        trip_id = str(trip_ids[i])

        # tiempos (departure/arrival) -> segundos
        t_u = _hms_to_seconds(dep[i - 1]) or _hms_to_seconds(arr[i - 1])
        t_v = _hms_to_seconds(arr[i]) or _hms_to_seconds(dep[i])

        if t_u is not None and t_v is not None:
            travel_secs = t_v - t_u
            if travel_secs < 0:
                travel_secs += 24 * 3600  # wrap-around
        else:
            # fallback por distancia
            la1 = coords_lat.get(u)
            lo1 = coords_lon.get(u)
            la2 = coords_lat.get(v)
            lo2 = coords_lon.get(v)
            km = haversine(la1, lo1, la2, lo2) if (la1 is not None and la2 is not None) else 0.0
            travel_secs = max(10, int((km / default_speed_kmh) * 3600))

        weight_minutes = travel_secs / 60.0
        g.add_edge(u, v, weight=weight_minutes)

        # canonical key para no duplicar en grafo no dirigido
        key = (u, v) if g.directed or u <= v else (v, u)

        # inicializar meta si no existe
        if key not in g.edge_meta:
            g.edge_meta[key] = {
                "trip_ids": set(),
                "route_ids": set(),
                "modes": set(),
                "headways": []
            }
        meta = g.edge_meta[key]

        # trip id
        meta["trip_ids"].add(trip_id)

        # route id: prefer mapping trips -> route, si no usar columna merged
        route_id = trip_to_route.get(trip_id)
        if route_id is None and "route_id" in stop_times_sorted.columns:
            try:
                route_id = stop_times_sorted.iloc[i]["route_id"]
            except Exception:
                route_id = None
        if pd.notna(route_id) and route_id is not None:
            meta["route_ids"].add(str(route_id))

        # mode inference: route_type > route_short prefix > stop id prefixes
        mode = "sitp"
        if route_type_arr is not None and not pd.isna(route_type_arr[i]):
            rt = str(route_type_arr[i]).strip()
            # heurístico simple (GTFS route_type numbers vary)
            if rt in ("0", "1", "2"):
                mode = "metro"
            elif rt in ("3",):
                mode = "tm"
            else:
                mode = "sitp"
        elif route_short_arr is not None and isinstance(route_short_arr[i], str):
            rs = route_short_arr[i].strip().upper()
            if rs.startswith("M"):
                mode = "metro"
            elif rs.startswith("T"):
                mode = "tm"
            else:
                mode = "sitp"
        else:
            if u.startswith("M") or v.startswith("M"):
                mode = "metro"
            elif u.startswith("T") or v.startswith("T"):
                mode = "tm"

        meta["modes"].add(mode)

        # headways (si existen para el trip)
        if trip_headways.get(trip_id):
            meta["headways"].extend([int(h) for h in trip_headways[trip_id] if h is not None])

    # (opcional) convert sets to lists for easier serialization later
    for k, m in g.edge_meta.items():
        m["trip_ids"] = list(m["trip_ids"])
        m["route_ids"] = list(m["route_ids"])
        m["modes"] = list(m["modes"])
        # headways ya es lista de ints (posiblemente vacía)

    print("Grafo construido exitosamente. Aristas con metadata:", len(g.edge_meta))
    return g, bpt

