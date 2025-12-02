import pandas as pd
import numpy as np
import zipfile
from math import radians, cos, sin, asin, sqrt
from typing import Tuple
from transport_opt.graph.model import Graph
from transport_opt.db.bplustree import BPlusTree

# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def _hms_to_seconds(t: str):
    """Convierte HH:MM:SS a segundos. Devuelve None si no es válido."""
    if t is None or pd.isna(t):
        return None
    try:
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s
    except:
        return None

def haversine(lat1, lon1, lat2, lon2):
    """Distancia Haversine en kilómetros."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return 6371 * c

# ============================================================
# LECTOR DE GTFS (OPTIMIZADO: SOLO LEE LAS COLUMNAS NECESARIAS)
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
            "routes": read_file("routes.txt", usecols=["route_id", "route_short_name"]),
            "frequencies": read_file("frequencies.txt")
        }

# ============================================================
# FUNCIÓN PRINCIPAL — ARMADO DEL GRAFO (HIPER OPTIMIZADO)
# ============================================================

def build_graph_from_gtfs(gtfs_zip_path: str, insert_to_bpt: bool = True, bpt_order: int = 32) -> Tuple[Graph, BPlusTree]:

    print("Leyendo archivos GTFS...")
    data = read_gtfs_tables(gtfs_zip_path)

    stops = data["stops"]
    stop_times = data["stop_times"]
    trips = data["trips"]
    routes = data["routes"]

    if stops is None or stop_times is None or trips is None or routes is None:
        raise ValueError("GTFS missing required files")

    # Convertir tipos
    stops["stop_id"] = stops["stop_id"].astype(str)
    stop_times["trip_id"] = stop_times["trip_id"].astype(str)
    stop_times["stop_id"] = stop_times["stop_id"].astype(str)
    stop_times["stop_sequence"] = stop_times["stop_sequence"].astype(float)

    # Merge rápido (1 solo merge en cadena)
    print("Fusionando tablas (merge)...")
    stop_times = stop_times.merge(trips, on="trip_id", how="left")\
                           .merge(routes, on="route_id", how="left")

    # Crear grafo y B+Tree
    print("Inicializando estructuras...")
    g = Graph(directed=False)
    bpt = BPlusTree(order=bpt_order) if insert_to_bpt else None

    # Insertar nodos
    print("Insertando nodos...")
    for _, row in stops.iterrows():
        sid = row["stop_id"]
        info = {
            "id": sid,
            "name": row.get("stop_name", ""),
            "lat": float(row["stop_lat"]) if pd.notna(row["stop_lat"]) else None,
            "lon": float(row["stop_lon"]) if pd.notna(row["stop_lon"]) else None,
        }
        g.add_node(sid)
        if bpt:
            bpt.insert(sid, info)

    # Ordenar por trip_id + secuencia
    print("Ordenando secuencias de viaje...")
    stop_times_sorted = stop_times.sort_values(["trip_id", "stop_sequence"])

    trip_ids = stop_times_sorted["trip_id"].values
    stop_ids = stop_times_sorted["stop_id"].values
    arr = stop_times_sorted["arrival_time"].values
    dep = stop_times_sorted["departure_time"].values

    # Index rápido de coordenadas
    coords_lat = stops.set_index("stop_id")["stop_lat"].astype(float)
    coords_lon = stops.set_index("stop_id")["stop_lon"].astype(float)

    default_speed_kmh = 18

    print("Construyendo aristas (optimizado)...")
    for i in range(1, len(stop_times_sorted)):
        # Solo conectar stops del mismo viaje
        if trip_ids[i] != trip_ids[i - 1]:
            continue

        u = stop_ids[i - 1]
        v = stop_ids[i]

        # Tiempos
        t_u = _hms_to_seconds(dep[i - 1]) or _hms_to_seconds(arr[i - 1])
        t_v = _hms_to_seconds(arr[i]) or _hms_to_seconds(dep[i])

        # Si hay tiempos reales
        if t_u is not None and t_v is not None:
            travel_secs = t_v - t_u
            if travel_secs < 0:  # cruce de medianoche
                travel_secs += 24 * 3600
        else:
            # Estimación usando Haversine
            la1 = coords_lat.get(u)
            lo1 = coords_lon.get(u)
            la2 = coords_lat.get(v)
            lo2 = coords_lon.get(v)

            km = haversine(la1, lo1, la2, lo2)
            travel_secs = max(10, int((km / default_speed_kmh) * 3600))

        g.add_edge(u, v, weight=travel_secs / 60)

    print("Grafo construido exitosamente.")
    return g, bpt

