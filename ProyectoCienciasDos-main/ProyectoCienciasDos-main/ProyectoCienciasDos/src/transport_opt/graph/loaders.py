# src/transport_opt/graph/loaders.py
import zipfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt
from typing import Tuple
from transport_opt.graph.model import Graph
from transport_opt.db.bplustree import BPlusTree

def _hms_to_seconds(t: str) -> int:
    """Convierte 'HH:MM:SS' (puede tener 24:xx:xx) a segundos desde medianoche."""
    if pd.isna(t):
        return None
    parts = t.split(':')
    if len(parts) != 3:
        return None
    h, m, s = map(int, parts)
    return h * 3600 + m * 60 + s

def haversine(lat1, lon1, lat2, lon2) -> float:
    # devuelve distancia en kilómetros
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    return km

def read_gtfs_tables(gtfs_zip_path: str):
    with zipfile.ZipFile(gtfs_zip_path) as zf:
        def read(name):
            try:
                with zf.open(name) as fh:
                    return pd.read_csv(fh, dtype=str)
            except KeyError:
                return None
        stops = read('stops.txt')
        stop_times = read('stop_times.txt')
        trips = read('trips.txt')
        routes = read('routes.txt')
        shapes = read('shapes.txt')
        frequencies = read('frequencies.txt')
    return dict(stops=stops, stop_times=stop_times, trips=trips, routes=routes, shapes=shapes, frequencies=frequencies)

def build_graph_from_gtfs(gtfs_zip_path: str, insert_to_bpt: bool = True, bpt_order: int = 32) -> Tuple[Graph, BPlusTree]:
    data = read_gtfs_tables(gtfs_zip_path)
    stops = data['stops']
    stop_times = data['stop_times']
    trips = data['trips']
    routes = data['routes']
    frequencies = data['frequencies']

    if stops is None or stop_times is None or trips is None or routes is None:
        raise ValueError("GTFS missing required files: stops, stop_times, trips or routes")

    # normalizar tipos
    stops['stop_id'] = stops['stop_id'].astype(str)
    stop_times['trip_id'] = stop_times['trip_id'].astype(str)
    stop_times['stop_id'] = stop_times['stop_id'].astype(str)

    # Construir Graph
    g = Graph(directed=False)  # SITP normalmente bidireccional por carreteras

    # B+ tree para indexado de stops (si se pide)
    bpt = None
    if insert_to_bpt:
        bpt = BPlusTree(order=bpt_order)

    # Insertar nodos (stops)
    for _, row in stops.iterrows():
        sid = row['stop_id']
        name = row.get('stop_name', '')
        lat = float(row['stop_lat']) if 'stop_lat' in row and pd.notna(row['stop_lat']) else None
        lon = float(row['stop_lon']) if 'stop_lon' in row and pd.notna(row['stop_lon']) else None
        info = {'id': sid, 'name': name, 'lat': lat, 'lon': lon}
        g.add_node(sid)
        if bpt:
            bpt.insert(sid, info)

    # preparar stop_times agrupados por trip_id, ordenados por stop_sequence
    stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(float)
    grouped = stop_times.sort_values(['trip_id','stop_sequence']).groupby('trip_id')

    # crear un map route_id -> route info
    route_map = {}
    if routes is not None:
        for _, r in routes.iterrows():
            route_map[str(r['route_id'])] = r.to_dict()

    # Si frequencies present, construir dict trip_id -> headway (seconds)
    freq_map = {}
    if frequencies is not None and not frequencies.empty:
        # frequencies may have trip_id OR (start_time, end_time, headway_secs) for a route; GTFS spec: may include trip_id
        if 'trip_id' in frequencies.columns:
            for _, f in frequencies.iterrows():
                if pd.notna(f.get('trip_id')):
                    freq_map[str(f['trip_id'])] = int(float(f['headway_secs']))
        else:
            # fallback: apply to trips by route_id + service_id window (approx)
            pass

    # Helper default speed: 18 km/h (0.3 km/min) → 18/60 = 0.3 km/min → 0.005 km/s
    default_speed_kmh = 18.0

    # crear edges recorriendo cada trip
    for trip_id, group in grouped:
        seq = list(group[['stop_id','arrival_time','departure_time']].itertuples(index=False, name=None))
        for i in range(len(seq)-1):
            u, arrival_u, departure_u = seq[i]
            v, arrival_v, departure_v = seq[i+1]
            # calcular travel_time en segundos si arrival/departure disponibles
            t_u = _hms_to_seconds(departure_u) or _hms_to_seconds(arrival_u)
            t_v = _hms_to_seconds(arrival_v) or _hms_to_seconds(departure_v)
            travel_secs = None
            if t_u is not None and t_v is not None:
                travel_secs = int(t_v - t_u)
                # manejar wrap-around si negativo (ej 25:00:00)
                if travel_secs < 0:
                    travel_secs += 24*3600
            else:
                # estimar por distancia y velocidad
                srow = stops[stops['stop_id']==u]
                trow = stops[stops['stop_id']==v]
                if not srow.empty and not trow.empty and pd.notna(srow.iloc[0].get('stop_lat')) and pd.notna(trow.iloc[0].get('stop_lat')):
                    lat1 = float(srow.iloc[0]['stop_lat']); lon1 = float(srow.iloc[0]['stop_lon'])
                    lat2 = float(trow.iloc[0]['stop_lat']); lon2 = float(trow.iloc[0]['stop_lon'])
                    km = haversine(lat1, lon1, lat2, lon2)
                    # time in seconds = (km / kmh) * 3600
                    travel_secs = max(10, int((km / default_speed_kmh) * 3600))  # mínimo 10s
                else:
                    travel_secs = 60  # fallback 1 min

            # peso en minutos por compatibilidad con tu Graph existente (usa minutos)
            weight_minutes = travel_secs / 60.0

            # get route/trip attributes optionally
            # encuentra route_id desde trips.csv (si existe)
            route_id = None
            # trips table might not be None
            if trips is not None and 'trip_id' in trips.columns:
                match = trips[trips['trip_id']==trip_id]
                if not match.empty and 'route_id' in match.columns:
                    route_id = str(match.iloc[0]['route_id'])
            # frequency/headway si está en freq_map
            headway = freq_map.get(trip_id, None)

            # agregar arista al grafo (podrías agregar multiplicidad si hay frecuencias)
            # para no duplicar demasiado, se hace add_edge una vez con atributos
            # Si ya existe arista entre u-v, podrías agregar metadata agregada (min weight, sum capacity)
            g.add_edge(u, v, weight=weight_minutes)

            # TODO: guardar metadata por arista (frecuencia, trip_id, route_id)
            # tu Graph actual solo tiene weight; si quieres metadata, podrías tener dict externo
    return g, bpt
