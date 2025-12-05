# src/transport_opt/graph/gtfs_utils.py
import zipfile
import pandas as pd
from collections import defaultdict
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from typing import Dict, Tuple, Optional

def _hms_to_seconds(t: Optional[str]) -> Optional[int]:
    if t is None or pd.isna(t):
        return None
    parts = str(t).split(':')
    if len(parts) != 3:
        return None
    h, m, s = map(int, parts)
    return h * 3600 + m * 60 + s

def haversine(lat1, lon1, lat2, lon2) -> float:
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return 6371 * c

def build_capacity_from_gtfs(
    gtfs_zip_path: str,
    graph,
    *,
    vehicle_capacity_map: Optional[Dict[str,int]] = None,
    observation_window_hours: Optional[float] = None
) -> Dict[str, Dict[str, int]]:
    """
    Estima una matriz 'capacity[u][v] = pasajeros por minuto' a partir del GTFS y del grafo.
    - vehicle_capacity_map: override por modo ('metro','tm','sitp') -> pasajeros/vehículo.
      defaults: metro=1000, tm=160, sitp=80
    - observation_window_hours: si None, se estima a partir de horarios en GTFS (desde primeros departures).
    Devuelve capacity como dict anidado apto para edmonds_karp.
    Además anexa metadata por arista en graph.edge_meta (dict (u,v) -> metadata).
    """
    if vehicle_capacity_map is None:
        vehicle_capacity_map = {'metro': 1000, 'tm': 160, 'sitp': 80, 'default': 80}

    # leer tablas
    with zipfile.ZipFile(gtfs_zip_path) as zf:
        def _read(name):
            try:
                with zf.open(name) as fh:
                    return pd.read_csv(fh, dtype=str)
            except KeyError:
                return None
        stops = _read('stops.txt')
        stop_times = _read('stop_times.txt')
        trips = _read('trips.txt')
        routes = _read('routes.txt')
        frequencies = _read('frequencies.txt')

    if stop_times is None or trips is None or routes is None:
        raise ValueError("GTFS missing required files (stop_times/trips/routes)")

    # tipos y limpieza
    stop_times['trip_id'] = stop_times['trip_id'].astype(str)
    stop_times['stop_id'] = stop_times['stop_id'].astype(str)
    stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(float)
    trips['trip_id'] = trips['trip_id'].astype(str)
    trips['route_id'] = trips['route_id'].astype(str)

    # map trip -> route
    trip_to_route = dict(zip(trips['trip_id'], trips['route_id']))

    # obtener start times por trip (departure_time de la primera parada)
    first_stop = stop_times.sort_values(['trip_id','stop_sequence']).groupby('trip_id').first().reset_index()
    first_stop['start_secs'] = first_stop['departure_time'].apply(_hms_to_seconds)
    # si hay NaN en start_secs, intentar arrival_time
    mask_na = first_stop['start_secs'].isna()
    if 'arrival_time' in first_stop.columns:
        first_stop.loc[mask_na, 'start_secs'] = first_stop.loc[mask_na, 'arrival_time'].apply(_hms_to_seconds)

    # construir lista de edges por trip y contar ocurrencias por arista
    edge_counts = defaultdict(int)
    edge_trips = defaultdict(set)  # para metadata: arista -> set(trip_ids)
    # iterar por trip (ordenado por stop_sequence)
    grouped = stop_times.sort_values(['trip_id','stop_sequence']).groupby('trip_id')
    for trip_id, group in grouped:
        seq = list(group['stop_id'])
        for i in range(len(seq)-1):
            u = seq[i]; v = seq[i+1]
            edge_counts[(u,v)] += 1
            edge_trips[(u,v)].add(trip_id)

    # estimar ventana de observacion en segundos
    min_start = first_stop['start_secs'].min()
    max_start = first_stop['start_secs'].max()
    if pd.isna(min_start) or pd.isna(max_start) or min_start==max_start:
        window_secs = observation_window_hours * 3600 if observation_window_hours else 3600
    else:
        window_secs = (max_start - min_start) if (max_start - min_start) > 0 else 3600
        # si window demasiado pequeño, usar al menos 1 hora
        window_secs = max(window_secs, 3600)

    # construir capacity: vehicles per minute * vehicle_capacity
    capacity = {}
    # ensure graph has edge_meta container
    try:
        edge_meta = getattr(graph, 'edge_meta', None)
        if edge_meta is None:
            graph.edge_meta = {}
            edge_meta = graph.edge_meta
    except Exception:
        # si graph no permite atributos, usar variable local (pero intentaremos asignar)
        edge_meta = {}

    for (u,v), cnt in edge_counts.items():
        # vehículos por segundo aproximadamente = cnt / window_secs
        veh_per_min = (cnt / window_secs) * 60.0  # vehículos por minuto
        # inferir mode: heurística a partir del id (prefijo) o de trips/routes si disponibles
        mode = 'default'
        # check trips -> route -> route_type or route_short_name
        sample_trip = next(iter(edge_trips[(u,v)]))
        route_id = trip_to_route.get(sample_trip)
        route_row = None
        if routes is not None and route_id is not None and 'route_id' in routes.columns:
            matches = routes[routes['route_id'].astype(str)==str(route_id)]
            if not matches.empty:
                route_row = matches.iloc[0].to_dict()
                # heurística por route_type si existe
                if 'route_type' in matches.columns:
                    rt = str(route_row.get('route_type','')).strip()
                    # GTFS route_type: 1=subway,3=bus (varía) --> map roughly
                    if rt in ('1','2','0'):
                        mode = 'metro'
                    elif rt in ('3','3.0','3.00','3'):
                        mode = 'tm'  # considerar como bus/trunk
                    else:
                        # fallback by route_short_name prefix
                        short = str(route_row.get('route_short_name','')).upper()
                        if short.startswith('M'):
                            mode = 'metro'
                        elif short.startswith('T'):
                            mode = 'tm'
                        else:
                            mode = 'sitp'
                else:
                    # fallback: short name prefix
                    short = str(route_row.get('route_short_name','')).upper()
                    if short.startswith('M'):
                        mode = 'metro'
                    elif short.startswith('T'):
                        mode = 'tm'
                    else:
                        mode = 'sitp'
        else:
            # fallback by stop id prefix (common in tu feed)
            if str(u).startswith('M') or str(v).startswith('M'):
                mode = 'metro'
            elif str(u).startswith('T') or str(v).startswith('T'):
                mode = 'tm'
            else:
                mode = 'sitp'

        veh_cap = vehicle_capacity_map.get(mode, vehicle_capacity_map.get('default', 80))
        # capacity in passengers per minute
        cap_pax_per_min = max(1, int(round(veh_per_min * veh_cap)))

        # guardar en capacity dict (dirigido)
        capacity.setdefault(u, {})[v] = cap_pax_per_min

        # metadata
        meta = {
            'edge_trip_count': cnt,
            'trip_examples': list(edge_trips[(u,v)])[:5],
            'route_id': route_id,
            'route_meta': route_row,
            'mode': mode,
            'veh_per_min': veh_per_min,
            'veh_capacity': veh_cap,
            'pax_per_min_est': cap_pax_per_min,
            'window_secs': window_secs
        }
        edge_meta[(u,v)] = meta

    # also ensure capacity entries exist for nodes without outgoing edges
    for n in graph.nodes:
        capacity.setdefault(n, {})

    # assign back edge_meta
    try:
        setattr(graph, 'edge_meta', edge_meta)
    except Exception:
        pass

    return capacity
