# src/transport_opt/sim/simulator.py
from typing import List, Dict, Any, Optional, Tuple
import random
import math
from collections import defaultdict, deque
from transport_opt.graph.algorithms import shortest_path

class Simulator:
    """
    Simulador por eventos discretos en pasos de 1 minuto (ventana configurable).
    - Usa rutas por camino más corto (Dijkstra ya en algorithms.shortest_path).
    - Respeta capacidad por arista (pasajeros por minuto).
    - Si hay headways en g.edge_meta, ajusta la capacidad usando vehicle_capacity.
    """

    def __init__(
        self,
        graph,
        capacity: Optional[Dict[str, Dict[str, int]]] = None,
        vehicle_capacity: int = 50,
        base_capacity_per_minute: int = 30
    ):
        self.g = graph
        self.vehicle_capacity = int(vehicle_capacity)
        self.base_capacity_per_minute = int(base_capacity_per_minute)
        # capacidad base (por minuto) si no se provee
        self.capacity = capacity if capacity is not None else self._derive_capacity_from_graph()

    def _derive_capacity_from_graph(self) -> Dict[str, Dict[str, int]]:
        """
        Crea una capacidad heurística basada en:
         - base_capacity_per_minute
         - multiplicadores por tipo (si el id empieza con 'M' o 'T')
         - si g.edge_meta tiene headways (en segundos), los usa para estimar vehículos/minuto
        """
        cap = {}
        for u in self.g.nodes:
            cap[u] = {}
        for u in self.g.adj:
            for v, w in self.g.adj[u]:
                # heurística base
                mult = 1
                if str(u).startswith('M') or str(v).startswith('M'):
                    mult = 3
                elif str(u).startswith('T') or str(v).startswith('T'):
                    mult = 2
                base = int(self.base_capacity_per_minute * mult)

                # intentar mejorar usando metadata headways
                eff = base
                try:
                    # canonical key like in loaders (for undirected graphs keys sorted)
                    key = (u, v) if self.g.directed or u <= v else (v, u)
                    meta = getattr(self.g, "edge_meta", {}).get(key)
                    if meta and meta.get("headways"):
                        # tomo el headway mínimo (más frecuente) en segundos
                        mins = [int(h) for h in meta["headways"] if h is not None]
                        if mins:
                            min_headway_secs = min(mins)
                            if min_headway_secs > 0:
                                vehicles_per_min = 60.0 / float(min_headway_secs)
                                eff_by_headway = max(base, int(round(vehicles_per_min * self.vehicle_capacity)))
                                eff = eff_by_headway
                except Exception:
                    pass
                cap[u][v] = int(eff)
        return cap

    def run(self, demand: List[Tuple[str, str, float]], window_seconds: int = 3600) -> Dict[str, Any]:
        """
        demand: lista de (origin, destination, rate_per_hour)
        window_seconds: ventana de simulación en segundos (por defecto 1 hora)
        Retorna un dict con métricas, cargas por arista, y resultados por pasajero.
        """
        # convertir rates a tasa por minuto
        minutes = max(1, int(math.ceil(window_seconds / 60)))
        arrivals_per_minute = []
        passengers = []
        pid = 0
        for origin, dest, rate_h in demand:
            # tasa por minuto
            rpm = float(rate_h) / 60.0
            # número total muestreado (heurístico)
            total = max(1, int(round(rate_h / 10.0)))  # similar a tu heurística previa
            for _ in range(total):
                # asigna minuto de llegada de forma uniforme en la ventana
                arrive_min = random.randint(0, minutes - 1)
                passengers.append({
                    "pid": pid,
                    "origin": origin,
                    "dest": dest,
                    "arrival_min": arrive_min
                })
                pid += 1

        # ordenar por llegada
        passengers.sort(key=lambda x: x["arrival_min"])

        # edge usage tracker por minuto: capacity_remaining[minute][(u,v)] = remaining cap
        # Para eficiencia almacenamos por minuto un dict de capacidades inicialmente copiado de self.capacity
        capacity_by_min = [None] * minutes
        for m in range(minutes):
            cap_copy = {}
            for u, nbrs in self.capacity.items():
                cap_copy[u] = dict(nbrs)  # shallow copy es suficiente (valores ints)
            capacity_by_min[m] = cap_copy

        # resultados
        results = []
        edge_load_total = defaultdict(int)
        blocked = 0

        # process each passenger sequentially (arrive_min -> attempt to traverse)
        for p in passengers:
            cur_min = p["arrival_min"]
            origin = p["origin"]
            dest = p["dest"]

            # get path + travel distance (minutes) using existing shortest_path (weights are minutes)
            path, travel_minutes_nominal = shortest_path(self.g, origin, dest)
            if path is None:
                results.append((p["pid"], None, math.inf))
                blocked += 1
                continue

            # simulate traversal minute-by-minute along path edges
            # start_time in minutes (can be fractional if edge weight <1, we discretize to minutes)
            t_min = cur_min
            wait_time = 0
            traversed = True
            # iterate edges
            for i in range(len(path) - 1):
                u = path[i]; v = path[i + 1]
                # integer minutes needed to traverse (ceil of weight)
                # but boarding consumes capacity in the minute of traversal start
                # weight may be fractional: convert to number of minutes to traverse
                # we assume traversal uses capacity at minute when passenger boards.
                w = None
                # find weight in graph adj list
                for nb, wt in self.g.adj[u]:
                    if nb == v:
                        w = float(wt)
                        break
                if w is None:
                    # fallback 1 minute
                    w = 1.0
                needed_minutes = max(1, int(math.ceil(w)))  # occupancy rounds up

                # try to find minute(s) with enough capacity to board at t_min
                boarded = False
                attempt_min = t_min
                while attempt_min < minutes:
                    cap_map = capacity_by_min[attempt_min]
                    rem = cap_map.get(u, {}).get(v, 0)
                    if rem >= 1:
                        # consume 1 passenger capacity at boarding minute
                        cap_map[u][v] -= 1
                        # count load
                        edge_load_total[(u, v)] += 1
                        # passenger will be "in transit" for needed_minutes, so next edge boarding earliest at attempt_min + needed_minutes
                        t_min = attempt_min + needed_minutes
                        boarded = True
                        break
                    else:
                        # wait one minute and try again
                        attempt_min += 1
                        wait_time += 1
                if not boarded:
                    # couldn't board within the window
                    traversed = False
                    break

            if not traversed:
                results.append((p["pid"], path, math.inf))
                blocked += 1
            else:
                total_travel_time = (t_min - cur_min)  # minutos incluyendo waits and travel
                results.append((p["pid"], path, total_travel_time))

        # aggregate metrics
        travel_times = [r[2] for r in results if r[2] < math.inf]
        avg_travel = sum(travel_times) / len(travel_times) if travel_times else math.inf

        return {
            "passenger_results": results,
            "edge_load": dict(edge_load_total),
            "avg_travel_time": avg_travel,
            "total_passengers": len(passengers),
            "blocked": blocked
        }
