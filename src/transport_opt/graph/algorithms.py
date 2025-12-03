# src/transport_opt/graph/algorithms.py
"""
Algoritmos de grafos para el proyecto de optimización de transporte:
- Dijkstra / shortest_path
- A* (astar) con heurística Haversine (minutos)
- Kruskal (MST) con UnionFind
- Edmonds-Karp (max flow)
- Utilidad para extraer stops_info desde un B+Tree
"""
import heapq
import math
from collections import deque, defaultdict
from typing import Dict, Tuple, Any, Optional, List, Callable

from .model import Graph, Edge

# -------------------------
# DIJKSTRA + UTIL
# -------------------------
def dijkstra(graph: Graph, source: str) -> Tuple[Dict[str, float], Dict[str, Optional[str]]]:
    """
    Dijkstra clásico.
    graph: Graph (debe exponer graph.nodes y graph.neighbors(u) -> list[(v, weight)])
    source: id del nodo origen (string)
    retorna: (dist, prev) donde dist[node] = costo mínimo y prev[node] = predecessor
    """
    if source not in graph.nodes:
        raise KeyError(f"Source node '{source}' not in graph")

    dist: Dict[str, float] = {node: math.inf for node in graph.nodes}
    prev: Dict[str, Optional[str]] = {node: None for node in graph.nodes}
    dist[source] = 0.0
    pq: List[Tuple[float, str]] = [(0.0, source)]
    visited = set()

    while pq:
        d, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        for v, w in graph.neighbors(u):
            alt = d + float(w)
            if alt < dist[v]:
                dist[v] = alt
                prev[v] = u
                heapq.heappush(pq, (alt, v))

    return dist, prev


def reconstruct_path(prev: Dict[str, Optional[str]], source: str, target: str) -> Optional[List[str]]:
    """
    Reconstruye el camino desde source hasta target usando el diccionario prev.
    Devuelve None si target no es alcanzable; si source==target devuelve [source].
    """
    if source == target:
        return [source]
    if target not in prev:
        return None
    if prev.get(target) is None:
        return None
    path: List[str] = []
    u = target
    # recorrer hacia atrás hasta source o hasta None
    while u is not None:
        path.append(u)
        if u == source:
            break
        u = prev.get(u)
    if path[-1] != source:
        return None
    path.reverse()
    return path


def shortest_path(graph: Graph, source: str, target: str) -> Tuple[Optional[List[str]], float]:
    """
    Wrapper: devuelve (path, cost) utilizando Dijkstra.
    Si no hay camino, devuelve (None, inf).
    """
    dist, prev = dijkstra(graph, source)
    if dist.get(target, math.inf) == math.inf:
        return None, math.inf
    path = reconstruct_path(prev, source, target)
    return path, dist[target]


# -------------------------
# A* (heuristic search)
# -------------------------
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in kilometers."""
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return 6371.0 * c


def default_heuristic_factory(stops_info: Dict[str, Dict[str, Any]], speed_kmh: float = 18.0) -> Callable[[str, str], float]:
    """
    Crea una heurística que estima el tiempo (en minutos) entre dos stops usando lat/lon.
    stops_info: dict stop_id -> {'lat': float, 'lon': float}
    speed_kmh: velocidad supuesta para la estimación (km/h)
    """
    def heuristic(u: str, v: str) -> float:
        su = stops_info.get(u)
        sv = stops_info.get(v)
        if not su or not sv:
            return 0.0
        lat1 = su.get('lat'); lon1 = su.get('lon')
        lat2 = sv.get('lat'); lon2 = sv.get('lon')
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return 0.0
        km = _haversine_km(float(lat1), float(lon1), float(lat2), float(lon2))
        minutes = (km / float(speed_kmh)) * 60.0
        return minutes
    return heuristic


def astar(graph: Graph, source: str, target: str, heuristic: Optional[Callable[[str, str], float]] = None) -> Tuple[Optional[List[str]], float]:
    """
    A* search. Heuristic must return estimated cost in same units as edge weights (minutes).
    Si heuristic es None usa heurística cero (equivalente a Dijkstra).
    Retorna (path, cost) o (None, inf).
    """
    if source not in graph.nodes or target not in graph.nodes:
        raise KeyError("source or target not in graph")
    if heuristic is None:
        heuristic = lambda u, v: 0.0

    open_set: List[Tuple[float, str]] = []
    heapq.heappush(open_set, (heuristic(source, target), source))
    g_score: Dict[str, float] = {n: math.inf for n in graph.nodes}
    f_score: Dict[str, float] = {n: math.inf for n in graph.nodes}
    came_from: Dict[str, Optional[str]] = {n: None for n in graph.nodes}

    g_score[source] = 0.0
    f_score[source] = heuristic(source, target)
    closed = set()

    while open_set:
        _, current = heapq.heappop(open_set)
        if current == target:
            path = reconstruct_path(came_from, source, target)
            return path, g_score[target]
        if current in closed:
            continue
        closed.add(current)
        for neighbor, weight in graph.neighbors(current):
            tentative_g = g_score[current] + float(weight)
            if tentative_g < g_score.get(neighbor, math.inf):
                came_from[neighbor] = current
                g_score[neighbor] = tentative_g
                f_score[neighbor] = tentative_g + heuristic(neighbor, target)
                heapq.heappush(open_set, (f_score[neighbor], neighbor))

    return None, math.inf


# -------------------------
# UNION-FIND / KRUSKAL (MST)
# -------------------------
class UnionFind:
    def __init__(self, elements):
        self.parent = {e: e for e in elements}
        self.rank = {e: 0 for e in elements}

    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return False
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        else:
            self.parent[ry] = rx
            if self.rank[rx] == self.rank[ry]:
                self.rank[rx] += 1
        return True


def kruskal_mst(graph: Graph) -> Tuple[List[Edge], float]:
    """
    Kruskal para MST. Retorna (mst_edges, total_weight).
    """
    unique_edges: Dict[Tuple[str, str], Edge] = {}
    for e in graph.edges:
        key = tuple(sorted((e.u, e.v)))
        if key not in unique_edges or e.w < unique_edges[key].w:
            unique_edges[key] = Edge(key[0], key[1], e.w)
    edges = list(unique_edges.values())
    edges.sort(key=lambda e: e.w)
    uf = UnionFind(graph.nodes)
    mst: List[Edge] = []
    total_weight = 0.0
    for e in edges:
        if uf.union(e.u, e.v):
            mst.append(e)
            total_weight += float(e.w)
    return mst, total_weight


# -------------------------
# EDMONDS-KARP (MAX FLOW)
# -------------------------
def edmonds_karp(capacity: Dict[str, Dict[str, int]], source: str, sink: str) -> Tuple[int, Dict[str, Dict[str, int]]]:
    """
    capacity: dict de dicts capacity[u][v] = cap (directed)
    Retorna: (max_flow, flow_dict)
    """
    flow = defaultdict(lambda: defaultdict(int))

    def bfs():
        parent = {source: None}
        q = deque([source])
        while q:
            u = q.popleft()
            # forward edges
            for v in capacity.get(u, {}):
                residual = capacity[u][v] - flow[u][v]
                if v not in parent and residual > 0:
                    parent[v] = u
                    if v == sink:
                        path = []
                        cur = v
                        while cur != source:
                            path.append((parent[cur], cur))
                            cur = parent[cur]
                        path.reverse()
                        return parent, path
                    q.append(v)
            # reverse edges with positive flow (allow cancel)
            for v in list(flow[u].keys()):
                if v not in parent and flow[v][u] > 0:
                    parent[v] = u
                    if v == sink:
                        path = []
                        cur = v
                        while cur != source:
                            path.append((parent[cur], cur))
                            cur = parent[cur]
                        path.reverse()
                        return parent, path
                    q.append(v)
        return None, None

    max_flow = 0
    while True:
        parent, path = bfs()
        if not path:
            break
        bottleneck = math.inf
        for u, v in path:
            if v in capacity.get(u, {}):
                residual = capacity[u][v] - flow[u][v]
            else:
                residual = flow[v][u]
            if residual < bottleneck:
                bottleneck = residual
        for u, v in path:
            if v in capacity.get(u, {}):
                flow[u][v] += bottleneck
            else:
                flow[v][u] -= bottleneck
        max_flow += bottleneck
    return max_flow, flow


# -------------------------
# UTIL: construir stops_info desde B+Tree
# -------------------------
def build_stops_info_from_bpt(bpt) -> Dict[str, Dict[str, Any]]:
    """
    Extrae dict stop_id -> {'lat':..., 'lon':...} recorriendo hojas del B+Tree.
    Asume que el valor almacenado para cada key tiene 'lat' y 'lon'.
    Si bpt es None o no tiene método traverse_leaves(), devue_
    """
def test_astar_basic():
    g = Graph(directed=False)
    g.add_edge("A","B",1)
    g.add_edge("B","C",1)
    path, dist = astar(g, "A", "C", heuristic=lambda u,v: 0.0)
    assert path == ["A","B","C"]
    assert dist == 2
