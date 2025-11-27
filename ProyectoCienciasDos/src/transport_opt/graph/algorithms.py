import heapq
import math
from collections import deque, defaultdict
from typing import Dict, Tuple, Any
from .model import Graph, Edge

def dijkstra(graph: Graph, source: str):
    dist = {node: math.inf for node in graph.nodes}
    prev = {node: None for node in graph.nodes}
    dist[source] = 0
    pq = [(0, source)]
    visited = set()
    while pq:
        d, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        for v, w in graph.neighbors(u):
            alt = d + w
            if alt < dist[v]:
                dist[v] = alt
                prev[v] = u
                heapq.heappush(pq, (alt, v))
    return dist, prev

def shortest_path(graph: Graph, source: str, target: str):
    dist, prev = dijkstra(graph, source)
    if dist.get(target, math.inf) == math.inf:
        return None, math.inf
    path = []
    u = target
    while u is not None:
        path.append(u)
        u = prev[u]
    path.reverse()
    return path, dist[target]

# UnionFind and kruskal
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

def kruskal_mst(graph: Graph):
    unique_edges = {}
    for e in graph.edges:
        key = tuple(sorted((e.u, e.v)))
        if key not in unique_edges or e.w < unique_edges[key].w:
            unique_edges[key] = Edge(key[0], key[1], e.w)
    edges = list(unique_edges.values())
    edges.sort(key=lambda e: e.w)
    uf = UnionFind(graph.nodes)
    mst = []
    total_weight = 0
    for e in edges:
        if uf.union(e.u, e.v):
            mst.append(e)
            total_weight += e.w
    return mst, total_weight

# Edmonds-Karp
def edmonds_karp(capacity: Dict[str, Dict[str, int]], source: str, sink: str):
    flow = defaultdict(lambda: defaultdict(int))
    def bfs():
        parent = {source: None}
        q = deque([source])
        while q:
            u = q.popleft()
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