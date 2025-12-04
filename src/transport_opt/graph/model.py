# src/transport_opt/graph/model.py
from collections import defaultdict, namedtuple
from typing import Iterable, List, Tuple, Dict, Any

Edge = namedtuple("Edge", ["u", "v", "w"])

class Graph:
    """
    Representación simple de grafo usada en el proyecto.
    - adj: map node -> list[(neighbor, weight)]
    - nodes: set de nodos
    - edges: lista de Edge (puede contener duplicados en grafos no dirigidos)
    - directed: bool
    - edge_meta: opcionalmente map (u,v)->list[dict] para metadata (trip_id, route_id, headway)
    """
    def __init__(self, directed: bool = False):
        self.adj: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        self.nodes = set()
        self.edges: List[Edge] = []
        self.directed = directed
        self.edge_meta: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    def add_node(self, node: str):
        self.nodes.add(node)
        # ensure adjacency entry exists
        _ = self.adj[node]

    def add_edge(self, u: str, v: str, weight: float = 1.0, meta: Dict[str, Any] = None):
        """
        Añade arista u->v con peso. Si el grafo no es dirigido,
        añade también v->u (esto hará que self.edges contenga
        dos Edge opuestos; los algoritmos que requieran MST deben dedupear).
        """
        self.nodes.add(u)
        self.nodes.add(v)
        self.adj[u].append((v, float(weight)))
        self.edges.append(Edge(u, v, float(weight)))
        if meta:
            self.edge_meta[(u, v)].append(meta)

        if not self.directed:
            self.adj[v].append((u, float(weight)))
            self.edges.append(Edge(v, u, float(weight)))
            if meta:
                self.edge_meta[(v, u)].append(meta)

    def neighbors(self, u: str):
        return self.adj.get(u, [])

    def has_node(self, u: str) -> bool:
        return u in self.nodes

    def to_networkx(self):
        """
        Utility to create a networkx Graph (undirected) or DiGraph from this structure.
        Requiere networkx instalado.
        """
        import networkx as nx
        G = nx.DiGraph() if self.directed else nx.Graph()
        for n in self.nodes:
            G.add_node(n)
        for u in self.adj:
            for v, w in self.adj[u]:
                # avoid duplicate if undirected and already added
                if not self.directed and G.has_edge(u, v):
                    continue
                G.add_edge(u, v, weight=w)
        return G

    def __len__(self):
        return len(self.nodes)
