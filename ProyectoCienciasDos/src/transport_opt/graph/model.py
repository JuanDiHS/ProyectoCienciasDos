from dataclasses import dataclass
from typing import Dict, List, Tuple, Set, DefaultDict
from collections import defaultdict

@dataclass
class Edge:
    u: str
    v: str
    w: float

class Graph:
    def __init__(self, directed: bool = False):
        self.adj: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)
        self.nodes: Set[str] = set()
        self.directed = directed
        self.edges: List[Edge] = []

    def add_node(self, node: str) -> None:
        self.nodes.add(node)
        self.adj.setdefault(node, [])

    def add_edge(self, u: str, v: str, weight: float = 1.0) -> None:
        self.add_node(u)
        self.add_node(v)
        self.adj[u].append((v, weight))
        self.edges.append(Edge(u, v, weight))
        if not self.directed:
            self.adj[v].append((u, weight))
            self.edges.append(Edge(v, u, weight))

    def neighbors(self, u: str):
        return self.adj.get(u, [])