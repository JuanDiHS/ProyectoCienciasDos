# tests/test_graph_algorithms.py
from transport_opt.graph.model import Graph
from transport_opt.graph.algorithms import shortest_path, astar

def test_shortest_path_simple():
    g = Graph(directed=False)
    g.add_edge("X", "Y", 1)
    g.add_edge("Y", "Z", 1)
    path, dist = shortest_path(g, "X", "Z")
    assert path == ["X", "Y", "Z"]
    assert dist == 2

def test_shortest_path_direct_link_preferred():
    g = Graph(directed=False)
    g.add_edge("A", "B", 5)
    g.add_edge("A", "C", 1)
    g.add_edge("C", "B", 1)
    path, dist = shortest_path(g, "A", "B")
    assert path == ["A", "C", "B"]
    assert dist == 2

def test_unreachable_returns_none_inf():
    g = Graph(directed=False)
    g.add_node("U")
    g.add_node("V")
    path, dist = shortest_path(g, "U", "V")
    assert path is None
    assert dist == float("inf")

def test_source_equals_target():
    g = Graph(directed=False)
    g.add_node("S")
    path, dist = shortest_path(g, "S", "S")
    # acepta [S] o (None,0) o dist==0 dependiendo de la implementación
    assert path == ["S"] or (path is None and dist == 0) or dist == 0

def test_astar_basic():
    g = Graph(directed=False)
    g.add_edge("A", "B", 1)
    g.add_edge("B", "C", 1)
    # con heurística cero A* == Dijkstra
    path, dist = astar(g, "A", "C", heuristic=lambda u,v: 0.0)
    assert path == ["A","B","C"]
    assert dist == 2
