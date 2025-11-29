from transport_opt.graph.model import Graph
from transport_opt.graph.algorithms import shortest_path

def test_shortest_path_simple():
    g = Graph(directed=False)
    g.add_edge("X","Y",1); g.add_edge("Y","Z",1)
    path, dist = shortest_path(g, "X", "Z")
    assert path == ["X","Y","Z"]
    assert dist == 2