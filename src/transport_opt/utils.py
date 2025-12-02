from transport_opt.graph.model import Graph

def build_sample_bogota_network() -> Graph:
    g = Graph(directed=False)
    nodes = ['M1','M2','M3','M4','T1','T2','T3','T4','T5','S1','S2','S3','S4','S5','S6']
    for n in nodes:
        g.add_node(n)
    g.add_edge('M1','M2',4); g.add_edge('M2','M3',5); g.add_edge('M3','M4',6)
    g.add_edge('T1','T2',3); g.add_edge('T2','T3',4); g.add_edge('T3','T4',4); g.add_edge('T4','T5',5)
    g.add_edge('S1','S2',6); g.add_edge('S2','S3',7); g.add_edge('S3','S4',5); g.add_edge('S4','S5',6); g.add_edge('S5','S6',6)
    g.add_edge('M2','T2',8); g.add_edge('M3','T4',10); g.add_edge('T1','S1',5); g.add_edge('T3','S3',4); g.add_edge('T5','S6',7)
    g.add_edge('S2','T2',6); g.add_edge('S4','M4',12)
    return g

def build_capacity_from_graph(graph, base_capacity_per_minute=30):
    capacity = {}
    for u in graph.nodes:
        capacity[u] = {}
    for u in graph.adj:
        for v, w in graph.adj[u]:
            cap = base_capacity_per_minute
            if u.startswith('M') or v.startswith('M'):
                cap = int(base_capacity_per_minute * 3)
            elif u.startswith('T') or v.startswith('T'):
                cap = int(base_capacity_per_minute * 2)
            capacity[u][v] = cap
    return capacity