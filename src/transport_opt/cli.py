from transport_opt.utils import build_sample_bogota_network, build_capacity_from_graph
from transport_opt.db.bplustree import BPlusTree
from transport_opt.sim.simulator import Simulator
from transport_opt.graph.algorithms import shortest_path, kruskal_mst, edmonds_karp
from transport_opt.viz.visualizer import visualize_graph

def main_demo():
    print("Construyendo red simplificada...")
    g = build_sample_bogota_network()

    bpt = BPlusTree(order=4)
    for n in g.nodes:
        info = {'id': n, 'type': ('metro' if n.startswith('M') else 'tm' if n.startswith('T') else 'sitp'), 'capacity_est': 1000 if n.startswith('M') else 600 if n.startswith('T') else 200}
        bpt.insert(n, info)
    print("B+ search M2:", bpt.search('M2'))

    print("Shortest path S1 -> M4:", shortest_path(g, 'S1', 'M4'))
    demand = [('S1','M4',300), ('S2','T5',180), ('M1','M4',800), ('S3','T1',120)]
    sim = Simulator(g, demand).run()
    print("Avg travel time:", sim['avg_travel_time'])

    capacity = build_capacity_from_graph(g, base_capacity_per_minute=30)
    capacity['SRC'] = {}
    capacity['SNK'] = {}
    for origin, dest, rate in demand:
        ppm = max(1, int(rate/60))
        capacity['SRC'][origin] = capacity['SRC'].get(origin, 0) + ppm
    for node in g.nodes:
        capacity.setdefault(node, {})
    capacity['M4']['SNK'] = 10000

    maxflow, flow = edmonds_karp(capacity, 'SRC', 'SNK')
    print("Max flow:", maxflow)
    mst, total_w = kruskal_mst(g)
    print("MST weight:", total_w)

    try:
        visualize_graph(g, edge_load=sim['edge_load'])
    except Exception:
        pass

if __name__ == "__main__":
    main_demo()