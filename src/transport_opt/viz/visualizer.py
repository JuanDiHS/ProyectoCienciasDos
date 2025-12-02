try:
    import networkx as nx
    import matplotlib.pyplot as plt
    VISUAL_AVAILABLE = True
except Exception:
    VISUAL_AVAILABLE = False
import math

def visualize_graph(graph, edge_load=None, title="Red"):
    if not VISUAL_AVAILABLE:
        print("visualization libs not installed")
        return
    G = nx.Graph()
    for n in graph.nodes:
        G.add_node(n)
    for u in graph.adj:
        for v, w in graph.adj[u]:
            if G.has_edge(u, v): continue
            G.add_edge(u, v, weight=w)
    pos = nx.spring_layout(G, seed=42)
    plt.figure(figsize=(9,6))
    nx.draw_networkx_nodes(G, pos, node_size=400)
    nx.draw_networkx_labels(G, pos, font_size=8)
    widths = []
    for u, v in G.edges():
        load = 0
        if edge_load:
            load = edge_load.get((u, v), edge_load.get((v, u), 0))
        widths.append(1 + math.log1p(load) if load>0 else 1)
    nx.draw_networkx_edges(G, pos, width=widths)
    plt.title(title)
    plt.axis('off')
    plt.show()