# src/transport_opt/viz/visualizer.py
"""
Visualizador sencillo que usa networkx + matplotlib para dibujar grafo y resaltar una ruta.
Si no están instalados, lanza ImportError al importarlo (CLI lo manejará).
"""
import networkx as nx
import matplotlib.pyplot as plt
import math

def visualize_path(graph, path, title="Ruta"):
    """
    graph: tu objeto Graph con graph.nodes y graph.adj (tipo dict node -> list[(nbr, weight)])
    path: lista de nodos (ordenados)
    """
    # construir networkx graph (undirected) con pesos
    G = nx.Graph()
    for u in graph.adj:
        for v, w in graph.adj[u]:
            if G.has_edge(u, v):
                continue
            G.add_edge(u, v, weight=w)

    # reduce tamaño para dibujar subgrafo si path es pequeño; de lo contrario dibuja toda la red
    plt.figure(figsize=(10, 7))
    pos = None
    try:
        pos = nx.spring_layout(G, seed=42)
    except Exception:
        pos = nx.random_layout(G)

    nx.draw_networkx_nodes(G, pos, node_size=20)
    nx.draw_networkx_edges(G, pos, width=0.5, alpha=0.5)
    # labels pequeños pueden saturar, así que solo si la red es pequeña
    if len(G.nodes) < 200:
        nx.draw_networkx_labels(G, pos, font_size=6)

    if path:
        # resaltar nodos y aristas del path
        path_edges = [(path[i], path[i+1]) for i in range(len(path)-1)]
        nx.draw_networkx_nodes(G, pos, nodelist=path, node_size=80, node_color='red')
        nx.draw_networkx_edges(G, pos, edgelist=path_edges, width=2.5, edge_color='red')

    plt.title(title)
    plt.axis('off')
    plt.show()
