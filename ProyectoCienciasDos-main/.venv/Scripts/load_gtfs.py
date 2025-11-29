import pickle, os
os.makedirs('data/processed', exist_ok=True)
with open('data/processed/gtfs_graph.pkl', 'wb') as fh:
    pickle.dump(g, fh)

# Export edges table
edges = []
for u in g.adj:
    for v,w in g.adj[u]:
        edges.append({'u':u,'v':v,'w':w})
import pandas as pd
pd.DataFrame(edges).to_csv('data/processed/gtfs_edges.csv', index=False)
# stops from B+ tree (traverse)
if bpt:
    pd.DataFrame(bpt.traverse_leaves(), columns=['stop_id','metadata']).to_csv('data/processed/gtfs_stops.csv', index=False)
