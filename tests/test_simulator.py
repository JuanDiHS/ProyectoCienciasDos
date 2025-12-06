# tests/test_simulator.py
from transport_opt.graph.model import Graph
from transport_opt.sim.simulator import Simulator

def test_sim_basic():
    g = Graph(directed=False)
    g.add_edge("A","B",1.0)
    g.add_edge("B","C",1.0)

    # small capacity: 1 pasajero por minuto
    cap = {"A":{"B":1},"B":{"A":1,"C":1},"C":{"B":1}}
    sim = Simulator(g, capacity=cap, vehicle_capacity=10)
    demand = [("A","C", 60)]  # 60 pasajeros/hora → ~1 por minuto
    res = sim.run(demand, window_seconds=60*10)  # 10 minutos
    assert res["total_passengers"] >= 1
    assert res["avg_travel_time"] != float("inf")
