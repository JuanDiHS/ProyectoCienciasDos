from dataclasses import dataclass
from typing import List, Tuple, Dict, Any
import random, math
from transport_opt.graph.algorithms import shortest_path

@dataclass
class Passenger:
    pid: int
    origin: str
    destination: str
    time: float

class Simulator:
    def __init__(self, graph, demand: List[Tuple[str, str, float]]):
        self.graph = graph
        self.demand = demand

    def run(self, sampling_factor: int = 10, window_seconds: int = 3600) -> Dict[str, Any]:
        simulated_passengers = []
        pid = 0
        for origin, destination, rate in self.demand:
            num = max(1, int(rate / sampling_factor))
            for _ in range(num):
                t = random.uniform(0, window_seconds)
                simulated_passengers.append(Passenger(pid, origin, destination, t))
                pid += 1
        simulated_passengers.sort(key=lambda p: p.time)
        edge_load = {}
        results = []
        for p in simulated_passengers:
            path, total_time = shortest_path(self.graph, p.origin, p.destination)
            if path is None:
                results.append((p.pid, None, math.inf)); continue
            for i in range(len(path)-1):
                u, v = path[i], path[i+1]
                edge_load[(u, v)] = edge_load.get((u, v), 0) + 1
            results.append((p.pid, path, total_time))
        times = [r[2] for r in results if r[2] < math.inf]
        avg_time = sum(times)/len(times) if times else math.inf
        return {'passenger_results': results, 'edge_load': edge_load, 'avg_travel_time': avg_time, 'total_passengers': len(simulated_passengers)}