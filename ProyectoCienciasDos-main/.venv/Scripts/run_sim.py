from transport_opt.utils import build_sample_bogota_network
from transport_opt.sim.simulator import Simulator

def main():
    g = build_sample_bogota_network()
    demand = [('S1','M4',300), ('M1','M4',800)]
    sim = Simulator(g, demand).run()
    print("Total passengers:", sim['total_passengers'])
    print("Avg travel time:", sim['avg_travel_time'])
    # opcional: imprimir carga de aristas
    print("Edge load sample:", dict(list(sim['edge_load'].items())[:10]))

if __name__ == "__main__":
    main()