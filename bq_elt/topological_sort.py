import networkx as nx
from collections import deque
from itertools import product
import yaml


def cp(lsts):
    return list(product(*lsts))


def topological_sort(graph):
    """
    https://github.com/coells/100days/blob/master/day%2081%20-%20topological%20sort.ipynb
    """
    topology = []
    degree = {n: graph.in_degree(n) for n in graph.nodes()}

    # nodes without incoming edges
    queue = deque(n for n, d in degree.items() if not d)

    while queue:
        n = queue.popleft()
        topology.append(n)

        # remove node's edges
        for m in list(graph[n]):
            degree[m] -= 1

            # enqueue nodes with no incoming edges
            if not degree[m]:
                queue.append(m)

    if len(topology) < len(graph):
        raise ValueError('graph contains cycle')

    return topology


if __name__ == '__main__':
    with open('jobs.yaml', 'r') as f:
        jobs = yaml.load(f, Loader=yaml.FullLoader)
    jobs_unsorted = [
        (value['input'], value['output'])
        for job in jobs['jobs']
        for key, value in job.items()
    ]
    edges = [edge for job in jobs_unsorted for edge in cp(job)]
    graph = nx.DiGraph(edges)
    topology = topological_sort(graph)
    print(topology) 
    print(edges)

