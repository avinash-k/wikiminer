#Written by Yongqian Li
#Released under GPL v3
def scale_values(hub_or_auth):
    hub_or_auth_sq_sum = sum(v**2 for k, v in hub_or_auth.iteritems()) #sum(hub_or_auth[n]**2 for n in sub_set)
    print 'hub_or_auth_sq_sum', hub_or_auth_sq_sum
    scaling_factor = 1.0 / (hub_or_auth_sq_sum**0.5)
    for n in hub_or_auth:
        hub_or_auth[n] = scaling_factor * hub_or_auth[n]

def hits(graph, graph_r, num_iter=20):
    auth = dict((x, 1.0) for x in graph)
    hub = dict((x, 1.0) for x in graph)
    scale_values(auth)  
    scale_values(hub)

    for i in range(num_iter):
        print i
        for n in graph:
            hub[n] = sum(auth[x] for x in graph[n] if x != n)
        scale_values(hub)
        for n in graph:
            auth[n] = sum(hub[x] for x in graph_r[n] if x != n)
        scale_values(auth)  
    return auth, hub

import random
def find_similar(root, graph, graph_r, num_iter=25, MAX_LINKS=50):
    root_set = set([root])
    if len(graph[root]) > 200:
        root_set.update(random.sample(graph[root], 200))
    else:
        root_set.update(graph[root])

    sub_set = set(root_set)
    for n in root_set:
        if len(graph[n]) > MAX_LINKS:
            sub_set.update(random.sample(graph[n], MAX_LINKS))
        else:
            sub_set.update(graph[n])
        if len(graph_r[n]) > MAX_LINKS:
            sub_set.update(random.sample(graph_r[n], MAX_LINKS))
        else:
            sub_set.update(graph_r[n])
    
    sub_graph = dict((k, [v for v in graph[k] if v in sub_set]) for k in sub_set)
    sub_graph_r = wikidump.reverse_graph(sub_graph)
    
    return sub_set, hits(sub_graph, sub_graph_r, num_iter)
    

def query(title, graph, graph_r, auth_g, hub_g):
    import models
    similar_set, (auth, hub) = find_similar(models.articles[title], graph, graph_r)    
    p = sorted(similar_set, key=lambda k: auth[k] / auth_g[k] if auth_g[k] > 0.0 else -1., reverse=True)
    for i in range(20):
        print models.articles[p[i]]
