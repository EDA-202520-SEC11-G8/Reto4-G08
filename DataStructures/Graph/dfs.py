from DataStructures.Map import map_linear_probing as map
from DataStructures.Queue import queue
from DataStructures.Stack import stack
from DataStructures.Graph.dfo_structure import new_dfo_structure
from DataStructures.Graph import digraph as G



def dfs(my_graph, source):
    """
    Ejecuta DFS desde un vertice origen.
    Retorna la estructura dfo_structure.
    """

    # crear la estructura
    dfo = new_dfo_structure(G.order(my_graph))

    dfo["edge_to"] = map.new_map(G.order(my_graph),0.5)
    
    dfs_vertex(my_graph, source, dfo)

    dfo["source"] = source
    
    return dfo



def dfs_vertex(my_graph, vertex, dfo):
    """
    Funcion recursiva para visitar vertices.
    """

    # marcar como visitado
    map.put(dfo["marked"], vertex, True)

    # agregar en preorden
    queue.enqueue(dfo["pre"], vertex)

    # recorrer los vecinos
    adjs = G.adjacents(my_graph, vertex)
    for w in adjs["elements"]:
        if not map.contains(dfo["marked"], w):
            map.put(dfo["edge_to"], w, vertex)
            dfs_vertex(my_graph, w, dfo)


    # agregar en postorden
    queue.enqueue(dfo["post"], vertex)

    # agregar en reverso postorden
    stack.push(dfo["reversepost"], vertex)
    
    
def has_path_to(vertex, dfo):
    """
    Retorna True si vertex fue visitado.
    """
    return map.contains(dfo["marked"], vertex)


def path_to(vertex, dfo):
    """
    Retorna una pila con el camino desde el origen hasta vertex.
    """

    # si no hay camino None
    if not has_path_to(vertex, dfo):
        return None

    # crear pila vacia
    path = stack.new_stack()

    current = vertex
    source = dfo["source"]
    
    while current != source:
        stack.push(path, current)
        current = map.get(dfo["edge_to"], current)

    # agregar la fuente al inicio
    stack.push(path, source)

    return path
