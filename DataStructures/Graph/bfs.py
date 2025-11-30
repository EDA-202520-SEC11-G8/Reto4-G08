from DataStructures.Map import map_linear_probing as map
from DataStructures.Queue import queue
from DataStructures.Stack import stack
from DataStructures.Graph import digraph as G


def new_bfo_structure(num_vertices):
    """
    Crea la estructura BFO (Breadth First Order).
    Similar a DFO pero usa BFS.
    """
    bfo = {
        "marked": map.new_map(num_vertices, 0.5),
        "edge_to": map.new_map(num_vertices, 0.5),
        "source": None
    }
    return bfo


def bfs(my_graph, source):
    """
    Ejecuta BFS desde un vertice origen.
    Retorna la estructura bfo_structure.
    """
    # Crear la estructura
    bfo = new_bfo_structure(G.order(my_graph))
    
    # Marcar el origen como visitado
    map.put(bfo["marked"], source, True)
    
    # Crear cola para BFS
    q = queue.new_queue()
    queue.enqueue(q, source)
    
    # Mientras haya vertices en la cola
    while not queue.is_empty(q):
        vertex = queue.dequeue(q)
        
        # Recorrer los vecinos
        adjs = G.adjacents(my_graph, vertex)
        for w in adjs["elements"]:
            if not map.contains(bfo["marked"], w):
                # Marcar como visitado
                map.put(bfo["marked"], w, True)
                # Guardar de donde vino
                map.put(bfo["edge_to"], w, vertex)
                # Agregar a la cola
                queue.enqueue(q, w)
    
    bfo["source"] = source
    
    return bfo


def has_path_to(vertex, bfo):
    """
    Retorna True si vertex fue visitado.
    """
    return map.contains(bfo["marked"], vertex)


def path_to(vertex, bfo):
    """
    Retorna una pila con el camino desde el origen hasta vertex.
    """
    # Si no hay camino, retornar None
    if not has_path_to(vertex, bfo):
        return None

    # Crear pila vacia
    path = stack.new_stack()

    current = vertex
    source = bfo["source"]
    
    while current != source:
        stack.push(path, current)
        current = map.get(bfo["edge_to"], current)

    # Agregar la fuente al inicio
    stack.push(path, source)

    return path