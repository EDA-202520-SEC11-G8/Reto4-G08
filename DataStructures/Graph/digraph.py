from DataStructures.Map import map_functions as mf
import DataStructures.Map.map_linear_probing as mlp

from DataStructures.Graph import vertex as vtx


def new_graph(order):
    graph = {
        "vertices": mlp.new_map(order, 0.5),  # load_factor = 0.5 
        "num_edges": 0
    }

    return graph

def insert_vertex(my_graph, key_u, info_u):
    # Crear un nuevo vértice 
    new_v = vtx.new_vertex(key_u, info_u)

    # Insertar o reemplazar el vértice en el mapa
    my_graph["vertices"] = mlp.put(my_graph["vertices"], key_u, new_v)

    return my_graph


def add_edge(my_graph, key_u, key_v, weight=1.0):
    # Buscar el vertice u
    vertex_u = mlp.get(my_graph["vertices"], key_u)
    if vertex_u is None:
        # Error si no existe u
        raise Exception("El vertice u no existe")

    # Buscar el vertice v
    vertex_v = mlp.get(my_graph["vertices"], key_v)
    if vertex_v is None:
        # Error si no existe v
        raise Exception("El vertice v no existe")

    # Revisar si el arco ya existe
    # Si existe, get_edge devuelve un arco; si no, devuelve None
    existe = vtx.get_edge(vertex_u, key_v)

    # Agregar o reemplazar el arco
    vtx.add_adjacent(vertex_u, key_v, weight)

    # Si el arco no existía, aumentar el número de arcos
    if existe is None:
        my_graph["num_edges"] += 1

    return my_graph

def contains_vertex(my_graph, key_u):
    """
    Retorna True si el vertice con llave key_u existe en el grafo.
    """
    return mlp.contains(my_graph["vertices"], key_u)

def order(my_graph):
    """
    Retorna el número de vértices del grafo (orden).
    """
    return mlp.size(my_graph["vertices"])

def size(my_graph):
    """
    Retorna el número de aristas del grafo.
    """
    return my_graph["num_edges"]


def degree(my_graph, key_u):
    """
    Retorna el grado (número de arcos salientes) del vértice key_u.
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        return 0  # No existe

    return vtx.degree(vertex)


def adjacents(my_graph, key_u):
    """
    Retorna una lista con las llaves de los vértices adyacentes a key_u.
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        raise Exception("El vertice no existe")
    
    adj_map = vtx.get_adjacents(vertex)

    
    return mlp.key_set(adj_map)

def vertices(my_graph):
    """
    Retorna una array_list con las llaves de todos los vertices.
    """
    # Obtener los vertices del grafo
    vertices_map = my_graph["vertices"]

    return mlp.key_set(vertices_map)

def edges_vertex(my_graph, key_u):
    """
    Retorna una lista con todos los arcos del vertice key_u.
    """
    vertex = mlp.get(my_graph["vertices"], key_u)
    if vertex is None:
        raise Exception("El vertice no existe")

    adj_map = vertex["adjacents"]
    return mlp.value_set(adj_map)

def get_vertex(my_graph, key_u):
    """
    Retorna el vertice completo con llave key_u.
    """
    return mlp.get(my_graph["vertices"], key_u)

def get_vertex_information(my_graph, key_u):
    """
    Retorna la informacion (value) del vertice con llave key_u.
    """
    vertex = mlp.get(my_graph["vertices"], key_u)

    if vertex is None:
        raise Exception("El vertice no existe")

    return vertex["value"]


def update_vertex_info(my_graph, key_u, new_info_u):
    """
    Actualiza la informacion del vertice key_u.
    """
    vertices_map = my_graph["vertices"]

    # Buscar el vertice
    vertex = mlp.get(vertices_map, key_u)

    if vertex is None:
        return my_graph

    vertex["value"] = new_info_u

    # Guardar el vertice actualizado en el mapa
    vertices_map = mlp.put(vertices_map, key_u, vertex)
    my_graph["vertices"] = vertices_map

    return my_graph

