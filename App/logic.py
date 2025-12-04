import time
import csv
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime
import sys
sys.setrecursionlimit(20000)

from DataStructures.List import array_list as lt
from DataStructures.Map import map_linear_probing as mp
from DataStructures.Graph import digraph as gp
from DataStructures.Graph import dfs as DFS
from DataStructures.Graph import vertex as vtx
from DataStructures.Stack import stack as st
from DataStructures.Graph import dijsktra as dk

# =============================================================================
# ----------------------------- ESTRUCTURA PRINCIPAL --------------------------
# =============================================================================
def new_logic():
    """
    Crea el catálogo principal.

    El catálogo contiene:
      - Una lista con todos los eventos cargados desde el archivo.
      - Una lista de nodos migratorios construidos a partir de los eventos.
      - Un mapa que relaciona cada event-id con el nodo al que pertenece.
      - Dos grafos :
            grafo_1: pesos por distancia entre nodos consecutivos.
            grafo_2: pesos por diferencia promedio de agua entre nodos.
    """
    catalog = {
        "eventos": lt.new_list(),
        "nodos": lt.new_list(),     
        "map_evento_nodo": mp.new_map(50000, 0.5),
        "grafo_1": gp.new_graph(10000),
        "grafo_2": gp.new_graph(10000) 
    }
    return catalog

# =============================================================================
# ------------------------------ CARGA DE DATOS -------------------------------
# =============================================================================
def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    ruta = "Data/" + filename
    with open(ruta, encoding="utf-8-sig") as f:
        lector = csv.DictReader(f)
        
        for evento in lector:
            lt.add_last(catalog["eventos"], evento)
            
    catalog["eventos"] = lt.merge_sort(catalog["eventos"], cmp_timestamp)
    # crear nodos migratorios
    nodos, map_evento_nodo = crear_nodos(catalog["eventos"])
    
    catalog["nodos"] = nodos
    catalog["map_evento_nodo"] = map_evento_nodo
    
    # crear grafos
    construir_grafos(catalog)

    return {
        "num_eventos": lt.size(catalog["eventos"]),
        "num_nodos": lt.size(catalog["nodos"])
    }
    
# =============================================================================
# ---------------------------- FUNCIONES AUXILIARES ---------------------------
# =============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """    
    Calcula la distancia entre dos puntos usando la fórmula Haversine.
    """
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c


def crear_nodo(evento):
    """
    Crea un nuevo nodo migratorio basado en un evento individual.

    Un nodo agrupa varios eventos cercanos en espacio (<3 km)
    y tiempo (<3 horas). 
    Además, calcula:
      - Lista de grullas presentes en el nodo.
      - Lista de eventos asociados.
      - Promedio de distancia al agua (comments).
    """
    nodo = {
        "id": evento["event-id"],
        "lat": float(evento["location-lat"]),
        "lon": float(evento["location-long"]),
        "timestamp": datetime.fromisoformat(evento["timestamp"]).replace(microsecond=0),
        "grullas": lt.new_list(),
        "eventos": lt.new_list(),
        "prom_agua": 0.0,
        "total_agua": 0.0,
        "count": 0
    }

    lt.add_last(nodo["grullas"], evento["tag-local-identifier"])
    lt.add_last(nodo["eventos"], evento)

    d = float(evento["comments"]) / 1000
    nodo["total_agua"] = d
    nodo["count"] = 1
    nodo["prom_agua"] = d

    return nodo


def agregar_evento_a_nodo(nodo, evento):
    """
    Agrega un evento adicional a un nodo migratorio existente.

    El nodo se actualiza así:
      - Se añade la grulla si no estaba registrada.
      - Se agrega el evento a la lista interna del nodo.
      - Se recalcula el promedio de distancia al agua.
    """
    
    if lt.is_present(nodo["grullas"], evento["tag-local-identifier"]) == -1:
        lt.add_last(nodo["grullas"], evento["tag-local-identifier"])

    lt.add_last(nodo["eventos"], evento)

    d = float(evento["comments"]) / 1000
    nodo["total_agua"] += d
    nodo["count"] += 1
    nodo["prom_agua"] = nodo["total_agua"] / nodo["count"]
    
    
def evento_encaja(nodo, evento):
    """
    Verifica si un evento pertenece a un nodo migratorio.

    Un evento pertenece a un nodo si:
      - Está a menos de 3 km del nodo.
      - La diferencia temporal es menor o igual a 3 horas.

    Returns:
        bool: True si encaja, False en caso contrario.
    """
    dist = haversine(
        nodo["lat"], nodo["lon"],
        float(evento["location-lat"]), float(evento["location-long"])
    )
    if dist > 3:
        return False

    t1 = nodo["timestamp"]
    t2 = datetime.fromisoformat(evento["timestamp"]).replace(microsecond=0)
    horas = abs((t2 - t1).total_seconds()) / 3600

    return horas <= 3

def crear_nodos(lista_eventos):
    """
    Construye la lista completa de nodos migratorios.

    Para cada evento:
      - Se revisa si encaja en un nodo ya existente.
      - Si encaja, se agrega al nodo correspondiente.
      - Si no encaja, se crea un nodo nuevo.
      - Se registra la relación event-id → nodo-id en un mapa.

    Args:
        lista_eventos (array_list): lista de eventos cargados del CSV.

    Returns:
        tuple: 
            array_list con los nodos construidos,
            mapa (event-id → nodo-id).
    """
    nodos = lt.new_list()
    mapa = mp.new_map(50000, 0.5)

    n = lt.size(lista_eventos)

    for i in range(n):
        evento = lt.get_element(lista_eventos, i)
        asignado = False

        for j in range(lt.size(nodos)):
            nodo = lt.get_element(nodos, j)

            if evento_encaja(nodo, evento):
                agregar_evento_a_nodo(nodo, evento)
                mp.put(mapa, evento["event-id"], nodo["id"])
                asignado = True
                break

        if not asignado:
            nuevo = crear_nodo(evento)
            lt.add_last(nodos, nuevo)
            mp.put(mapa, evento["event-id"], nuevo["id"])

    return nodos, mapa


def construir_grafos(catalog):
    """
    Construye los dos grafos del reto usando los nodos migratorios.

    grafo_1:
        Peso del arco : distancia Haversine entre nodos.

    grafo_2:
        Peso del arco : diferencia absoluta en el promedio
        de distancia al agua entre nodos.
    Args:
        catalog (dict): catálogo con nodos y grafos creados.
    """
    nodos = catalog["nodos"]
    eventos = catalog["eventos"]
    mapa_evento_nodo = catalog["map_evento_nodo"]

    g1 = catalog["grafo_1"]
    g2 = catalog["grafo_2"]

    # Insertar todos los nodos como vértices
    for i in range(lt.size(nodos)):
        nodo = lt.get_element(nodos, i)
        gp.insert_vertex(g1, nodo["id"], nodo)
        gp.insert_vertex(g2, nodo["id"], nodo)

    # Agrupar eventos por grulla
    grupos = mp.new_map(100, 0.5)

    for i in range(lt.size(eventos)):
        ev = lt.get_element(eventos, i)
        crane = ev["tag-local-identifier"]

        lista = mp.get(grupos, crane)
        if lista is None:
            nueva = lt.new_list()
            lt.add_last(nueva, ev)
            mp.put(grupos, crane, nueva)
        else:
            lt.add_last(lista, ev)

    # Estructuras para almacenar distancias de viajes A->B
    distancias = mp.new_map(1000, 0.5)
    aguas = mp.new_map(1000, 0.5)

    # Procesar cada grulla
    claves = mp.key_set(grupos)

    for i in range(lt.size(claves)):

        crane_id = lt.get_element(claves, i)
        lista = mp.get(grupos, crane_id)

        # ordenar eventos por timestamp
        lista_ordenada = lt.merge_sort(lista, cmp_timestamp)

        nodo_prev = None
        evento_prev = None
        
        for j in range(lt.size(lista_ordenada)):
            evento = lt.get_element(lista_ordenada, j)
            nodo_actual = mp.get(mapa_evento_nodo, evento["event-id"])

            # primer evento
            if nodo_prev is None:
                nodo_prev = nodo_actual
                evento_prev = evento
                continue

            if nodo_prev != nodo_actual:

                clave = f"{nodo_prev}->{nodo_actual}"
                Nact = buscar_nodo_por_id(nodos, nodo_actual)
                
                # distancia haversine entre los eventos
                lat,lon = float(evento["location-lat"]), float(evento["location-long"])
                lat_prev,lon_prev = float(evento_prev["location-lat"]), float(evento_prev["location-long"])
                
                d = haversine(lat, lon,
                              lat_prev, lon_prev)

                # registrar distancia migratoria
                lista_d = mp.get(distancias, clave)
                if lista_d is None:
                    nueva = lt.new_list()
                    lt.add_last(nueva, d)
                    mp.put(distancias, clave, nueva)
                else:
                    lt.add_last(lista_d, d)

                # registrar distancia promedio al agua
                lista_a = mp.get(aguas, clave)
                if lista_a is None:
                    nueva = lt.new_list()
                    lt.add_last(nueva, Nact["prom_agua"])
                    mp.put(aguas, clave, nueva)
                else:
                    lt.add_last(lista_a, Nact["prom_agua"])

            nodo_prev = nodo_actual
            evento_prev = evento

    # Crear arcos con promedio
    claves_arcos = mp.key_set(distancias)

    for i in range(lt.size(claves_arcos)):

        clave = lt.get_element(claves_arcos, i)
        origen, destino = clave.split("->")

        lista_d = mp.get(distancias, clave)
        lista_a = mp.get(aguas, clave)

        # promedio distancia
        total_d = 0
        for k in range(lt.size(lista_d)):
            total_d += lt.get_element(lista_d, k)
        w_dist = total_d / lt.size(lista_d)

        # promedio agua
        total_a = 0
        for k in range(lt.size(lista_a)):
            total_a += lt.get_element(lista_a, k)
        w_agua = total_a / lt.size(lista_a)

        gp.add_edge(g1, origen, destino, w_dist)
        gp.add_edge(g2, origen, destino, w_agua)




def cmp_timestamp(e1, e2):
    """
    Compara dos eventos por su timestamp.
    Retorna True si e1 ocurre antes que e2.
    """
    t1 = datetime.fromisoformat(e1["timestamp"])
    t2 = datetime.fromisoformat(e2["timestamp"])
    return t1 <= t2

def buscar_nodo_por_id(nodos, nodo_id):
    """
    Retorna el nodo cuyo id coincide con nodo_id.
    Busca linealmente en array_list nodos.
    """
    for i in range(lt.size(nodos)):
        nodo = lt.get_element(nodos, i)
        if nodo["id"] == nodo_id:
            return nodo
    return None

def topological_sort(graph):
    """
    Orden topológico usando Kahn (versión simplificada).
    Si hay ciclo, retorna None.
    """
    vertices = gp.vertices(graph)
    N = lt.size(vertices)

    indeg = mp.new_map(N, 0.5)

    # inicializar grados en 0
    for i in range(N):
        v = lt.get_element(vertices, i)
        mp.put(indeg, v, 0)

    # calcular indegree
    for i in range(N):
        u = lt.get_element(vertices, i)
        adjs = gp.adjacents(graph, u)
        
        for j in range(lt.size(adjs)):
            w = lt.get_element(adjs, j)
            mp.put(indeg, w, mp.get(indeg, w) + 1)
        

    # cola de nodos con indegree 0
    cero = lt.new_list()
    for i in range(N):
        v = lt.get_element(vertices, i)
        if mp.get(indeg, v) == 0:
            lt.add_last(cero, v)

    orden = lt.new_list()
    idx = 0

    # proceso de Kahn
    while idx < lt.size(cero):
        u = lt.get_element(cero, idx)
        lt.add_last(orden, u)

        adjs = gp.adjacents(graph, u)
        for j in range(lt.size(adjs)):
            v = lt.get_element(adjs, j)
            mp.put(indeg, v, mp.get(indeg, v) - 1)
            if mp.get(indeg, v) == 0:
                lt.add_last(cero, v)
        idx += 1

    # si no tiene todos los vértices, hay ciclo
    if lt.size(orden) != N:
        return None

    return orden

def longest_path_in_dag(graph):
    """
    Ruta más larga en términos de número de nodos.
    """
    topo = topological_sort(graph)
    if topo is None:
        return None

    n = lt.size(topo)
    
    dist = mp.new_map(n, 0.5)
    prev = mp.new_map(n, 0.5)
    
    # inicializar dist = 1
    for i in range(n):
        v = lt.get_element(topo, i)
        mp.put(dist, v, 1)
        mp.put(prev, v, None)
    
    # relajar usando el orden topológico
    for i in range(n):
        u = lt.get_element(topo, i)
        du = mp.get(dist, u)
        
        adjs = gp.adjacents(graph, u)
        
        for j in range(lt.size(adjs)):
            v = lt.get_element(adjs, j)
            if du + 1 > mp.get(dist, v):
                mp.put(dist, v, du + 1)
                mp.put(prev, v, u)
                   
    maxv = None
    maxd = 0
    verts = gp.vertices(graph)
    for i in range(lt.size(verts)):
        v = lt.get_element(verts, i)
        d = mp.get(dist, v)
        if d > maxd:
            maxd = d
            maxv = v

    if maxv is None:
        return None
    
    rev = lt.new_list()
    cur = maxv
    while cur is not None:
        lt.add_last(rev, cur)
        cur = mp.get(prev, cur)

    # invertir
    path = lt.new_list()
    for i in range(lt.size(rev)-1, -1, -1):
        lt.add_last(path, lt.get_element(rev, i))

    return path

def cmp_subred(a, b):
    sa = a["total_nodos"]
    sb = b["total_nodos"]

    # Orden descendente por cantidad de nodos
    if sa > sb:
        return True
    elif sa < sb:
        return False
    
    # Si empatan, ordenar por id_subred ascendente
    return a["id_subred"] < b["id_subred"]

def obtener_nodos_dfs(grafo, origen):
    """
    Usa DFS para encontrar todos los nodos conectados desde un origen.
    Retorna una lista con los IDs de los nodos.
    """
    # DFS dado por la estructura
    resultado_dfs = DFS.dfs(grafo, origen)
    mapa_visitados = resultado_dfs["marked"]
    
    # Extraemos las llaves (IDs de nodos) del mapa
    lista_ids = mp.key_set(mapa_visitados)
    return lista_ids
def calcular_estadisticas_subred(id_subred, lista_ids, catalogo_nodos):
    """
    Procesa una lista de IDs de nodos para calcular:
    - Rangos de Lat/Lon
    - Grullas únicas
    - Estructura de datos para el reporte
    """
    total_nodos_ids = lt.size(lista_ids)
    
    min_lat, max_lat = 1000.0, -1000.0
    min_lon, max_lon = 1000.0, -1000.0
    
    mapa_grullas = mp.new_map(5000, 0.5)
    detalles_nodos = lt.new_list() 
    
    # Recorremos todos los nodos de la subred
    for i in range(total_nodos_ids):
        nid = lt.get_element(lista_ids, i)
        nodo = buscar_nodo_por_id(catalogo_nodos, nid)
        
        if nodo is not None:
            # Actualizar coordenadas extremas
            lat, lon = nodo["lat"], nodo["lon"]
            if lat < min_lat: min_lat = lat
            if lat > max_lat: max_lat = lat
            if lon < min_lon: min_lon = lon
            if lon > max_lon: max_lon = lon
            
            # Recolectar grullas
            tags = nodo["grullas"]
            for k in range(lt.size(tags)):
                t = lt.get_element(tags, k)
                mp.put(mapa_grullas, t, True)
            
            # Guardar info para visualización
            info_simple = {
                "id": nid,
                "lat": lat,
                "lon": lon
            }
            lt.add_last(detalles_nodos, info_simple)

    # Lista de grullas únicas
    lista_grullas = mp.key_set(mapa_grullas)

    real_size = lt.size(detalles_nodos)

    primeros_3_nodos = lt.new_list()
    ultimos_3_nodos = lt.new_list()
    
    # Primeros 3
    limit_n = min(3, real_size)
    for k in range(limit_n):
        lt.add_last(primeros_3_nodos, lt.get_element(detalles_nodos, k))
    
    # Últimos 3
    start_n = max(3, real_size - 3)
    if start_n < limit_n: start_n = limit_n
        
    for k in range(start_n, real_size):
        lt.add_last(ultimos_3_nodos, lt.get_element(detalles_nodos, k))

    # Grullas
    total_grullas = lt.size(lista_grullas)
    primeras_3_grullas = lt.new_list()
    ultimas_3_grullas = lt.new_list()
    
    limit_g = min(3, total_grullas)
    for k in range(limit_g):
        lt.add_last(primeras_3_grullas, lt.get_element(lista_grullas, k))
        
    start_g = max(3, total_grullas - 3)
    if start_g < limit_g: start_g = limit_g
    for k in range(start_g, total_grullas):
        lt.add_last(ultimas_3_grullas, lt.get_element(lista_grullas, k))

    return {
        "id_subred": id_subred,
        "total_nodos": total_nodos_ids, # Mantenemos el total teórico para el reporte
        "total_individuos": total_grullas,
        "rango_lat": (min_lat, max_lat),
        "rango_lon": (min_lon, max_lon),
        "primeros_nodos": primeros_3_nodos,
        "ultimos_nodos": ultimos_3_nodos,
        "primeras_grullas": primeras_3_grullas,
        "ultimas_grullas": ultimas_3_grullas
    }

def buscar_nodo_mas_cercano(nodos, lat_user, lon_user):
    """
    Retorna el ID del nodo cuya ubicación (lat/lon)
    es la más cercana al punto GPS dado por el usuario.

    Args:
        nodos (array_list): nodos migratorios.
        lat_user, lon_user (float): coordenadas del usuario.

    Returns:
        str: id del nodo más cercano.
    """
    mejor_id = None
    mejor_dist = 999999999

    N = lt.size(nodos)

    for i in range(N):
        nodo = lt.get_element(nodos, i)
        lat = nodo["lat"]
        lon = nodo["lon"]

        d = haversine(lat, lon, lat_user, lon_user)
        if d < mejor_dist:
            mejor_dist = d
            mejor_id = nodo["id"]

    return mejor_id

def bfs_camino(grafo, origen, destino):
    """
    BFS estándar para obtener el camino desde origen hasta destino.

    Args:
        grafo: grafo dirigido.
        origen: id del nodo inicial.
        destino: id del nodo objetivo.

    Returns:
        lista (array_list) con los ids del camino en orden.
        Si no existe camino, retorna None.
    """
    # cola FIFO
    cola = lt.new_list()
    # mapa de visitados
    visitados = mp.new_map(20000, 0.5)
    # mapa para guardar padre de cada nodo
    padre = mp.new_map(20000, 0.5)

    lt.add_last(cola, origen)
    mp.put(visitados, origen, True)
    mp.put(padre, origen, None)

    pos = 0
    while pos < lt.size(cola):
        actual = lt.get_element(cola, pos)

        if actual == destino:
            break

        adjs = gp.adjacents(grafo, actual)
        size_adj = lt.size(adjs)

        for i in range(size_adj):
            nxt = lt.get_element(adjs, i)

            if not mp.contains(visitados, nxt):
                mp.put(visitados, nxt, True)
                mp.put(padre, nxt, actual)
                lt.add_last(cola, nxt)

        pos += 1

    # Si destino no fue visitado, no existe camino
    if not mp.contains(visitados, destino):
        return None

    # Reconstruir camino desde destino → origen
    rev = lt.new_list()
    cur = destino

    while cur is not None:
        lt.add_last(rev, cur)
        cur = mp.get(padre, cur)

    # Invertir la lista
    path = lt.new_list()
    for i in range(lt.size(rev)-1, -1, -1):
        lt.add_last(path, lt.get_element(rev, i))

    return path

def ultimo_nodo_en_radio(camino, nodos, nodo_origen, radio_km):
    """
    Dado un camino (lista de nodos) halla el último nodo
    cuya distancia respecto del nodo de origen esté dentro del radio.

    Args:
        camino: lista con los ids del camino.
        nodos: lista completa de nodos migratorios.
        nodo_origen: id del nodo de origen.
        radio_km: distancia de interés.

    Returns:
        id del último nodo en el área.
        Si ninguno califica → devuelve el origen mismo.
    """
    origen_real = buscar_nodo_por_id(nodos, nodo_origen)
    lat0, lon0 = origen_real["lat"], origen_real["lon"]

    ultimo = nodo_origen

    for i in range(lt.size(camino)):
        nid = lt.get_element(camino, i)
        nodo = buscar_nodo_por_id(nodos, nid)

        if nodo is None:
            continue

        d = haversine(lat0, lon0, nodo["lat"], nodo["lon"])
        if d <= radio_km:
            ultimo = nid

    return ultimo


def nodos_en_radio(nodos, nodo_origen, radio_km):
    """
    Devuelve lista de nodos cuyo centro está dentro del radio indicado.
     en array_list del curso.
    """

    lista = lt.new_list()
    origen = buscar_nodo_por_id(nodos, nodo_origen)

    lat0 = origen["lat"]
    lon0 = origen["lon"]

    for i in range(lt.size(nodos)):
        nodo = lt.get_element(nodos, i)
        d = haversine(lat0, lon0, nodo["lat"], nodo["lon"])

        if d <= radio_km:
            lt.add_last(lista, nodo["id"])

    return lista

# Funciones de consulta sobre el catálogo
def req_1(catalog, lat_o, lon_o, lat_d, lon_d, crane_id):
    """
    Detecta el camino de un individuo (grulla) entre dos puntos migratorios.
    Usa DFS y los pesos de arcos del grafo para calcular distancias.
    """
    nodos = catalog["nodos"]
    grafo = catalog["grafo_1"]
    
    # 1. Encontrar nodos más cercanos usando función auxiliar
    origen = buscar_nodo_mas_cercano(nodos, lat_o, lon_o)
    destino = buscar_nodo_mas_cercano(nodos, lat_d, lon_d)
    
    # 2. Buscar nodo de origen por ID y verificar grulla (inline)
    nodo_origen_obj = None
    for i in range(lt.size(nodos)):
        nodo = lt.get_element(nodos, i)
        if nodo["id"] == origen:
            nodo_origen_obj = nodo
            break
    
    if nodo_origen_obj is None:
        return {
            "mensaje": f"El nodo de origen {origen} no existe.",
            "origen": origen,
            "destino": destino,
            "grulla": crane_id
        }
    
    # Verificar si la grulla está en el nodo de origen
    grullas_origen = nodo_origen_obj["grullas"]
    presente = False
    for i in range(lt.size(grullas_origen)):
        if lt.get_element(grullas_origen, i) == crane_id:
            presente = True
            break
    
    if not presente:
        return {
            "mensaje": f"La grulla {crane_id} no se encuentra en el nodo de origen {origen}.",
            "origen": origen,
            "destino": destino,
            "grulla": crane_id
        }
    # 3. DFS para encontrar camino
    dfs_result = DFS.dfs(grafo, origen)
    
    if not DFS.has_path_to(destino, dfs_result):
        return {
            "mensaje": f"No existe un camino viable desde {origen} hasta {destino}.",
            "origen": origen,
            "destino": destino,
            "grulla": crane_id
        }
    # 4. Reconstrucción de camino desde pila DFS
    camino_stack = DFS.path_to(destino, dfs_result)
    camino = lt.new_list()
    while not st.is_empty(camino_stack):
        lt.add_last(camino, st.pop(camino_stack))
    
    total_puntos = lt.size(camino)
    
    # 5. Calcular distancia total y construir detalles en un solo recorrido
    total_distancia = 0.0
    detalles_completos = lt.new_list()
    for i in range(total_puntos):
        node_id = lt.get_element(camino, i)
        
        # Buscar nodo actual usando buscar_nodo_por_id
        nodo_actual = buscar_nodo_por_id(nodos, node_id)
        
        if nodo_actual is None:
            # Detalle vacío si no se encuentra el nodo
            lt.add_last(detalles_completos, {
                "id": "Unknown",
                "lat": "Unknown",
                "lon": "Unknown",
                "num_grullas": "Unknown",
                "first3": lt.new_list(),
                "last3": lt.new_list(),
                "dist_next": "Unknown"
            })
            continue
        
        # Obtener lista de grullas
        tags = nodo_actual["grullas"]
        gsize = lt.size(tags)
        
        # Extraer primeras 3 grullas
        first3 = lt.new_list()
        limite_first = min(3, gsize)
        for j in range(limite_first):
            lt.add_last(first3, lt.get_element(tags, j))
        
        # Extraer últimas 3 grullas
        last3 = lt.new_list()
        inicio_last = max(0, gsize - 3)
        for j in range(inicio_last, gsize):
            lt.add_last(last3, lt.get_element(tags, j))
        
        # Calcular distancia al siguiente nodo
        dist_sig = "END"
        if i < total_puntos - 1:
            next_id = lt.get_element(camino, i + 1)
            vertex = gp.get_vertex(grafo, node_id)
            if vertex is not None:
                edge = vtx.get_edge(vertex, next_id)
                if edge is not None:
                    peso_arco = edge["weight"]
                    dist_sig = peso_arco
                    total_distancia += peso_arco
        
        # Agregar detalle completo
        lt.add_last(detalles_completos, {
            "id": nodo_actual["id"],
            "lat": nodo_actual["lat"],
            "lon": nodo_actual["lon"],
            "num_grullas": gsize,
            "first3": first3,
            "last3": last3,
            "dist_next": dist_sig
        })
    
    # 6. Crear lista combinada: primeros 5 + separador + últimos 5
    detalles_para_mostrar = lt.new_list()
    
    # Si hay 10 o menos nodos, mostrar todos
    if total_puntos <= 10:
        for i in range(total_puntos):
            lt.add_last(detalles_para_mostrar, lt.get_element(detalles_completos, i))
    else:
        # Más de 10 nodos: primeros 5 + separador + últimos 5
        # Agregar primeros 5
        for i in range(5):
            lt.add_last(detalles_para_mostrar, lt.get_element(detalles_completos, i))
        
        # Agregar separador
        lt.add_last(detalles_para_mostrar, {
            "id": "...",
            "lat": "...",
            "lon": "...",
            "num_grullas": "...",
            "first3": "...",
            "last3": "...",
            "dist_next": "..."
        })
        
        # Agregar últimos 5
        for i in range(total_puntos - 5, total_puntos):
            lt.add_last(detalles_para_mostrar, lt.get_element(detalles_completos, i))
    
    # 7. Respuesta final
    return {
        "mensaje": f"Camino encontrado para la grulla {crane_id}.",
        "grulla": crane_id,
        "origen": origen,
        "destino": destino,
        "distancia_total": total_distancia,
        "total_puntos": total_puntos,
        "detalles_mostrar": detalles_para_mostrar
    }


def req_2(catalog, lat_o, lon_o, lat_d, lon_d, radio_km):
    """
    Detectar movimientos entre dos puntos migratorios alrededor de un área.

    Parámetros:
        lat_o, lon_o -> coordenadas del origen
        lat_d, lon_d -> coordenadas del destino
        radio_km -> radio de interés

    Retorna:
        Diccionario con toda la información solicitada.
    """

    nodos = catalog["nodos"]
    grafo = catalog["grafo_1"]

    # 1. Encontrar nodos migratorios más cercanos (Haversine)
    origen = buscar_nodo_mas_cercano(nodos, lat_o, lon_o)
    destino = buscar_nodo_mas_cercano(nodos, lat_d, lon_d)

    # 2. BFS para encontrar ruta
    camino = bfs_camino(grafo, origen, destino)

    if camino is None:
        return {
            "mensaje": "No existe un camino viable entre los puntos.",
            "origen": origen,
            "destino": destino
        }

    total_puntos = lt.size(camino)

    # 3. Último nodo dentro del radio
    ultimo_radio = ultimo_nodo_en_radio(camino, nodos, origen, radio_km)

    # 4. Distancia total del recorrido (Haversine)
    total_distancia = 0
    for i in range(total_puntos - 1):
        a = lt.get_element(camino, i)
        b = lt.get_element(camino, i + 1)

        na = buscar_nodo_por_id(nodos, a)
        nb = buscar_nodo_por_id(nodos, b)

        if na is not None and nb is not None:
            total_distancia += haversine(na["lat"], na["lon"], nb["lat"], nb["lon"])

    # 5. Detalles de 5 primeros y 5 últimos
    primeros = lt.new_list()
    ultimos = lt.new_list()

    limite = 5 if total_puntos >= 5 else total_puntos
    inicio_u = total_puntos - limite

    # primeros 5
    for i in range(limite):
        nid = lt.get_element(camino, i)
        lt.add_last(primeros, nid)

    # últimos 5
    for i in range(inicio_u, total_puntos):
        nid = lt.get_element(camino, i)
        lt.add_last(ultimos, nid)

    # 6. Información detallada: id, lat, lon, grullas, 3 primeras y 3 últimas, dist siguiente
    detalles = lt.new_list()

    for i in range(total_puntos):
        nid = lt.get_element(camino, i)
        nodo = buscar_nodo_por_id(nodos, nid)

        if nodo is None:
            info = {
                "id": nid,
                "lat": "Unknown",
                "lon": "Unknown",
                "num_grullas": "Unknown",
                "first3": "Unknown",
                "last3": "Unknown",
                "dist_next": "Unknown"
            }
            lt.add_last(detalles, info)
            continue

        tags = nodo["grullas"]
        gsize = lt.size(tags)

        first3 = lt.new_list()
        last3 = lt.new_list()

        limite_g = 3 if gsize >= 3 else gsize

        # primeras 3
        for j in range(limite_g):
            lt.add_last(first3, lt.get_element(tags, j))

        # ultimas 3
        inicio_g = gsize - 3 if gsize >= 3 else 0
        for j in range(inicio_g, gsize):
            lt.add_last(last3, lt.get_element(tags, j))

        # distancia al siguiente nodo
        if i < total_puntos - 1:
            nxt = lt.get_element(camino, i + 1)
            nodo2 = buscar_nodo_por_id(nodos, nxt)
            if nodo2 is not None:
                dist_sig = haversine(nodo["lat"], nodo["lon"], nodo2["lat"], nodo2["lon"])
            else:
                dist_sig = "Unknown"
        else:
            dist_sig = "Unknown"

        info = {
            "id": nodo["id"],
            "lat": nodo["lat"],
            "lon": nodo["lon"],
            "num_grullas": gsize,
            "first3": first3,
            "last3": last3,
            "dist_next": dist_sig
        }

        lt.add_last(detalles, info)

    # -----------------------------
    # EMPAQUETAR RESPUESTA
    # -----------------------------
    return {
        "origen": origen,
        "destino": destino,
        "ultimo_en_radio": ultimo_radio,
        "distancia_total": total_distancia,
        "total_puntos": total_puntos,
        "primeros_5_ids": primeros,
        "ultimos_5_ids": ultimos,
        "detalles": detalles
    }


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    - Saca el camino más largo (por # de nodos)
    - Calcula individuos totales
    - Extrae 5 primeros y 5 últimos nodos
    """
    # Obtener el camino más largo
    grafo = catalog["grafo_1"]
    nodos = catalog["nodos"]
    
    camino = longest_path_in_dag(grafo)
    if camino is None:
        return {
            "mensaje": "El grafo no es un DAG o no tiene camino válido."
        }
    total_puntos = lt.size(camino)
    individuos = mp.new_map(40000, 0.5)
    
    # Lista donde guardaremos los detalles finales de cada nodo
    detalles = lt.new_list()
    
    # Recorremos el camino
    for i in range(total_puntos):
        nid = lt.get_element(camino, i)

        nodo = buscar_nodo_por_id(nodos, nid)

        # Si por alguna razón el nodo no existe
        if nodo is None:
            nodo = {
                "id": nid,
                "lat": "Unknown",
                "lon": "Unknown",
                "grullas": lt.new_list()
            }

        grullas = nodo["grullas"]
        
        # registrar individuos únicos
        for j in range(lt.size(grullas)):
            gid = lt.get_element(grullas, j)
            mp.put(individuos, gid, True)
        
        gsize = lt.size(grullas)

        first3 = lt.new_list()
        last3 = lt.new_list()

        # primeras 3
        limite_first = 3 if gsize >= 3 else gsize
        for f in range(limite_first):
            lt.add_last(first3, lt.get_element(grullas, f))

        # últimas 3
        inicio_last = gsize - 3 if gsize >= 3 else 0
        for f in range(inicio_last, gsize):
            lt.add_last(last3, lt.get_element(grullas, f))
        
        dist_prev = "Unknown"
        dist_next = "Unknown"

        # nodo previo
        if i > 0:
            prev_id = lt.get_element(camino, i - 1)
            prev = buscar_nodo_por_id(nodos, prev_id)
            if prev is not None:
                dist_prev = haversine(nodo["lat"], nodo["lon"], prev["lat"], prev["lon"])

        # nodo siguiente
        if i < total_puntos - 1:
            next_id = lt.get_element(camino, i + 1)
            nxt = buscar_nodo_por_id(nodos, next_id)
            if nxt is not None:
                dist_next = haversine(nodo["lat"], nodo["lon"], nxt["lat"], nxt["lon"])
            
        
        info = {
            "id": nodo["id"],
            "lat": nodo["lat"],
            "lon": nodo["lon"],
            "num_grullas": gsize,
            "first3": first3,
            "last3": last3,
            "dist_prev": dist_prev,
            "dist_next": dist_next
        }

        lt.add_last(detalles, info)
        
    primeros_5 = lt.new_list()
    ultimo_5 = lt.new_list()
    dsize = lt.size(detalles)

    if dsize >= 5:
        limite = 5
        inicio = dsize - 5
    else:
        limite = dsize
        inicio = 0
        
    # primeros 5
    for x in range(limite):
        lt.add_last(primeros_5, lt.get_element(detalles, x))

    # últimos 5
    for x in range(inicio, dsize):
        lt.add_last(ultimo_5, lt.get_element(detalles, x))
        
    total_ind = lt.size(mp.key_set(individuos))
    
    return {
        "total_puntos": total_puntos,
        "total_individuos": total_ind,
        "primeros_5": primeros_5,
        "ultimos_5": ultimo_5
    }
    
def req_4(catalog, lat_o, lon_o):
    """
    Estima la red de puntos más cercanos a fuentes hídricas 
    que pueden usar los individuos para su migración desde un punto de inicio.
    Usa el algoritmo de Prim (MST) sobre el grafo de distancias a fuentes hídricas.
    """
    import math
    from DataStructures.Map import priority_queue as pq
    
    nodos = catalog["nodos"]
    grafo = catalog["grafo_2"]  # Grafo de distancias a fuentes hídricas

    # 1. Encontrar nodo migratorio más cercano al origen
    origen = buscar_nodo_mas_cercano(nodos, lat_o, lon_o)

    # Verificar que el nodo de origen existe
    nodo_origen_obj = buscar_nodo_por_id(nodos, origen)

    if nodo_origen_obj is None:
        return {
            "mensaje": f"El nodo de origen {origen} no existe.",
            "origen": origen
        }

    # 2. Ejecutar algoritmo de Prim desde el origen (inline)
    vertices = gp.vertices(grafo)
    num_vertices = lt.size(vertices)

    # Crear estructuras para Prim
    marked = mp.new_map(num_vertices, 0.5)
    edge_from = mp.new_map(num_vertices, 0.5)
    dist_to = mp.new_map(num_vertices, 0.5)
    heap = pq.new_heap(is_min_pq=True)

    # Inicializar todas las distancias a infinito
    for i in range(num_vertices):
        v = lt.get_element(vertices, i)
        mp.put(marked, v, False)
        mp.put(dist_to, v, math.inf)
        mp.put(edge_from, v, None)

    # El origen tiene distancia 0
    mp.put(dist_to, origen, 0)
    pq.insert(heap, 0, origen)

    # Algoritmo de Prim
    while not pq.is_empty(heap):
        u = pq.remove(heap)
        
        ya_marcado = mp.get(marked, u)
        if not ya_marcado:
            mp.put(marked, u, True)

            vertex = gp.get_vertex(grafo, u)
            
            if vertex is not None:
                adjs = gp.adjacents(grafo, u)

                for j in range(lt.size(adjs)):
                    v = lt.get_element(adjs, j)
                    v_marcado = mp.get(marked, v)
                    
                    if not v_marcado:
                        edge = vtx.get_edge(vertex, v)
                        
                        if edge is not None:
                            peso = edge["weight"]
                            dist_actual = mp.get(dist_to, v)

                            if peso < dist_actual:
                                mp.put(dist_to, v, peso)
                                mp.put(edge_from, v, {"from": u, "to": v, "weight": peso})
                                
                                existe_en_heap = pq.contains(heap, v)
                                if existe_en_heap:
                                    pq.improve_priority(heap, peso, v)
                                else:
                                    pq.insert(heap, peso, v)

    # 3. Extraer nodos del MST y calcular distancia total (inline)
    nodos_mst = lt.new_list()
    distancia_total_agua = 0.0
    
    claves = mp.key_set(marked)
    for i in range(lt.size(claves)):
        nodo_id = lt.get_element(claves, i)
        esta_marcado = mp.get(marked, nodo_id)
        
        if esta_marcado:
            lt.add_last(nodos_mst, nodo_id)
            
            # Calcular distancia al mismo tiempo
            edge = mp.get(edge_from, nodo_id)
            if edge is not None:
                distancia_total_agua += edge["weight"]
    
    total_puntos = lt.size(nodos_mst)

    # 4. Recolectar individuos únicos que usan el corredor
    mapa_individuos = mp.new_map(100, 0.5)

    for i in range(total_puntos):
        nodo_id = lt.get_element(nodos_mst, i)
        nodo_actual = buscar_nodo_por_id(nodos, nodo_id)

        if nodo_actual is not None:
            grullas = nodo_actual["grullas"]
            for j in range(lt.size(grullas)):
                grulla_id = lt.get_element(grullas, j)
                mp.put(mapa_individuos, grulla_id, True)

    lista_individuos = mp.key_set(mapa_individuos)
    total_individuos = lt.size(lista_individuos)

    # 5. Construir lista de detalles para TODOS los nodos del MST
    detalles_completos = lt.new_list()

    for i in range(total_puntos):
        nodo_id = lt.get_element(nodos_mst, i)
        nodo_actual = buscar_nodo_por_id(nodos, nodo_id)

        if nodo_actual is None:
            lt.add_last(detalles_completos, {
                "id": "Unknown",
                "lat": "Unknown",
                "lon": "Unknown",
                "num_grullas": "Unknown",
                "first3": lt.new_list(),
                "last3": lt.new_list()
            })
        else:
            tags = nodo_actual["grullas"]
            gsize = lt.size(tags)

            # Extraer primeras 3 grullas
            first3 = lt.new_list()
            limite_first = min(3, gsize)
            for j in range(limite_first):
                lt.add_last(first3, lt.get_element(tags, j))

            # Extraer últimas 3 grullas
            last3 = lt.new_list()
            inicio_last = max(0, gsize - 3)
            for j in range(inicio_last, gsize):
                lt.add_last(last3, lt.get_element(tags, j))

            lt.add_last(detalles_completos, {
                "id": nodo_actual["id"],
                "lat": nodo_actual["lat"],
                "lon": nodo_actual["lon"],
                "num_grullas": gsize,
                "first3": first3,
                "last3": last3
            })

    # 6. Crear lista combinada: primeros 5 + separador + últimos 5
    detalles_para_mostrar = lt.new_list()
    
    # Si hay 10 o menos nodos, mostrar todos
    if total_puntos <= 10:
        for i in range(total_puntos):
            lt.add_last(detalles_para_mostrar, lt.get_element(detalles_completos, i))
    else:
        # Más de 10 nodos: primeros 5 + separador + últimos 5
        # Agregar primeros 5
        for i in range(5):
            lt.add_last(detalles_para_mostrar, lt.get_element(detalles_completos, i))
        
        # Agregar separador
        lt.add_last(detalles_para_mostrar, {
            "id": "...",
            "lat": "...",
            "lon": "...",
            "num_grullas": "...",
            "first3": "...",
            "last3": "..."
        })
        
        # Agregar últimos 5
        for i in range(total_puntos - 5, total_puntos):
            lt.add_last(detalles_para_mostrar, lt.get_element(detalles_completos, i))

    # 7. Respuesta final
    return {
        "mensaje": f"Corredor hídrico construido desde el origen {origen}.",
        "origen": origen,
        "total_puntos": total_puntos,
        "total_individuos": total_individuos,
        "distancia_total_agua": distancia_total_agua,
        "detalles_mostrar": detalles_para_mostrar
    }

def req5(catalog, lat_user, lon_user, radio_km):

    nodos = catalog["nodos"]
    grafo = catalog["grafo_1"]  # se usa el grafo de distancias
    g2 = catalog["grafo_2"]     # para el check de "seguro hacia el este"

    # 1) Nodo más cercano al usuario
    origen = buscar_nodo_mas_cercano(nodos, lat_user, lon_user)

    # 2) Ejecutar Dijkstra usando la estructura del curso
    resultado = dk.dijkstra(grafo, origen)

    # 3) Hallar todos los nodos dentro del radio
    dentro = nodos_en_radio(nodos, origen, radio_km)

    # 4) De todos los nodos dentro del radio, elegir el más lejano (según Dijkstra)
    mejor = None
    mejor_dist = -1

    for i in range(lt.size(dentro)):
        destino = lt.get_element(dentro, i)
        d = dk.dist_to(resultado, destino)

        if d is not None and d > mejor_dist:
            mejor_dist = d
            mejor = destino

    if mejor is None:
        return None

    # 5) Obtener camino origen → mejor (solo con las estructuras del curso)
    camino = dk.path_to(resultado, mejor)

    # 6) Medir seguridad (promedio de agua en g2)
    seguridad_total = 0
    saltos = 0

    for i in range(lt.size(camino) - 1):
        u = lt.get_element(camino, i)
        v = lt.get_element(camino, i + 1)

        w = gp.get_edge_weight(g2, u, v)
        seguridad_total += w
        saltos += 1

    if saltos > 0:
        seguridad_prom = seguridad_total / saltos
    else:
        seguridad_prom = 0

    # 7) Preparar respuesta homogénea con REQ 2
    return {
        "origen": origen,
        "destino": mejor,
        "distancia_total": mejor_dist,
        "camino": camino,
        "saltos": lt.size(camino),
        "seguridad_prom": seguridad_prom
    }


def req_6(catalog):
    """
    Identifica grupos hídricos aislados (subredes).
    Retorna un diccionario con el total y la lista de subredes ordenada.
    """
    grafo = catalog["grafo_2"] # Grafo de proximidad hídrica
    nodos_global = catalog["nodos"]
    vertices = gp.vertices(grafo)
    num_vertices = lt.size(vertices)
    visitados_global = mp.new_map(num_vertices + 100, 0.5)
    lista_subredes = lt.new_list()
    contador_id = 1
    
    # iterar sobre todos los vértices del grafo
    for i in range(num_vertices):
        nodo_id = lt.get_element(vertices, i)
        
        # si ya pertenece a una subred, saltar
        if mp.contains(visitados_global, nodo_id):
            continue
        
        # encontramos una nueva subred: obtener todos los nodos conectados (DFS)
        ids_componente = obtener_nodos_dfs(grafo, nodo_id)
        
        # marcar estos nodos como visitados globalmente
        size_comp = lt.size(ids_componente)
        for j in range(size_comp):
            uid = lt.get_element(ids_componente, j)
            mp.put(visitados_global, uid, True)
            
        # calcular estadísticas y guardar
        stats = calcular_estadisticas_subred(contador_id, ids_componente, nodos_global)
        lt.add_last(lista_subredes, stats)
        
        contador_id += 1
        
    # ordenar por tamaño (mayor a menor)
    lista_ordenada = lt.merge_sort(lista_subredes, cmp_subred)
    
    return {
        "total_subredes": lt.size(lista_ordenada),
        "subredes": lista_ordenada
    }


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
