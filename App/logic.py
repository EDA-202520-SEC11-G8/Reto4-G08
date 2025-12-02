import time
import csv
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime


from DataStructures.List import array_list as lt
from DataStructures.Map import map_linear_probing as mp
from DataStructures.Graph import digraph as gp


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



            
# Funciones de consulta sobre el catálogo

def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


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
