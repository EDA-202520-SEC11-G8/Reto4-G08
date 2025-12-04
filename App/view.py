import sys
import time
from tabulate import tabulate as tb
import App.logic as l

from DataStructures.List import array_list as lt
from DataStructures.Graph import digraph as gp

def new_logic():
    """
        Se crea una instancia del controlador
    """
    control = l.new_logic()
    return control

def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    print("\nSeleccione el archivo que desea cargar:")
    print("1- 1000_cranes_mongolia_30pct.csv")
    print("2- 1000_cranes_mongolia_80pct.csv")
    print("3- 1000_cranes_mongolia_small.csv")
    print("4- 1000_cranes_mongolia_large.csv")

    opcion = input("\nIngrese una opción (1-4): ")
    
    if opcion == "1":
        filename = "1000_cranes_mongolia_30pct.csv"
    elif opcion == "2":
        filename = "1000_cranes_mongolia_80pct.csv"
    elif opcion == "3":
        filename = "1000_cranes_mongolia_small.csv"
    elif opcion == "4":
        filename = "1000_cranes_mongolia_large.csv"
    else:
        print("Opción inválida. Intente de nuevo.")
        return
    
    print(f"\nCargando información desde Data/{filename}...\n")
    
    start = l.get_time()
    res = l.load_data(control, filename)
    end = l.get_time()
    tiempo = round(l.delta_time(start, end), 2)
    
    
    nodos = control["nodos"]
    grafo1 = control["grafo_1"]
    grafo2 = control["grafo_2"]
    
    
    # ===========================================================
    #          IMPRESIÓN DEL RESUMEN DE CARGA
    # ===========================================================
    print("\n======================================================")
    print("                  CARGA DE DATOS")
    print("======================================================")
    
    total_eventos = res["num_eventos"]
    total_nodos = res["num_nodos"]

    contador_grulla = contar_grullas(nodos)
    
    print(f"- Total de grullas reconocidas        : {contador_grulla}")
    print(f"- Total de eventos cargados           : {total_eventos}")
    print(f"- Total de nodos construidos          : {total_nodos}")
    print(f"- Tiempo de carga (segundos)          : {tiempo}")

    # ===========================================================
    #   GRAFO 1 (Distancias migratorias)
    # ===========================================================
    print("\n======================================================")
    print("              GRAFO 1 – DISTANCIAS MIGRATORIAS")
    print("======================================================")
    print(f"- Total de vértices : {gp.order(grafo1)}")
    print(f"- Total de arcos    : {gp.size(grafo1)}")
    
    print("\n--- Primeros 5 nodos ---")
    print(tb(nodos_to_table(nodos, primeros=True), headers="keys", tablefmt="grid"))

    print("\n--- Últimos 5 nodos ---")
    print(tb(nodos_to_table(nodos, primeros=False), headers="keys", tablefmt="grid"))
    

    # ===========================================================
    #   GRAFO 2 (Diferencia de agua)
    # ===========================================================

    print("\n======================================================")
    print("        GRAFO 2 – PROXIMIDAD A FUENTES HÍDRICAS")
    print("======================================================")
    print(f"- Total de vértices : {gp.order(grafo2)}")
    print(f"- Total de arcos    : {gp.size(grafo2)}")

    print("\n--- Primeros 5 nodos ---")
    print(tb(nodos_to_table(nodos, primeros=True, incluir_distancia=True), headers="keys", tablefmt="grid"))

    print("\n--- Últimos 5 nodos ---")
    print(tb(nodos_to_table(nodos, primeros=False, incluir_distancia=True), headers="keys", tablefmt="grid"))

def buscar_detalle(detalles, nid):
    """
    Busca en la lista 'detalles' la info del nodo con id = nid.
    """
    for i in range(lt.size(detalles)):
        d = lt.get_element(detalles, i)
        if d["id"] == nid:
            return d
    return None

def nodos_to_table(nodos, primeros=True, incluir_distancia=False):
    """
    Convierte los primeros o últimos 5 nodos en una tabla imprimible.
    """
    total = lt.size(nodos)
    tabla = []

    if primeros:
        r_inicio, r_fin = 0, min(5, total)
    else:
        r_inicio, r_fin = max(0, total - 5), total

    for i in range(r_inicio, r_fin):
        nodo = lt.get_element(nodos, i)

        grullas = []
        for j in range(lt.size(nodo["grullas"])):
            grullas.append(lt.get_element(nodo["grullas"], j))

        fila = {
            "Identificador único": nodo["id"],
            "Posición (lat,long)": f"({nodo['lat']:.4f}, {nodo['lon']:.4f})",
            "Fecha de creación": nodo["timestamp"],
            "Grullas (tags)": grullas,
            "Conteo de eventos": lt.size(nodo["eventos"])
        }
        if incluir_distancia:
            fila["Dist. Hídri prom (km)"] = round(nodo["prom_agua"],4)
            
        tabla.append(fila)

    return tabla

def contar_grullas(nodos):
    """
    Cuenta grullas únicas.
    """
    grullas = {} # Dico para guardar unicos

    n = lt.size(nodos)
    for i in range(n):
        nodo = lt.get_element(nodos, i)
        lista_grullas = nodo["grullas"]

        for j in range(lt.size(lista_grullas)):
            g = lt.get_element(lista_grullas, j)
            grullas[g] = True

    return len(grullas)


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    #TODO: Realizar la función para imprimir un elemento
    pass

def print_req_1(control):
    """
    Función que imprime la solución del Requerimiento 1 en consola
    """
    print("\n===== Resultados del Requerimiento 1 =====")
    
    # Entrada del usuario
    lat_o = float(input("Ingrese la latitud del punto de origen: "))
    lon_o = float(input("Ingrese la longitud del punto de origen: "))
    lat_d = float(input("Ingrese la latitud del punto de destino: "))
    lon_d = float(input("Ingrese la longitud del punto de destino: "))
    crane_id = input("Ingrese el identificador único de la grulla: ")
    
    # Ejecutar requerimiento
    inicio = time.time()
    res = l.req_1(control, lat_o, lon_o, lat_d, lon_d, crane_id)
    fin = time.time()
    
    print(f"\nTiempo de ejecución: {round((fin - inicio) * 1000, 2)} ms\n")
    
    # Verificar si hay camino válido
    if "detalles_mostrar" not in res:
        print(res.get("mensaje", "No se encontró información del camino."))
        print(f"- Nodo origen más cercano : {res.get('origen', 'Unknown')}")
        print(f"- Nodo destino más cercano: {res.get('destino', 'Unknown')}")
        print("")
        return
    
    # Resumen general
    print(res["mensaje"])
    print(f"- Primer nodo donde se encuentra la grulla: {res['origen']}")
    print(f"- Grulla identificada : {res['grulla']}")
    print(f"- Distancia total del camino (km): {round(res['distancia_total'], 2)}")
    print(f"- Total de puntos del camino: {res['total_puntos']}\n")
    
    print("Nodos del recorrido (primeros 5 y últimos 5):\n")
    
    # Construir tabla directamente (inline de convertir_a_filas)
    filas = []
    detalles = res["detalles_mostrar"]
    
    for i in range(lt.size(detalles)):
        nodo = lt.get_element(detalles, i)
        
        # Procesar latitud
        lat = nodo.get("lat")
        lat_final = "..." if lat == "..." else round(float(lat), 4)
        
        # Procesar longitud
        lon = nodo.get("lon")
        lon_final = "..." if lon == "..." else round(float(lon), 4)
        
        # Procesar first3
        first3 = nodo.get("first3")
        if first3 == "...":
            first3_final = "..."
        else:
            lista_first = []
            for j in range(lt.size(first3)):
                lista_first.append(str(lt.get_element(first3, j)))
            first3_final = ", ".join(lista_first) if lista_first else "N/A"
        
        # Procesar last3
        last3 = nodo.get("last3")
        if last3 == "...":
            last3_final = "..."
        else:
            lista_last = []
            for j in range(lt.size(last3)):
                lista_last.append(str(lt.get_element(last3, j)))
            last3_final = ", ".join(lista_last) if lista_last else "N/A"
        
        filas.append({
            "ID": nodo.get("id"),
            "Lat": lat_final,
            "Lon": lon_final,
            "#Grullas": nodo.get("num_grullas"),
            "3 primeras": first3_final,
            "3 últimas": last3_final,
            "Dist next": nodo.get("dist_next")
        })
    
    print(tb(filas, headers="keys", tablefmt="presto"))
    print("")


def print_req_2(control):
    """
    Función que imprime la solución del Requerimiento 2 en consola
    """
    print("\n===== Resultados del Requerimiento 2 =====")

    # Pedir parámetros al usuario
    lat_o = float(input("Ingrese la latitud del punto de origen: "))
    lon_o = float(input("Ingrese la longitud del punto de origen: "))
    lat_d = float(input("Ingrese la latitud del punto de destino: "))
    lon_d = float(input("Ingrese la longitud de destino: "))
    radio = float(input("Ingrese el radio de interés (km): "))

    # Ejecutar requerimiento
    inicio = time.time()
    res = l.req_2(control, lat_o, lon_o, lat_d, lon_d, radio)
    fin = time.time()

    print(f"\nTiempo de ejecución: {round((fin - inicio) * 1000, 2)} ms\n")

    # Si no hay camino
    if "detalles_mostrar" not in res:  
        print(res.get("mensaje", "No existe camino"))
        print(f"- Nodo origen más cercano : {res['origen']}")
        print(f"- Nodo destino más cercano: {res['destino']}")
        print("")
        return

    # -------------------------
    # Resumen general del REQ 2
    # -------------------------
    print(f"- Nodo migratorio origen : {res['origen']}")
    print(f"- Nodo migratorio destino: {res['destino']}")
    print(f"- Último nodo dentro del área (radio={radio} km): {res['ultimo_en_radio']}")
    print(f"- Distancia total del camino (km): {round(res['distancia_total'], 4)}")
    print(f"- Total de puntos del camino: {res['total_puntos']}\n")

    print("Nodos del recorrido (primeros 5 y últimos 5):\n")
    
    filas = []
    detalles_mostrar = res["detalles_mostrar"]  
    
    for i in range(lt.size(detalles_mostrar)):
        nodo = lt.get_element(detalles_mostrar, i)
        
        # Procesar latitud
        lat = nodo.get("lat")
        lat_final = "..." if lat == "..." else round(float(lat), 4)
        
        # Procesar longitud
        lon = nodo.get("lon")
        lon_final = "..." if lon == "..." else round(float(lon), 4)
        
        # Procesar first3
        first3 = nodo.get("first3")
        if first3 == "...":
            first3_final = "..."
        else:
            lista_first = []
            for j in range(lt.size(first3)):
                lista_first.append(str(lt.get_element(first3, j)))
            first3_final = ", ".join(lista_first) if lista_first else "N/A"
        
        # Procesar last3
        last3 = nodo.get("last3")
        if last3 == "...":
            last3_final = "..."
        else:
            lista_last = []
            for j in range(lt.size(last3)):
                lista_last.append(str(lt.get_element(last3, j)))
            last3_final = ", ".join(lista_last) if lista_last else "N/A"
        
        # Procesar distancia
        dist_next = nodo.get("dist_next")
        if dist_next == "Unknown" or dist_next == "...":
            dist_final = dist_next
        else:
            dist_final = round(float(dist_next), 4)
        
        filas.append({
            "ID": nodo.get("id"),
            "Lat": lat_final,
            "Lon": lon_final,
            "#Grullas": nodo.get("num_grullas"),
            "3 primeras": first3_final,
            "3 últimas": last3_final,
            "Dist next": dist_final
        })
    
    print(tb(filas, headers="keys", tablefmt="presto"))
    print("")


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    inicio = time.time()
    res = l.req_3(control)
    
    fin = time.time()
    
    print("\n===== Resultados del Requerimiento 3 =====")
    print(f"Tiempo de ejecución: {fin - inicio} ms\n")
    if "mensaje" in res:
        print(res["mensaje"])
        print("")
        return
    print(f"- Total de puntos en la ruta : {res['total_puntos']}")
    print(f"- Total de individuos que usan la ruta : {res['total_individuos']}")
    
    filas = []
    N = 5
    primeros = res["primeros_5"]
    ultimos = res["ultimos_5"]
    
    for p in primeros:
        filas.append({
            "ID": p["id"],
            "Lat": p["lat"],
            "Lon": p["lon"],
            "#Grullas": p["num_grullas"],
            "3 primeras": p["first3"],
            "3 últimas": p["last3"],
            "Dist prev": p["dist_prev"],
            "Dist next": p["dist_next"]
        })
        
    if res["total_puntos"] > 2 * N:
        filas.append({
            "ID": "...", "Lat": "...", "Lon": "...",
            "#Grullas": "...",
            "3 primeras": "...",
            "3 últimas": "...",
            "Dist prev": "...",
            "Dist next": "..."
        })

    # últimos 5
    for p in ultimos:
        filas.append({
            "ID": p["id"],
            "Lat": p["lat"],
            "Lon": p["lon"],
            "#Grullas": p["num_grullas"],
            "3 primeras": p["first3"],
            "3 últimas": p["last3"],
            "Dist prev": p["dist_prev"],
            "Dist next": p["dist_next"]
        })

    print(tb(filas, headers="keys", tablefmt="presto"))
    print("")


def print_req_4(control):
    """
    Función que imprime la solución del Requerimiento 4 en consola
    """
    print("\n===== Resultados del Requerimiento 4 =====")
    
    # Entrada del usuario
    lat_o = float(input("Ingrese la latitud del punto de origen: "))
    lon_o = float(input("Ingrese la longitud del punto de origen: "))
    
    # Ejecutar requerimiento
    inicio = time.time()
    res = l.req_4(control, lat_o, lon_o)
    fin = time.time()
    
    print(f"\nTiempo de ejecución: {round((fin - inicio) * 1000, 2)} ms\n")
    
    # Verificar si hay detalles válidos
    if "detalles_mostrar" not in res:
        print(res.get("mensaje", "No se pudo construir el corredor hídrico."))
        print(f"- Nodo origen más cercano : {res.get('origen', 'Unknown')}")
        print("")
        return
    
    # Resumen general
    print(res["mensaje"])
    print(f"- Nodo migratorio origen : {res['origen']}")
    print(f"- Total de puntos en el corredor: {res['total_puntos']}")
    print(f"- Total de individuos que usan el corredor: {res['total_individuos']}")
    print(f"- Distancia total del corredor a fuentes hídricas (km): {round(res['distancia_total_agua'], 2)}\n")

    print("Nodos del corredor hídrico (primeros 5 y últimos 5):\n")
    
    # Construir tabla directamente (inline de convertir_a_filas)
    filas = []
    detalles = res["detalles_mostrar"]
    
    for i in range(lt.size(detalles)):
        nodo = lt.get_element(detalles, i)
        
        # Procesar latitud
        lat = nodo.get("lat")
        lat_final = "..." if lat == "..." else round(float(lat), 4)
        
        # Procesar longitud
        lon = nodo.get("lon")
        lon_final = "..." if lon == "..." else round(float(lon), 4)
        
        # Procesar first3
        first3 = nodo.get("first3")
        if first3 == "...":
            first3_final = "..."
        else:
            lista_first = []
            for j in range(lt.size(first3)):
                lista_first.append(str(lt.get_element(first3, j)))
            first3_final = ", ".join(lista_first) if lista_first else "N/A"
        
        # Procesar last3
        last3 = nodo.get("last3")
        if last3 == "...":
            last3_final = "..."
        else:
            lista_last = []
            for j in range(lt.size(last3)):
                lista_last.append(str(lt.get_element(last3, j)))
            last3_final = ", ".join(lista_last) if lista_last else "N/A"
        
        filas.append({
            "ID": nodo.get("id"),
            "Lat": lat_final,
            "Lon": lon_final,
            "#Grullas": nodo.get("num_grullas"),
            "3 primeras": first3_final,
            "3 últimas": last3_final
        })
    
    print(tb(filas, headers="keys", tablefmt="presto"))
    print("")


def print_req_5(control):
    """
    Función que imprime la solución del Requerimiento 5 en consola
    """
    print("\n===== Resultados del Requerimiento 5 =====")

    # ----------------------------------------
    # Entrada del usuario
    # ----------------------------------------
    lat_o = float(input("Ingrese la latitud del punto de origen: "))
    lon_o = float(input("Ingrese la longitud del punto de origen: "))
    lat_d = float(input("Ingrese la latitud del punto de destino: "))
    lon_d = float(input("Ingrese la longitud del punto de destino: "))

    print("\nSeleccione el tipo de optimización:")
    print("1 - DISTANCIA de desplazamiento")
    print("2 - PROXIMIDAD a fuentes hídricas")

    grafo_sel = input("Ingrese 1 o 2: ").strip()

    if grafo_sel == "1":
        tipo = "distancia"
    elif grafo_sel == "2":
        tipo = "hidrico"  # ✅ CAMBIO: tu req_5 espera "hidrico"
    else:
        print("Opción inválida.\n")
        return

    # ----------------------------------------
    # Ejecutar requerimiento
    # ----------------------------------------
    inicio = time.time()
    res = l.req_5(control, lat_o, lon_o, lat_d, lon_d, tipo)
    fin = time.time()

    print(f"\nTiempo de ejecución: {round((fin - inicio)*1000, 2)} ms\n")

    # ----------------------------------------
    # Verificar si hay error
    # ----------------------------------------
    if res is None or "detalles_mostrar" not in res:
        print(res.get("mensaje", "No se encontró camino válido"))
        if res:
            print(f"- Nodo origen : {res.get('origen', 'Unknown')}")
            print(f"- Nodo destino: {res.get('destino', 'Unknown')}")
        print("")
        return

    # ----------------------------------------
    # Resumen general
    # ----------------------------------------
    print(res.get("mensaje", "Ruta encontrada"))
    print(f"- Tipo de optimización: {res['tipo_optimizacion']}")
    print(f"- Nodo migratorio origen : {res['origen']}")
    print(f"- Nodo migratorio destino: {res['destino']}")
    print(f"- Costo total del camino : {round(res['costo_total'], 4)}")
    print(f"- Total de puntos (vértices): {res['total_puntos']}")
    print(f"- Total de arcos : {res['total_arcos']}\n")  # ✅ CAMBIO: "total_arcos"

    # ----------------------------------------
    # Mostrar tabla usando detalles_mostrar
    # ----------------------------------------
    print("Nodos del recorrido (primeros 5 y últimos 5):\n")
    
    filas = []
    detalles_mostrar = res["detalles_mostrar"]  # ✅ CAMBIO
    
    for i in range(lt.size(detalles_mostrar)):
        nodo = lt.get_element(detalles_mostrar, i)
        
        # Procesar latitud
        lat = nodo.get("lat")
        lat_final = "..." if lat == "..." else round(float(lat), 4)
        
        # Procesar longitud
        lon = nodo.get("lon")
        lon_final = "..." if lon == "..." else round(float(lon), 4)
        
        # Procesar first3
        first3 = nodo.get("first3")
        if first3 == "...":
            first3_final = "..."
        else:
            lista_first = []
            for j in range(lt.size(first3)):
                lista_first.append(str(lt.get_element(first3, j)))
            first3_final = ", ".join(lista_first) if lista_first else "N/A"
        
        # Procesar last3
        last3 = nodo.get("last3")
        if last3 == "...":
            last3_final = "..."
        else:
            lista_last = []
            for j in range(lt.size(last3)):
                lista_last.append(str(lt.get_element(last3, j)))
            last3_final = ", ".join(lista_last) if lista_last else "N/A"
        
        # Procesar peso siguiente
        peso_sig = nodo.get("peso_siguiente")  # ✅ CAMBIO: no "dist_next"
        if peso_sig == "Unknown" or peso_sig == "...":
            peso_final = peso_sig
        else:
            peso_final = round(float(peso_sig), 4)
        
        filas.append({
            "ID": nodo.get("id"),
            "Lat": lat_final,
            "Lon": lon_final,
            "#Grullas": nodo.get("num_grullas"),
            "3 primeras": first3_final,
            "3 últimas": last3_final,
            "Peso Sig.": peso_final  # ✅ CAMBIO: nombre más descriptivo
        })
    
    print(tb(filas, headers="keys", tablefmt="presto"))
    print("")

def print_req_6(control):
    """
    Función que imprime la solución del Requerimiento 6 en consola.
    """
    print("\nEjecutando análisis de subredes hídricas (Req 6)...")
    
    start = l.get_time()
    resultado = l.req_6(control) # Retorna diccionario
    end = l.get_time()
    
    tiempo = round(l.delta_time(start, end), 2)
    
    # Extraer datos del retorno
    total = resultado["total_subredes"]
    lista = resultado["subredes"]
    
    print(f"\n===== Resultados del Requerimiento 6 =====")
    print(f"Tiempo de ejecución: {tiempo} ms")
    print(f"Total de subredes hídricas identificadas: {total}")
    
    # Definir cuántas mostrar (Top 5) 
    cantidad_mostrar = 5
    if total < 5:
        cantidad_mostrar = total
        
    print(f"\nMostrando las {cantidad_mostrar} subredes más grandes:\n")
    
    for i in range(cantidad_mostrar):
        sub = lt.get_element(lista, i)
        
        # Datos generales
        id_sub = sub["id_subred"]
        n_puntos = sub["total_nodos"]
        n_ind = sub["total_individuos"]
        lat_min, lat_max = sub["rango_lat"]
        lon_min, lon_max = sub["rango_lon"]
        
        print(f"--------------------------------------------------")
        print(f"SUBRED {id_sub}")
        print(f" > Total Puntos Migratorios : {n_puntos}")
        print(f" > Total Individuos         : {n_ind}")
        print(f" > Latitud (Min, Max)       : ({lat_min:.4f}, {lat_max:.4f})")
        print(f" > Longitud (Min, Max)      : ({lon_min:.4f}, {lon_max:.4f})")
        
        # Tabla de puntos (3 primeros / 3 últimos)
        print("\n > Detalle de Puntos (3 primeros / 3 últimos):")
        
        filas_tabla = []
        
        # Procesar primeros 3 nodos
        p_nodos = sub["primeros_nodos"]
        for k in range(lt.size(p_nodos)):
            nodo = lt.get_element(p_nodos, k)
            filas_tabla.append({
                "ID": nodo["id"],
                "Lat": f"{nodo['lat']:.4f}",
                "Lon": f"{nodo['lon']:.4f}"
            })
            
        # Separador visual si hay muchos nodos
        if n_puntos > 6:
            filas_tabla.append({"ID": "...", "Lat": "...", "Lon": "..."})
            
        # Procesar últimos 3 nodos
        u_nodos = sub["ultimos_nodos"]
        for k in range(lt.size(u_nodos)):
            nodo = lt.get_element(u_nodos, k)
            filas_tabla.append({
                "ID": nodo["id"],
                "Lat": f"{nodo['lat']:.4f}",
                "Lon": f"{nodo['lon']:.4f}"
            })
            
        print(tb(filas_tabla, headers="keys", tablefmt="presto"))
        
        # Listado de grullas (3 primeras / 3 últimas)
        print("\n > Grullas en la subred (Tags):")
        tags_str = []
        
        p_grullas = sub["primeras_grullas"]
        for k in range(lt.size(p_grullas)):
            tags_str.append(str(lt.get_element(p_grullas, k)))
            
        if n_ind > 6:
            tags_str.append("...")
            
        u_grullas = sub["ultimas_grullas"]
        for k in range(lt.size(u_grullas)):
            tags_str.append(str(lt.get_element(u_grullas, k)))
            
        print(f"   [{', '.join(tags_str)}]")
        print("")
        
        
# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 6:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
