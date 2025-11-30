from DataStructures.Map import pq_entry as pqe
from DataStructures.List import array_list as al

def new_heap(is_min_pq=True):
    elements = al.new_list()
    elements = al.add_last(elements,None)
    
    if is_min_pq:
        cmp_function = default_compare_lower_value
    else:
        cmp_function = default_compare_higher_value
        
        
    my_heap = {
        "elements": elements,
        "size": 0,
        "cmp_function": cmp_function
    }

    return my_heap

def default_compare_lower_value(father_node, child_node):
    """
    Función de comparación por defecto para un heap orientado al menor.

    Retorna True si la prioridad del nodo padre es menor o igual que la del nodo hijo.
    De lo contrario, retorna False.
    """
    if pqe.get_priority(father_node) <= pqe.get_priority(child_node):
        return True
    return False


def default_compare_higher_value(father_node, child_node):
    """
    Función de comparación por defecto para un heap orientado al mayor.

    Retorna True si la prioridad del nodo padre es mayor o igual que la del nodo hijo.
    De lo contrario, retorna False.
    """
    if pqe.get_priority(father_node) >= pqe.get_priority(child_node):
        return True
    return False

def exchange(my_heap, pos1, pos2):
    """
    Intercambia los elementos en las posiciones pos1 y pos2 dentro del heap.
    """
    elements = my_heap["elements"]["elements"]
    temp = elements[pos1]
    elements[pos1] = elements[pos2]
    elements[pos2] = temp
    
    
    
    
def priority(my_heap, parent, child):
    """
    Indica si el nodo 'parent' tiene mayor prioridad que el nodo 'child'
    según la función de comparación del heap.
    """
    return my_heap["cmp_function"],(parent, child)


def size(my_heap):
    return my_heap["size"]

def is_empty(my_heap):
    return size(my_heap) == 0
 
def swim(my_heap, pos):
    list = my_heap["elements"]["elements"]
    while pos > 1:
        pri = priority(my_heap, list[pos // 2], list[pos])
        if pri[0] == default_compare_lower_value:
            if not default_compare_lower_value(pri[1][0], pri[1][1]):
                list[pos // 2], list[pos] = list[pos], list[pos // 2]
                pos = pos // 2
            else:
                pos = 1
        elif pri[0] == default_compare_higher_value:
            if not default_compare_higher_value(pri[1][0], pri[1][1]):
                list[pos // 2], list[pos] = list[pos], list[pos // 2]
                pos = pos // 2
            else:
                pos = 1
        else:
            pos = 1
    return my_heap

def insert(my_heap, priority, value):
    new = pqe.new_pq_entry(priority, value)
    my_heap["elements"]["elements"].append(new)
    my_heap["elements"]["size"] += 1
    my_heap["size"] += 1
    swim(my_heap, my_heap["size"])
    return my_heap


def sink(my_heap, pos):
    """Baja el elemento en la posición 'pos' hasta su lugar correcto"""
    elements = my_heap["elements"]["elements"]
    size = my_heap["size"]

    # Mientras tenga hijos
    while 2 * pos <= size:
        j = 2 * pos  # hijo izquierdo

        # Elegir el mejor hijo según min/max heap
        if j < size and not my_heap["cmp_function"](elements[j], elements[j + 1]):
            j += 1  # hijo derecho es mejor

        # Si el padre ya tiene mayor prioridad que el hijo, parar
        if my_heap["cmp_function"](elements[pos], elements[j]):
            break

        # Intercambiar padre con el mejor hijo
        elements[pos], elements[j] = elements[j], elements[pos]
        pos = j

    return my_heap


def remove(my_heap):
    """Elimina y retorna el valor con mayor prioridad"""
    if is_empty(my_heap):
        return None

    elements = my_heap["elements"]["elements"]
    root = elements[1]["value"]  # valor del primero

    # Mover el último al primer lugar
    elements[1] = elements[my_heap["size"]]
    elements.pop()

    # Actualizar tamaño
    my_heap["size"] -= 1
    my_heap["elements"]["size"] -= 1

    # Reorganizar heap si no está vacío
    if my_heap["size"] > 0:
        sink(my_heap, 1)

    return root

def get_first_priority(my_heap):
    """Retorna el valor con mayor prioridad sin eliminarlo"""
    if is_empty(my_heap):
        return None
    return my_heap["elements"]["elements"][1]["value"]


def is_present_value(my_heap, value):
    """Busca un valor en el heap y retorna su posición, o -1 si no está"""
    elements = my_heap["elements"]["elements"]
    for i in range(1, my_heap["size"] + 1):
        if pqe.get_value(elements[i]) == value:
            return i
    return -1

def contains(my_heap, value):
    """Retorna True si el valor existe en el heap"""
    return is_present_value(my_heap, value) != -1

def improve_priority(my_heap, priority, value):
    """
    Mejora la prioridad de un valor existente si la nueva prioridad es mejor.
    """
    pos = is_present_value(my_heap, value)
    if pos == -1:
        return my_heap  # valor no encontrado

    entry = my_heap["elements"]["elements"][pos]

    temp = pqe.new_pq_entry(priority, value)  # nodo temporal para comparar

    # Si la nueva prioridad no es mejor, no hacer nada
    if my_heap["cmp_function"](entry, temp):
        return my_heap

    # Actualizar prioridad
    pqe.set_priority(entry, priority)

    # Reorganizar hacia arriba
    swim(my_heap, pos)
    return my_heap