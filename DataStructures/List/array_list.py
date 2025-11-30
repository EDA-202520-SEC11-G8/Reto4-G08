from tracemalloc import start


def new_list():
    new_list = {
        "elements": [],
        "size": 0,
    }
    return new_list

def size(my_list):
    """
    Retorna el tamaño de la lista.
    """
    return my_list["size"]

def is_empty(my_list):
    return my_list["size"] == 0

def first_element(my_list):
    """
    Retorna el primer elemento de la lista.
    """
    if my_list["size"] == 0:
        print("first_element(): la lista está vacía")
        return None
    return my_list["elements"][0]
def last_element(my_list):
    """
    Eliminar el ultimo elemento de la lista.
    """
    if my_list["size"] == 0:
        print("last_element(): la lista está vacía")
        return None
    return my_list["elements"][-1]
  
def get_element(my_list, index):    
    if index < 0 or index >= size(my_list):
        print("get_element(): índice fuera de rango:", index)
        return None
    return my_list["elements"][index]

def is_present(my_list, element, cmp_function=None):
    n = size(my_list)
    for idx in range(n):
        info = my_list["elements"][idx]
        if cmp_function is None:
            if element == info:
                return idx
        else:
            res = cmp_function(element, info)
            # si el comparador devuelve int tipo 0/1/-1
            if isinstance(res, int):
                if res == 0:
                    return idx
            else:
                # asumimos booleano (True significa igual)
                if res:
                    return idx
    return -1
def add_first(my_list, element):
    """
    Agrega un elemento al inicio de la lista.
    Inserta el elemento al inicio de la lista y actualiza el tamaño de la lista en 1.
    """
    my_list["elements"].insert(0, element) 
    my_list["size"] += 1
    return my_list


def add_last(my_list, element):
    """Agrega un elemento al final de la lista.
    Inserta el elemento al final de la lista y aumenta el tamaño de la lista en 1.
    
    Args:
        my_list (array_list): Lista a la cual se le agregará el element
        element (any): Elemento a agregar

    Returns:
        array_list: Lista con el elemento agregado al final.
    """
    my_list["elements"].append(element) 
    my_list["size"] += 1
    return my_list

def remove_first(lst):
    """
    Elimina y retorna el primer elemento de la lista.
    Lanza IndexError si la lista está vacía.
    """
    if lst["size"] == 0:
        print("La lista está vacía")
        return None
    first = lst["elements"][0]
    # desplazar a la izquierda y ajustar tamaño
    lst["elements"].pop(0)
    lst["size"] -= 1
    return first

def remove_last(my_list):
    """
    Eliminar el ultimo elemento de la lista.
    """
    if my_list["size"] == 0:
        print("La lista está vacía, no se puede eliminar el último elemento")
        return None
    
    last_element = my_list["elements"].pop()
    
    my_list['size'] -= 1
    
    return last_element

def insert_element(my_list, pos, element):
    if pos < 0 or pos > size(my_list):
        print("insert_element(): posición fuera de rango:", pos)
        return my_list
    my_list["elements"].insert(pos, element)
    my_list["size"] += 1
    return my_list


def delete_element(my_list, pos):
    if pos < 0 or pos >= size(my_list):
        print("delete_element(): posición fuera de rango:", pos)
        return my_list
    my_list["elements"].pop(pos)
    my_list["size"] -= 1
    return my_list

def change_info(my_list, pos, new_info):
    """
    Reemplaza la información almacenada en la posición 'pos' por 'new_info'.
    Lanza IndexError si 'pos' está fuera de rango.
    """
    if pos < 0 or pos >= size(my_list):
        print("change_info(): posición fuera de rango:", pos)
        return my_list
    my_list["elements"][pos] = new_info
    return my_list

def exchange(my_list, pos1, pos2):
    """Intercambia dos elementos en las posiciones dadas dentro de la lista.
    Si una de las posiciones no es válida, lanza un error IndexError.
    """
    if pos1 < 0 or pos1 >= size(my_list) or pos2 < 0 or pos2 >= size(my_list):
        print("exchange(): posiciones fuera de rango", pos1, pos2)
        return my_list
    my_list["elements"][pos1], my_list["elements"][pos2] = my_list["elements"][pos2], my_list["elements"][pos1]
    return my_list


def sub_list(my_list, pos_i, num_elements):
    n = size(my_list)
    if n == 0:
        return {"elements": [], "size": 0}
    if pos_i < 0:
        print("sub_list(): pos_i negativo, ajustado a 0")
        pos_i = 0
    if num_elements < 0:
        print("sub_list(): num_elements negativo -> devolviendo vacía")
        return {"elements": [], "size": 0}
    if pos_i >= n:
        print("sub_list(): pos_i fuera de rango, devolviendo vacía")
        return {"elements": [], "size": 0}
    end = min(pos_i + num_elements, n)
    return {"elements": my_list["elements"][pos_i:end], "size": end - pos_i}

def default_sort_criteria(element_1, element_2)->bool:
    """Función de comparación por defecto para ordenar elementos.
    Compara dos elementos y retorna True si el primer elemento es menor que el segundo y False en caso contrario.
    """
    return element_1 <= element_2

def insertion_sort(my_list, cmp_function=default_sort_criteria):
    n = size(my_list)
    for i in range(1, n):
        key = my_list["elements"][i]
        j = i - 1
        while j >= 0 and cmp_function(key, my_list["elements"][j]):
            my_list["elements"][j + 1] = my_list["elements"][j]
            j -= 1
        my_list["elements"][j + 1] = key
    return my_list

def selection_sort(my_list, cmp_function=default_sort_criteria):
    n = size(my_list)
    if n <= 1:
        return my_list
    for i in range(n - 1):
        min_id = i
        for j in range(i + 1, n):
            if cmp_function(my_list["elements"][j], my_list["elements"][min_id]):
                min_id = j
        if min_id != i:
            my_list = exchange(my_list, i, min_id)
    return my_list

def shell_sort(my_list, sort_crit):
    """Ordena una lista utilizando el algoritmo de ordenamiento Shell Sort.
    Se recorre la lista y se ordena los elementos con un gap determinado. 
    Se repite el proceso con un gap menor hasta que la lista esté ordenada.
    """
    gap = my_list["size"] // 2
    
    while gap>0:
        for i in range(gap,my_list["size"]):
            temp = my_list["elements"][i]
            j = i 
            
            while j >= gap and not sort_crit(my_list["elements"][j-gap],temp):
                my_list["elements"][j] = my_list["elements"][j-gap]
                j -= gap
            
            my_list["elements"][j] = temp
            
        gap //= 2
        
    return my_list

def merge(left, right, cmp_function):
    """
    Mezcla dos sublistas ordenadas (left y right) en una nueva lista ordenada.
    """
    result = new_list()
    i = j = 0

    while i < size(left) and j < size(right):
        elem_left = get_element(left, i)
        elem_right = get_element(right, j)

        if cmp_function(elem_left, elem_right):
            add_last(result, elem_left)
            i += 1
        else:
            add_last(result, elem_right)
            j += 1

    while i < size(left):
        add_last(result, get_element(left, i))
        i += 1

    while j < size(right):
        add_last(result, get_element(right, j))
        j += 1

    return result

def merge_sort(my_list, cmp_function):
    """
    Ordena un array_list usando Merge Sort (recursivo).
    my_list: {"elements": [...], "size": n}
    cmp_function: función de comparación que retorna True si elem1 <= elem2
    """
    n = size(my_list)
    if n <= 1:
        return my_list

    mid = n // 2
    left = sub_list(my_list, 0, mid)
    right = sub_list(my_list, mid, n - mid)

    left_sorted = merge_sort(left, cmp_function)
    right_sorted = merge_sort(right, cmp_function)

    return merge(left_sorted, right_sorted, cmp_function)


def quick_sort(my_list, cmp_function):
    """Ordena un array_list usando QuickSort (recursivo).

    Args:
        my_list (dict):  {"elements": [...], "size": n}
        cmp_function (funcion): función para ordenar
    """
    n = size(my_list)
    if n <=1:
        return my_list
    
    # El primer elemento es el primer pivote
    pivot = get_element(my_list, 0)
    
    left = new_list()
    right = new_list()
    
    # Repartimos elementos en las dos listas
    for i in range(1,n):
        element = get_element(my_list, i)
        if cmp_function(element, pivot):
            add_last(left, element)
        else:
            add_last(right, element)
            
    # Recursion
    left_sorted = quick_sort(left, cmp_function)
    right_sorted = quick_sort(right, cmp_function)
    
    result = new_list()
    
    for i in range(size(left_sorted)):
        add_last(result, get_element(left_sorted, i))
        
    add_last(result, pivot)
    
    for i in range(size(right_sorted)):
        add_last(result, get_element(right_sorted, i))
        
    return result
