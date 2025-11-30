def new_node(value):
    return {"info": value, "next": None}

def new_list():
    return {
        "size": 0,
        "first": None,
        "last": None
    }

def add_first(lst, element):
    node = {"info": element, "next": lst["first"]}
    lst["first"] = node
    if lst["size"] == 0:
        lst["last"] = node
    lst["size"] += 1
    return lst  

def add_last(lst, element):
    node = {"info": element, "next": None}
    if lst["size"] == 0:
        lst["first"] = node
    else:
        lst["last"]["next"] = node
    lst["last"] = node
    lst["size"] += 1
    return lst  

def size(lst):
    return lst["size"]

def first_element(lst):
    if lst["size"] == 0:
        print("IndexError: list is empty")
        return None
    return lst["first"]["info"]

def get_element(my_list, pos):
    if pos < 0 or pos >= my_list["size"]:
        print("IndexError: list index out of range")
        return None
    searchpos = 0
    node = my_list["first"]
    while searchpos < pos:
        node = node["next"]
        searchpos += 1
    return node["info"]

def is_present(my_list, element, cmp_function):
    is_in_array = False
    temp = my_list["first"]
    count = 0
    while not is_in_array and temp is not None:
        if cmp_function(element, temp["info"]) == 0:
            is_in_array = True
        else:
            temp = temp["next"]
            count += 1
    if not is_in_array:
        count = -1
    return count

def remove_first(my_list):
    if my_list["size"] == 0:
        print("IndexError: list is empty")
        return None
    first_data = my_list["first"]["info"]
    my_list["first"] = my_list["first"]["next"]
    my_list["size"] -= 1
    if my_list["size"] == 0:
        my_list["last"] = None
    return first_data

def change_info(my_list, pos, new_info):
    if pos < 0 or pos >= my_list["size"]:
        print("IndexError: list index out of range")
        return my_list
    current = my_list["first"]
    current_index = 0
    while current_index < pos:
        current = current["next"]
        current_index += 1
    current["info"] = new_info
    return my_list

def last_element(my_list):
    if my_list["size"] == 0 or my_list["last"] is None:
        print("IndexError: list is empty")
        return None
    return my_list["last"]["info"]

def remove_last(lst):
    if lst["size"] == 0:
        print("IndexError: list is empty")
        return None
    if lst["size"] == 1:
        info = lst["first"]["info"]
        lst["first"] = None
        lst["last"] = None
        lst["size"] = 0
        return info
    prev = None
    curr = lst["first"]
    while curr["next"] is not None:
        prev = curr
        curr = curr["next"]
    info = curr["info"]
    prev["next"] = None
    lst["last"] = prev
    lst["size"] -= 1
    return info

def sub_list(lst, pos_i, num_elements):
    if pos_i < 0 or num_elements < 0:
        print("IndexError: invalid indexes")
        return None
    if pos_i >= lst["size"] or pos_i + num_elements > lst["size"]:
        print("IndexError: range out of list")
        return None
    new_lst = new_list()
    curr = lst["first"]
    idx = 0
    while idx < pos_i and curr is not None:
        curr = curr["next"]
        idx += 1
    copied = 0
    while copied < num_elements and curr is not None:
        add_last(new_lst, curr["info"])
        curr = curr["next"]
        copied += 1
    return new_lst

def insert_element(my_list, pos, element):
    if pos < 0 or pos > my_list["size"]:
        print("IndexError: list index out of range")
        return my_list
    node = new_node(element)
    if pos == 0:
        node["next"] = my_list["first"]
        my_list["first"] = node
        if my_list["size"] == 0:
            my_list["last"] = node
        my_list["size"] += 1
        return my_list
    current = my_list["first"]
    index = 0
    while index < pos - 1:
        current = current["next"]
        index += 1
    node["next"] = current["next"]
    current["next"] = node
    if node["next"] is None:
        my_list["last"] = node
    my_list["size"] += 1
    return my_list

def is_empty(my_list):
    return my_list["size"] == 0

def delete_element(my_list, pos):
    if pos < 0 or pos >= size(my_list):
        print("IndexError: list index out of range")
        return my_list
    if pos == 0:
        remove_first(my_list)
        return my_list
    current = my_list["first"]
    index = 0
    while index < pos - 1:
        current = current["next"]
        index += 1
    to_delete = current["next"]
    current["next"] = to_delete["next"]
    my_list["size"] -= 1
    if current["next"] is None:
        my_list["last"] = current
    return my_list

def exchange(my_list, pos1, pos2):
    if pos1 < 0 or pos1 >= my_list["size"] or pos2 < 0 or pos2 >= my_list["size"]:
        print("IndexError: list index out of range")
        return my_list
    if pos1 == pos2:
        return my_list
    if pos1 > pos2:
        pos1, pos2 = pos2, pos1
    node1 = my_list["first"]
    for _ in range(pos1):
        node1 = node1["next"]
    node2 = my_list["first"]
    for _ in range(pos2):
        node2 = node2["next"]
    node1["info"], node2["info"] = node2["info"], node1["info"]
    return my_list

def default_sort_criteria(element_1, element_2)->bool:
    """Función de comparación por defecto para ordenar elementos.
    Compara dos elementos y retorna True si el primer elemento es menor que el segundo y False en caso contrario.

    Args:
        element_1 (any): Primer elemento a comparar. 
        element_2 (any): Segundo elemento a comparar.
    Retorno : 
        bool : True si el primer elemento es menor que el segundo, False en caso contrario.
    """
    return element_1 <= element_2

def selection_sort(my_list, sort_crit=default_sort_criteria):
    """
    Ordena una lista simplemente enlazada usando Selection Sort.
    Usa una función de comparación sort_crit(a,b) -> bool.
    """
    n = size(my_list)
    if n <= 1:
        return my_list
    for i in range(n - 1):
        min_index = i
        # buscamos el elemento más pequeño en el resto de la lista
        for j in range(i + 1, n):
            if sort_crit(get_element(my_list, j), get_element(my_list, min_index)):
                min_index = j
        # si encontramos uno más pequeño, intercambiamos
        if min_index != i:
            exchange(my_list, i, min_index)

    return my_list

def insertion_sort(my_list, cmp_function):
    """
    Ordena una lista simplemente enlazada usando Insertion Sort.
    Usa las funciones ya implementadas en el TAD (get_element, change_info, etc.).
    """
    n = size(my_list)
    for i in range(1, n):
        
        key = get_element(my_list, i)
        j = i - 1

        
        while j >= 0 and not cmp_function(get_element(my_list, j), key):
            change_info(my_list, j + 1, get_element(my_list, j))
            j -= 1

        
        change_info(my_list, j + 1, key)
        
    return my_list


def shell_sort(my_list, sort_crit=default_sort_criteria):
    n = size(my_list)
    gap = n // 2 

    # mientras el gap sea mayor que 0
    while gap > 0:
        for i in range(gap, n):
            temp = get_element(my_list, i)   # guardamos el valor actual
            j = i

            while j >= gap and sort_crit(temp, get_element(my_list, j - gap)):
                change_info(my_list, j, get_element(my_list, j - gap))
                j -= gap

            change_info(my_list, j, temp)
        # reducimos el gap a la mitad
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

def merge_sort(my_list, cmp_function= default_sort_criteria):
    """
    Ordena un array_list usando Merge Sort (recursivo).
    my_list: Single linked List
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
        my_list (dict):  Single linked List
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

def default_function(elemen_1, element_2):

   if elemen_1 > element_2:
      return 1
   elif elemen_1 < element_2:
      return -1
   return 0