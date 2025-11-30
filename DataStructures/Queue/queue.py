def new_queue():
    return {"size": 0, "elements": []}
    
    
def enqueue(my_queue,
            element:any):
    """Agregar un elemento al final de la cola

    Args:
        my_queue (dict): La cola a la que se le agregará el elemento.
        element (any): El elemento que se agregará a la cola

    Returns:
        dict: La cola con el elemento agregado.
    """
    my_queue["elements"].append(element)
    my_queue["size"] += 1
    return my_queue

def dequeue(my_queue):
    """
    Elimina y retorna el primer elemento de la cola.
    Si la cola está vacía, lanza un error: EmptyStructureError: queue is empty.

    Args:
        my_queue (dict): La cola de la que se eliminará el primer elemento.

    Returns:
        any: Elemento retirado de la cola.
    """
    if is_empty(my_queue):
        raise Exception("EmptyStructureError: queue is empty")
    element = my_queue["elements"].pop(0)
    my_queue["size"] -= 1
    return element

def peek(my_queue):
    """
    Retorna el primer elemento de la cola sin eliminarlo.

    Si la cola está vacía, lanza un error: EmptyStructureError: queue is empty.

    Args:
        my_queue (dict): La cola de la que se retornará el primer elemento.

    Returns:
        any: Primer elemento de la cola.
    """
    if is_empty(my_queue):
        raise Exception("EmptyStructureError: queue is empty")
    return my_queue["elements"][0]

def is_empty(my_queue)->bool:
    """Verifica si la cola está vacía.

    Args:
        my_queue (dict): La cola que se verificará si está vacía.

    Returns:
        bool: True si la cola está vacía, de lo contrario False.
    """
    if my_queue['size']==0:
        return True
    
    return False

def size(my_queue)->int:
    """Retorna el tamaño de la cola.

    Args:
        my_queue (dict): La cola de la que se retornará el tamaño.

    Returns:
        int: Tamaño de la cola.
    """
    
    return my_queue['size']

