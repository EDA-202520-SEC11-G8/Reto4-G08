from DataStructures.List import array_list as lt
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf
import random as rd 

def is_available(table, pos):

   entry = lt.get_element(table, pos)
   if me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
      return True
   return False

def default_compare(key, entry):

   if key == me.get_key(entry):
      return 0
   elif key > me.get_key(entry):
      return 1
   return -1

def find_slot(my_map, key, hash_value):
    first_avail = None
    ocupied = False
    max_iter = my_map["capacity"]
    iter_count = 0

    while iter_count < max_iter:
        entry = lt.get_element(my_map["table"], hash_value)

        # Slot libre
        if entry is None or me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
            if first_avail is None:
                first_avail = hash_value
            # Slot disponible encontrado, se puede usar
            break

        # Slot con la misma clave
        elif key == me.get_key(entry):
            first_avail = hash_value
            ocupied = True
            break

        # Siguiente slot
        hash_value = (hash_value + 1) % my_map["capacity"]
        iter_count += 1

    if first_avail is None:
        print("error: tabla llena, no se encontró slot libre")

    return ocupied, first_avail


def new_map(num_elements, load_factor, prime=109345121):
    capacity = mf.next_prime(num_elements/ load_factor)
    
    new_table = lt.new_list()
    
    for i in range(capacity):
        lt.add_last(
            new_table,
            me.new_map_entry(None, None)
        )

    return {
        "prime":prime,
        "capacity":capacity,
        "scale":rd.randint(1, prime-1),
        "shift":rd.randint(0, prime-1),
        "table":new_table,
        "current_factor":0,
        "limit_factor":load_factor,
        "size":0
    }
   
def rehash(my_map):
    old_table = my_map["table"]
    capacity = my_map["capacity"]
    load_factor = my_map["limit_factor"]

    new_capacity = mf.next_prime(2 * capacity)
    new_map_ = new_map(new_capacity, load_factor, my_map["prime"])

    for i in range(lt.size(old_table)):
        entry = lt.get_element(old_table, i)
        if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            put(new_map_, me.get_key(entry), me.get_value(entry))

    return new_map_


def put(my_map, key, value):
    hash_val = mf.hash_value(my_map, key) % my_map["capacity"]
    ocupied, pos = find_slot(my_map, key, hash_val)

    if ocupied:
        entry = lt.get_element(my_map["table"], pos)
        entry["value"] = value
        lt.change_info(my_map["table"], pos, entry)
    else:
        new_entry = me.new_map_entry(key, value)
        lt.change_info(my_map["table"], pos, new_entry)
        my_map["size"] += 1

    my_map["current_factor"] = my_map["size"] / my_map["capacity"]

    if my_map["current_factor"] > my_map["limit_factor"]:
        new_map_ = rehash(my_map)
        my_map.update(new_map_)

    return my_map

def contains(my_map, key):
    hash_value = mf.hash_value(my_map, key) % my_map["capacity"]
    ocupied, slot = find_slot(my_map, key, hash_value)

    if ocupied:
        entry = lt.get_element(my_map["table"], slot)
        return me.get_key(entry) == key
    
    return False 

def get(my_map, key):
    hash_value = mf.hash_value(my_map, key) % my_map["capacity"]
    ocupied, slot = find_slot(my_map, key, hash_value)
    if ocupied:
        entry = lt.get_element(my_map["table"], slot)
        return me.get_value(entry)   
    return None
def size(my_map):
    return my_map["size"]
def remove(my_map, key):
    hash_value = mf.hash_value(my_map, key) % my_map["capacity"]
    ocupied, pos = find_slot(my_map, key, hash_value)

    if ocupied:
        empty_entry = me.new_map_entry("__EMPTY__", "__EMPTY__")
        lt.change_info(my_map["table"], pos, empty_entry)
        my_map["size"] -= 1
        my_map["current_factor"] = my_map["size"] / my_map["capacity"]

    return my_map
def is_empty(my_map):
    return size(my_map)==0
def key_set(my_map):
    keys = lt.new_list()
    table = my_map["table"]
    for i in range(lt.size(table)):
        entry = lt.get_element(table, i)
        if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            lt.add_last(keys, me.get_key(entry))
    return keys
def value_set(my_map):
    value_set = lt.new_list()
    table = my_map["table"]
    for i in range(lt.size(table)):
        entry = lt.get_element(table, i)
        if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            lt.add_last(value_set, entry["value"])
    return value_set