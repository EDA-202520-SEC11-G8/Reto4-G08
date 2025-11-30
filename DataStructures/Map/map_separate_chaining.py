from DataStructures.List import array_list as lt
from DataStructures.List import single_linked_list as sl
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf
import random as rd 

def default_compare(key, element):

   if (key == me.get_key(element)):
      return 0
   elif (key > me.get_key(element)):
      return 1
   return -1

def new_map(num_elements, load_factor, prime=109345121):
    capacity = mf.next_prime(num_elements/ load_factor)
    
    new_table = lt.new_list()
    
    for i in range(capacity):
        lt.add_last(
            new_table,
            sl.new_list()
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
    old_capacity = my_map["capacity"]

    new_capacity = old_capacity * 2
    my_map["capacity"] = new_capacity
    my_map["table"] = lt.new_list()
    my_map["size"] = 0

    for i in range(new_capacity):
        lt.add_last(my_map["table"], sl.new_list())

    for i in range(old_capacity):
        bucket = lt.get_element(old_table, i)
        for j in range(sl.size(bucket)):
            entry = sl.get_element(bucket, j)
            put(my_map, entry["key"], entry["value"])

    return my_map
 
def put(my_map, key, value):
    hash_val = mf.hash_value(my_map, key) % my_map["capacity"]
    new_entry = me.new_map_entry(key, value)
    bucket = lt.get_element(my_map["table"], hash_val)

    for i in range(sl.size(bucket)):
        entry = sl.get_element(bucket, i)
        if default_compare(key, entry) == 0:
            entry["value"] = value
            sl.change_info(bucket, i, entry)
            return my_map  

    sl.add_last(bucket, new_entry)
    my_map["size"] += 1
    my_map["current_factor"] = my_map["size"] / my_map["capacity"]

    if my_map["current_factor"] > my_map["limit_factor"]:
        return rehash(my_map)

    return my_map

def contains(my_map, key):
   hash_value = mf.hash_value(my_map, key) % my_map["capacity"]
   bucket = lt.get_element(my_map["table"], hash_value)
   for i in range(sl.size(bucket)):
      entry = sl.get_element(bucket, i)
      if default_compare(key, entry) == 0:
         return True
   return False

def get(my_map, key):
   hash_value = mf.hash_value(my_map, key) % my_map["capacity"]
   bucket = lt.get_element(my_map["table"], hash_value)
   for i in range(sl.size(bucket)):
      entry = sl.get_element(bucket, i)
      if default_compare(key, entry) == 0:
         return me.get_value(entry)
   return None

def remove(my_map, key):
    hash_value = mf.hash_value(my_map, key) % my_map["capacity"]
    bucket = lt.get_element(my_map["table"], hash_value)
    for i in range(sl.size(bucket)):
        entry = sl.get_element(bucket, i)
        if default_compare(key, entry) == 0:
            sl.delete_element(bucket, i)
            my_map["size"] -= 1
            my_map["current_factor"] = my_map["size"] / my_map["capacity"]
            return my_map  
    return my_map


def size(my_map):
    return my_map["size"]
 
def is_empty(my_map):
   return size(my_map)==0

def key_set(my_map):
   keys = lt.new_list()
   table = my_map["table"]

   for i in range(lt.size(table)):              
      bucket = lt.get_element(table, i)        
      for j in range(sl.size(bucket)):         
         entry = sl.get_element(bucket, j)
         if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            lt.add_last(keys, me.get_key(entry))
   return keys


def value_set(my_map):
   values = lt.new_list()
   table = my_map["table"]

   for i in range(lt.size(table)):             
      bucket = lt.get_element(table, i)        
      for j in range(sl.size(bucket)):         
         entry = sl.get_element(bucket, j)
         if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            lt.add_last(values, me.get_value(entry))
   return values