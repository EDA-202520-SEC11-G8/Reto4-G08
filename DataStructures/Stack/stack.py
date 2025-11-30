import DataStructures.List.single_linked_list as lt

def new_stack():
    stack = lt.new_list()
    return stack


def push(my_stack, element):
    new_node = lt.new_node(element)
    if my_stack['first'] is None:
        my_stack['first'] = new_node
        my_stack['last'] = new_node
    else:
        new_node['next'] = my_stack['first']
        my_stack['first'] = new_node
    my_stack['size'] += 1
    return my_stack

def pop(my_stack):
    if my_stack["size"] == 0:
        print("Stack vacío")
        return None
    value = my_stack["first"]["info"]
    my_stack["first"] = my_stack["first"]["next"]
    my_stack["size"] -= 1
    return value


def peek(my_stack):
    if my_stack['size'] > 0:
        return my_stack['first']['info'] 
    return None

def is_empty(my_stack):
    return my_stack['size'] == 0

def size(my_stack):
    return my_stack['size']
def top(my_stack):
    if my_stack['size'] == 0:
        return None
    return my_stack['first']['info']