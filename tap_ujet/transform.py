def flatten_children(tree, nodes):
    """
    Flatten all children nodes in all descendents.

    :type tree: list(dict)
    :type nodes: list(dict)
    """

    record = dict(tree)
    record.pop('children', None)
    nodes.append(record)
    if isinstance(tree.get('children'), list):
        children = tree.get('children')
        for child in children:
            if isinstance(child.get('children'), list):
                flatten_children(child, nodes=nodes)
            else:
                record = dict(child)
                record.pop('children', None)
                nodes.append(record)


def transform_recursive_tree(this_json):
    """Flatten all in list"""

    nodes = []
    for record in list(this_json):
        flatten_children(record, nodes=nodes)
    return nodes


def transform_json(this_json, stream_name):
    if stream_name in ('team_tree', 'menu_tree'):
        denested_json = transform_recursive_tree(this_json)
    else:
        denested_json = this_json

    return denested_json
