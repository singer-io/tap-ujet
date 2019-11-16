def flatten_children(tree, nodes=list()):
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

def transform_team_tree(this_json, data_key):
    nodes = []
    for record in list(this_json):
        flatten_children(record, nodes=nodes)
    return nodes

# Run other transforms, as needed: flatten trees
def transform_json(this_json, stream_name, data_key):
    # ADD TRANSFORMATIONS
    if stream_name in ('team_tree', 'menu_tree'):
        denested_json = transform_team_tree(this_json, data_key)
    else:
        denested_json = this_json

    return denested_json
