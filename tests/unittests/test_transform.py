import unittest

from tap_ujet.transform import flatten_children, transform_recursive_tree, transform_json


class TestFlattenChildren(unittest.TestCase):
    """flatten_children recursively flattens a tree-structured record."""

    def test_leaf_node_is_appended_without_children_key(self):
        """A node with no children is appended as-is (minus 'children' key)."""
        node = {'id': 1, 'name': 'Root'}
        nodes = []
        flatten_children(node, nodes)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]['id'], 1)
        self.assertNotIn('children', nodes[0])

    def test_children_key_removed_from_root(self):
        """'children' key is stripped from the root node in the output."""
        node = {'id': 1, 'name': 'Root', 'children': [{'id': 2, 'name': 'Child'}]}
        nodes = []
        flatten_children(node, nodes)
        self.assertNotIn('children', nodes[0])

    def test_single_child_appended(self):
        """Root plus one child → two flat records."""
        node = {
            'id': 1,
            'name': 'Root',
            'children': [{'id': 2, 'name': 'Child'}],
        }
        nodes = []
        flatten_children(node, nodes)
        ids = [n['id'] for n in nodes]
        self.assertIn(1, ids)
        self.assertIn(2, ids)

    def test_nested_children_all_appended(self):
        """Deep nesting is fully flattened."""
        node = {
            'id': 1,
            'name': 'Root',
            'children': [
                {
                    'id': 2,
                    'name': 'Child',
                    'children': [{'id': 3, 'name': 'Grandchild'}],
                }
            ],
        }
        nodes = []
        flatten_children(node, nodes)
        ids = [n['id'] for n in nodes]
        self.assertIn(1, ids)
        self.assertIn(2, ids)
        self.assertIn(3, ids)

    def test_multiple_children_each_appended(self):
        """All siblings at the same level are appended."""
        node = {
            'id': 1,
            'name': 'Root',
            'children': [
                {'id': 2, 'name': 'Child A'},
                {'id': 3, 'name': 'Child B'},
            ],
        }
        nodes = []
        flatten_children(node, nodes)
        self.assertEqual(len(nodes), 3)


class TestTransformRecursiveTree(unittest.TestCase):
    """transform_recursive_tree flattens a list of tree-structured records."""

    def test_empty_list_returns_empty(self):
        self.assertEqual(transform_recursive_tree([]), [])

    def test_flat_list_returns_same_records(self):
        data = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
        result = transform_recursive_tree(data)
        self.assertEqual(len(result), 2)

    def test_nested_tree_is_fully_flattened(self):
        data = [
            {
                'id': 1,
                'name': 'Root',
                'children': [
                    {'id': 2, 'name': 'Child'},
                ]
            }
        ]
        result = transform_recursive_tree(data)
        ids = [r['id'] for r in result]
        self.assertIn(1, ids)
        self.assertIn(2, ids)


class TestTransformJson(unittest.TestCase):
    """transform_json routes data through the correct transform strategy."""

    def test_team_tree_is_flattened(self):
        """team_tree stream uses recursive tree flattening."""
        data = [
            {
                'id': 10,
                'name': 'Engineering',
                'children': [{'id': 11, 'name': 'Backend'}],
            }
        ]
        result = transform_json(data, 'team_tree')
        ids = [r['id'] for r in result]
        self.assertIn(10, ids)
        self.assertIn(11, ids)

    def test_menu_tree_is_flattened(self):
        """menu_tree stream uses recursive tree flattening."""
        data = [
            {
                'id': 20,
                'name': 'Main Menu',
                'children': [{'id': 21, 'name': 'Sub Menu'}],
            }
        ]
        result = transform_json(data, 'menu_tree')
        ids = [r['id'] for r in result]
        self.assertIn(20, ids)
        self.assertIn(21, ids)

    def test_regular_stream_returned_unchanged(self):
        """Non-tree streams are returned as-is."""
        data = [{'id': 1, 'name': 'Agent A'}, {'id': 2, 'name': 'Agent B'}]
        result = transform_json(data, 'agents')
        self.assertEqual(result, data)

    def test_calls_stream_returned_unchanged(self):
        """calls stream data is returned without tree flattening."""
        data = [{'id': 99, 'status': 'completed'}]
        result = transform_json(data, 'calls')
        self.assertEqual(result, data)

    def test_teams_regular_stream_not_flattened(self):
        """teams stream (not team_tree) is returned without modification."""
        data = [{'id': 5, 'name': 'Support', 'children': [{'id': 6}]}]
        result = transform_json(data, 'teams')
        # Should not flatten - returned as the original list
        self.assertEqual(result, data)
