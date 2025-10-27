"""
Test cases for parent-tap-stream-id functionality in tap-ujet.

This module tests the parent-child relationship metadata generation
for streams that have defined parent relationships.
"""
import unittest
from unittest.mock import patch, MagicMock
import json
import os

from tap_ujet.discover import discover
from tap_ujet.schema import get_schemas
from tap_ujet.streams import STREAMS
from singer.catalog import Catalog
from singer import metadata


class TestParentTapStreamId(unittest.TestCase):
    """Test parent-tap-stream-id metadata functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Define expected parent-child relationships
        self.expected_parent_relationships = {
            'agent_activity_logs': 'agents',
            'menu_tree': 'menus',
            'team_tree': 'teams'
        }
        
        # Define streams that should NOT have parents
        self.independent_streams = [
            'agents', 'calls', 'chats', 'menus', 'teams', 'user_statuses'
        ]

    @patch('tap_ujet.schema.get_abs_path')
    @patch('builtins.open')
    def test_schema_metadata_generation(self, mock_open, mock_get_abs_path):
        """Test that get_schemas() correctly generates parent-tap-stream-id metadata."""
        # Mock schema files
        mock_schemas = {
            'agents': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'agent_activity_logs': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'menus': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'menu_tree': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'teams': {'type': 'object', 'properties': {'id': {'type': 'integer'}}},
            'team_tree': {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
        }
        
        def mock_open_side_effect(file_path, *args, **kwargs):
            # Extract stream name from file path
            stream_name = os.path.basename(file_path).replace('.json', '')
            mock_file = MagicMock()
            mock_file.__enter__.return_value = mock_file
            mock_file.read.return_value = json.dumps(mock_schemas.get(stream_name, {}))
            return mock_file
        
        mock_open.side_effect = mock_open_side_effect
        mock_get_abs_path.side_effect = lambda x: f"mocked_path/{x}"
        
        # Call get_schemas
        schemas, field_metadata = get_schemas()
        
        # Test that child streams have parent-tap-stream-id in metadata
        for child_stream, expected_parent in self.expected_parent_relationships.items():
            with self.subTest(child_stream=child_stream):
                self.assertIn(child_stream, field_metadata,
                             f"Field metadata missing for '{child_stream}'")
                
                mdata_list = field_metadata[child_stream]
                mdata_map = metadata.to_map(mdata_list)
                
                # Check for parent-tap-stream-id in root metadata
                root_metadata = mdata_map.get((), {})
                self.assertIn('parent-tap-stream-id', root_metadata,
                             f"parent-tap-stream-id missing from '{child_stream}' metadata")
                
                actual_parent = root_metadata['parent-tap-stream-id']
                self.assertEqual(actual_parent, expected_parent,
                               f"Expected parent-tap-stream-id '{expected_parent}' for '{child_stream}', "
                               f"got '{actual_parent}'")

    @patch('tap_ujet.schema.get_abs_path')
    @patch('builtins.open')
    def test_independent_streams_no_parent_metadata(self, mock_open, mock_get_abs_path):
        """Test that independent streams do not have parent-tap-stream-id metadata."""
        # Mock schema files
        mock_schemas = {
            stream: {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
            for stream in self.independent_streams
        }
        
        def mock_open_side_effect(file_path, *args, **kwargs):
            stream_name = os.path.basename(file_path).replace('.json', '')
            mock_file = MagicMock()
            mock_file.__enter__.return_value = mock_file
            mock_file.read.return_value = json.dumps(mock_schemas.get(stream_name, {}))
            return mock_file
        
        mock_open.side_effect = mock_open_side_effect
        mock_get_abs_path.side_effect = lambda x: f"mocked_path/{x}"
        
        # Call get_schemas
        schemas, field_metadata = get_schemas()
        
        # Test that independent streams do NOT have parent-tap-stream-id
        for stream_name in self.independent_streams:
            with self.subTest(stream_name=stream_name):
                if stream_name in field_metadata:  # Only test if stream exists in test data
                    mdata_list = field_metadata[stream_name]
                    mdata_map = metadata.to_map(mdata_list)
                    
                    root_metadata = mdata_map.get((), {})
                    self.assertNotIn('parent-tap-stream-id', root_metadata,
                                   f"Independent stream '{stream_name}' should not have "
                                   f"parent-tap-stream-id metadata")


class TestDiscoverWithParentMetadata(unittest.TestCase):
    """Test discover function with parent-tap-stream-id metadata."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.expected_parent_relationships = {
            'agent_activity_logs': 'agents',
            'menu_tree': 'menus',
            'team_tree': 'teams'
        }

    @patch('tap_ujet.discover.get_schemas')
    def test_discover_includes_parent_metadata_in_catalog(self, mock_get_schemas):
        """Test that discover() includes parent-tap-stream-id in catalog entries."""
        # Mock schemas and metadata
        mock_schemas = {}
        mock_field_metadata = {}
        
        for stream_name in STREAMS.keys():
            mock_schemas[stream_name] = {
                'type': 'object',
                'properties': {'id': {'type': 'integer'}}
            }
            
            # Create base metadata
            mdata = metadata.new()
            mdata = metadata.get_standard_metadata(
                schema=mock_schemas[stream_name],
                key_properties=STREAMS[stream_name].get('key_properties', []),
                valid_replication_keys=STREAMS[stream_name].get('replication_keys'),
                replication_method=STREAMS[stream_name].get('replication_method')
            )
            
            # Add parent-tap-stream-id for child streams
            if stream_name in self.expected_parent_relationships:
                mdata_map = metadata.to_map(mdata)
                parent_stream = self.expected_parent_relationships[stream_name]
                mdata_map = metadata.write(mdata_map, (), 'parent-tap-stream-id', parent_stream)
                mdata = metadata.to_list(mdata_map)
            
            mock_field_metadata[stream_name] = mdata
        
        mock_get_schemas.return_value = (mock_schemas, mock_field_metadata)
        
        # Call discover
        catalog = discover()
        
        # Verify catalog structure
        self.assertIsInstance(catalog, Catalog)
        self.assertTrue(len(catalog.streams) > 0)
        
        # Test parent-tap-stream-id in catalog entries
        catalog_streams = {stream.tap_stream_id: stream for stream in catalog.streams}
        
        for child_stream, expected_parent in self.expected_parent_relationships.items():
            with self.subTest(child_stream=child_stream):
                self.assertIn(child_stream, catalog_streams,
                             f"Child stream '{child_stream}' not found in catalog")
                
                stream_entry = catalog_streams[child_stream]
                mdata_map = metadata.to_map(stream_entry.metadata)
                
                root_metadata = mdata_map.get((), {})
                self.assertIn('parent-tap-stream-id', root_metadata,
                             f"parent-tap-stream-id missing from catalog entry for '{child_stream}'")
                
                actual_parent = root_metadata['parent-tap-stream-id']
                self.assertEqual(actual_parent, expected_parent,
                               f"Expected parent-tap-stream-id '{expected_parent}' in catalog "
                               f"for '{child_stream}', got '{actual_parent}'")


class TestMinimalParentTapStreamId(unittest.TestCase):
    """Minimal test cases for parent-tap-stream-id functionality."""
    
    def test_agent_activity_logs_has_agents_parent(self):
        """Minimal test: agent_activity_logs should have agents as parent."""
        self.assertEqual(STREAMS['agent_activity_logs'].get('parent'), 'agents')
    
    def test_menu_tree_has_menus_parent(self):
        """Minimal test: menu_tree should have menus as parent."""
        self.assertEqual(STREAMS['menu_tree'].get('parent'), 'menus')
    
    def test_team_tree_has_teams_parent(self):
        """Minimal test: team_tree should have teams as parent."""
        self.assertEqual(STREAMS['team_tree'].get('parent'), 'teams')
    
    def test_parent_streams_exist(self):
        """Minimal test: All referenced parent streams exist in configuration."""
        parent_streams = {'agents', 'menus', 'teams'}
        for parent_stream in parent_streams:
            self.assertIn(parent_stream, STREAMS, 
                         f"Parent stream '{parent_stream}' missing from STREAMS")
    
    def test_child_streams_have_correct_replication_method(self):
        """Minimal test: Child streams have appropriate replication methods."""
        # agent_activity_logs should be INCREMENTAL (has replication_keys)
        self.assertEqual(STREAMS['agent_activity_logs']['replication_method'], 'INCREMENTAL')
        
        # menu_tree and team_tree should be FULL_TABLE (hierarchical data)
        self.assertEqual(STREAMS['menu_tree']['replication_method'], 'FULL_TABLE')
        self.assertEqual(STREAMS['team_tree']['replication_method'], 'FULL_TABLE')


if __name__ == '__main__':
    unittest.main()