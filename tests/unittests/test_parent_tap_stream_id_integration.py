"""
Integration tests for parent-tap-stream-id functionality.

These tests verify end-to-end functionality of parent-child relationships
in the tap-ujet discovery and catalog generation process.
"""
import unittest
import json
import tempfile
import os
from unittest.mock import patch, MagicMock

from tap_ujet.discover import discover
from tap_ujet.streams import STREAMS
from singer import metadata


class TestParentTapStreamIdIntegration(unittest.TestCase):
    """Integration tests for parent-tap-stream-id functionality."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.expected_relationships = {
            'agent_activity_logs': 'agents',
            'menu_tree': 'menus', 
            'team_tree': 'teams'
        }
        
        # Sample schema content for each stream
        self.sample_schemas = {
            'agents': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "first_name": {"type": ["null", "string"]},
                    "last_name": {"type": ["null", "string"]},
                    "email": {"type": ["null", "string"]}
                }
            },
            'agent_activity_logs': {
                "type": "object", 
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "agent_id": {"type": ["null", "integer"]},
                    "started_at": {"type": ["null", "string"], "format": "date-time"},
                    "activity": {"type": ["null", "string"]}
                }
            },
            'menus': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "menu_type": {"type": ["null", "string"]}
                }
            },
            'menu_tree': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "parent_id": {"type": ["null", "integer"]},
                    "position": {"type": ["null", "integer"]}
                }
            },
            'teams': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "agents_count": {"type": ["null", "integer"]}
                }
            },
            'team_tree': {
                "type": "object", 
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "parent_id": {"type": ["null", "integer"]},
                    "agents_count": {"type": ["null", "integer"]}
                }
            },
            'calls': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "call_type": {"type": ["null", "string"]},
                    "updated_at": {"type": ["null", "string"], "format": "date-time"}
                }
            },
            'chats': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "chat_duration": {"type": ["null", "integer"]},
                    "updated_at": {"type": ["null", "string"], "format": "date-time"}
                }
            },
            'user_statuses': {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "color": {"type": ["null", "string"]}
                }
            }
        }

    @patch('tap_ujet.schema.get_abs_path')
    @patch('builtins.open')
    def test_full_catalog_generation_with_parent_metadata(self, mock_open, mock_get_abs_path):
        """Test complete catalog generation includes parent-tap-stream-id metadata."""
        
        def mock_open_side_effect(file_path, *args, **kwargs):
            # Extract stream name from file path
            filename = os.path.basename(file_path)
            stream_name = filename.replace('.json', '')
            
            mock_file = MagicMock()
            mock_file.__enter__.return_value = mock_file
            
            if stream_name in self.sample_schemas:
                mock_file.read.return_value = json.dumps(self.sample_schemas[stream_name])
            else:
                # Fallback for unknown streams
                mock_file.read.return_value = json.dumps({
                    "type": "object",
                    "properties": {"id": {"type": ["null", "integer"]}}
                })
            
            return mock_file
        
        mock_open.side_effect = mock_open_side_effect
        mock_get_abs_path.side_effect = lambda x: f"schemas/{x}"
        
        # Generate catalog
        catalog = discover()
        
        # Verify catalog structure
        self.assertIsNotNone(catalog)
        self.assertTrue(len(catalog.streams) > 0)
        
        # Create lookup for easier testing
        streams_by_id = {stream.tap_stream_id: stream for stream in catalog.streams}
        
        # Test all expected parent relationships
        for child_stream, expected_parent in self.expected_relationships.items():
            with self.subTest(child_stream=child_stream, expected_parent=expected_parent):
                # Verify child stream exists in catalog
                self.assertIn(child_stream, streams_by_id,
                             f"Child stream '{child_stream}' missing from catalog")
                
                child_entry = streams_by_id[child_stream]
                
                # Verify parent stream exists in catalog
                self.assertIn(expected_parent, streams_by_id,
                             f"Parent stream '{expected_parent}' missing from catalog")
                
                # Check metadata for parent-tap-stream-id
                mdata_map = metadata.to_map(child_entry.metadata)
                root_metadata = mdata_map.get((), {})
                
                self.assertIn('parent-tap-stream-id', root_metadata,
                             f"parent-tap-stream-id missing from '{child_stream}' metadata")
                
                actual_parent = root_metadata['parent-tap-stream-id']
                self.assertEqual(actual_parent, expected_parent,
                               f"Incorrect parent-tap-stream-id for '{child_stream}': "
                               f"expected '{expected_parent}', got '{actual_parent}'")
        
        # Test that independent streams do NOT have parent-tap-stream-id
        independent_streams = ['agents', 'calls', 'chats', 'menus', 'teams', 'user_statuses']
        for stream_name in independent_streams:
            if stream_name in streams_by_id:  # Only test if stream exists
                with self.subTest(stream_name=stream_name):
                    stream_entry = streams_by_id[stream_name]
                    mdata_map = metadata.to_map(stream_entry.metadata)
                    root_metadata = mdata_map.get((), {})
                    
                    self.assertNotIn('parent-tap-stream-id', root_metadata,
                                   f"Independent stream '{stream_name}' should not have "
                                   f"parent-tap-stream-id metadata")

    def test_catalog_serialization_with_parent_metadata(self):
        """Test that catalog with parent-tap-stream-id can be properly serialized."""
        with patch('tap_ujet.schema.get_abs_path') as mock_path, \
             patch('builtins.open') as mock_open:
            
            def mock_open_side_effect(file_path, *args, **kwargs):
                filename = os.path.basename(file_path)
                stream_name = filename.replace('.json', '')
                
                mock_file = MagicMock()
                mock_file.__enter__.return_value = mock_file
                mock_file.read.return_value = json.dumps(
                    self.sample_schemas.get(stream_name, {
                        "type": "object", 
                        "properties": {"id": {"type": ["null", "integer"]}}
                    })
                )
                return mock_file
            
            mock_open.side_effect = mock_open_side_effect
            mock_path.side_effect = lambda x: f"schemas/{x}"
            
            # Generate catalog
            catalog = discover()
            
            # Test serialization to JSON
            try:
                catalog_dict = catalog.to_dict()
                catalog_json = json.dumps(catalog_dict, indent=2)
                
                # Verify it's valid JSON
                parsed_catalog = json.loads(catalog_json)
                self.assertIn('streams', parsed_catalog)
                
                # Verify parent-tap-stream-id appears in serialized metadata
                for stream_data in parsed_catalog['streams']:
                    stream_id = stream_data['tap_stream_id']
                    if stream_id in self.expected_relationships:
                        expected_parent = self.expected_relationships[stream_id]
                        
                        # Find root metadata entry
                        root_metadata = None
                        for metadata_entry in stream_data['metadata']:
                            if metadata_entry['breadcrumb'] == []:
                                root_metadata = metadata_entry['metadata']
                                break
                        
                        self.assertIsNotNone(root_metadata,
                                           f"Root metadata missing for '{stream_id}'")
                        self.assertIn('parent-tap-stream-id', root_metadata,
                                     f"parent-tap-stream-id missing from serialized metadata "
                                     f"for '{stream_id}'")
                        self.assertEqual(root_metadata['parent-tap-stream-id'], expected_parent,
                                       f"Incorrect parent-tap-stream-id in serialized catalog "
                                       f"for '{stream_id}'")
                
            except (TypeError, ValueError) as e:
                self.fail(f"Catalog serialization failed: {e}")


class TestParentTapStreamIdEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for parent-tap-stream-id."""
    
    def test_nonexistent_parent_reference(self):
        """Test behavior when parent stream doesn't exist in STREAMS."""
        # This test ensures our current implementation is robust
        # We expect that if someone adds a parent reference to a non-existent stream,
        # it should not break the tap
        
        test_streams = STREAMS.copy()
        test_streams['test_child'] = {
            'key_properties': ['id'],
            'replication_method': 'FULL_TABLE',
            'parent': 'nonexistent_parent'  # This parent doesn't exist
        }
        
        # The get_schemas function should handle this gracefully
        # In the current implementation, it will still add the parent-tap-stream-id
        # even if the parent stream doesn't exist in STREAMS
        with patch('tap_ujet.streams.STREAMS', test_streams):
            with patch('tap_ujet.schema.get_abs_path') as mock_path, \
                 patch('builtins.open') as mock_open:
                
                mock_file = MagicMock()
                mock_file.__enter__.return_value = mock_file  
                mock_file.read.return_value = json.dumps({
                    "type": "object",
                    "properties": {"id": {"type": ["null", "integer"]}}
                })
                mock_open.return_value = mock_file
                mock_path.side_effect = lambda x: f"schemas/{x}"
                
                # This should not raise an exception
                try:
                    from tap_ujet.schema import get_schemas
                    schemas, field_metadata = get_schemas()
                    
                    # Verify that parent-tap-stream-id is still added
                    if 'test_child' in field_metadata:
                        mdata_map = metadata.to_map(field_metadata['test_child'])
                        root_metadata = mdata_map.get((), {})
                        self.assertEqual(root_metadata.get('parent-tap-stream-id'), 
                                       'nonexistent_parent')
                        
                except Exception as e:
                    self.fail(f"get_schemas should handle nonexistent parent gracefully: {e}")

    def test_circular_parent_reference(self):
        """Test that circular parent references don't cause issues."""
        # This is more of a documentation test - our current implementation
        # doesn't prevent circular references, but they shouldn't break anything
        # since we only use parent info for metadata, not for processing logic
        
        test_streams = {
            'stream_a': {
                'key_properties': ['id'],
                'replication_method': 'FULL_TABLE',
                'parent': 'stream_b'
            },
            'stream_b': {
                'key_properties': ['id'], 
                'replication_method': 'FULL_TABLE',
                'parent': 'stream_a'
            }
        }
        
        with patch('tap_ujet.streams.STREAMS', test_streams):
            with patch('tap_ujet.schema.get_abs_path') as mock_path, \
                 patch('builtins.open') as mock_open:
                
                mock_file = MagicMock()
                mock_file.__enter__.return_value = mock_file
                mock_file.read.return_value = json.dumps({
                    "type": "object",
                    "properties": {"id": {"type": ["null", "integer"]}}
                })
                mock_open.return_value = mock_file
                mock_path.side_effect = lambda x: f"schemas/{x}"
                
                # This should not cause infinite loops or exceptions
                try:
                    from tap_ujet.schema import get_schemas
                    schemas, field_metadata = get_schemas()
                    
                    # Both should have their respective parent-tap-stream-id values
                    for stream_name, expected_parent in [('stream_a', 'stream_b'), ('stream_b', 'stream_a')]:
                        if stream_name in field_metadata:
                            mdata_map = metadata.to_map(field_metadata[stream_name])
                            root_metadata = mdata_map.get((), {})
                            self.assertEqual(root_metadata.get('parent-tap-stream-id'), 
                                           expected_parent)
                            
                except Exception as e:
                    self.fail(f"Circular parent references should not break get_schemas: {e}")


if __name__ == '__main__':
    unittest.main()