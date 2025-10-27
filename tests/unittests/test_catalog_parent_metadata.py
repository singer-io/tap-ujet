import unittest
from unittest.mock import patch
from tap_ujet.discover import discover
from tap_ujet.streams import STREAMS
from singer import metadata


class TestCatalogParentMetadata(unittest.TestCase):
    """Test cases for verifying parent-tap-stream-id inclusion in catalog metadata."""

    def setUp(self):
        """Set up test fixtures."""
        self.catalog = discover()
        
    def test_streams_with_parent_have_parent_tap_stream_id_in_metadata(self):
        """Test that streams with 'parent' in STREAMS config have 'parent-tap-stream-id' in catalog metadata."""
        # Get streams that should have parent metadata
        streams_with_parents = {
            stream_name: stream_config.get('parent')
            for stream_name, stream_config in STREAMS.items()
            if 'parent' in stream_config
        }
        
        # Verify we have streams with parents to test
        self.assertGreater(len(streams_with_parents), 0, 
                          "No streams with parent configuration found. Test setup issue.")
        
        for stream_name, expected_parent in streams_with_parents.items():
            with self.subTest(stream=stream_name):
                # Find the stream in the catalog
                catalog_stream = None
                for stream in self.catalog.streams:
                    if stream.tap_stream_id == stream_name:
                        catalog_stream = stream
                        break
                
                self.assertIsNotNone(catalog_stream, 
                                   f"Stream '{stream_name}' not found in catalog")
                
                # Convert metadata to map for easier access
                mdata_map = metadata.to_map(catalog_stream.metadata)
                
                # Check if parent-tap-stream-id exists in root metadata
                self.assertIn((), mdata_map, 
                             f"Root metadata missing for stream '{stream_name}'")
                
                root_metadata = mdata_map[()]
                self.assertIn('parent-tap-stream-id', root_metadata,
                             f"'parent-tap-stream-id' missing in metadata for stream '{stream_name}'")
                
                # Verify the parent-tap-stream-id value matches expected parent
                actual_parent = root_metadata['parent-tap-stream-id']
                self.assertEqual(actual_parent, expected_parent,
                               f"Expected parent '{expected_parent}' but got '{actual_parent}' for stream '{stream_name}'")
    
    def test_streams_without_parent_do_not_have_parent_tap_stream_id_in_metadata(self):
        """Test that streams without 'parent' in STREAMS config do not have 'parent-tap-stream-id' in catalog metadata."""
        # Get streams that should NOT have parent metadata
        streams_without_parents = [
            stream_name for stream_name, stream_config in STREAMS.items()
            if 'parent' not in stream_config
        ]
        
        # Verify we have streams without parents to test
        self.assertGreater(len(streams_without_parents), 0,
                          "No streams without parent configuration found. Test setup issue.")
        
        for stream_name in streams_without_parents:
            with self.subTest(stream=stream_name):
                # Find the stream in the catalog
                catalog_stream = None
                for stream in self.catalog.streams:
                    if stream.tap_stream_id == stream_name:
                        catalog_stream = stream
                        break
                
                self.assertIsNotNone(catalog_stream,
                                   f"Stream '{stream_name}' not found in catalog")
                
                # Convert metadata to map for easier access
                mdata_map = metadata.to_map(catalog_stream.metadata)
                
                # Check if root metadata exists
                if () in mdata_map:
                    root_metadata = mdata_map[()]
                    # If root metadata exists, parent-tap-stream-id should NOT be present
                    self.assertNotIn('parent-tap-stream-id', root_metadata,
                                   f"'parent-tap-stream-id' should not be present in metadata for stream '{stream_name}' without parent configuration")
