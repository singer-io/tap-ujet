#!/usr/bin/env python3
"""
Simple test runner for parent-tap-stream-id functionality.
Run this from the tap-ujet root directory.
"""
import sys
import os
import unittest

# Add the current directory to Python path so we can import tap_ujet
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Now we can import our tap modules
from tap_ujet.streams import STREAMS
from tap_ujet.discover import discover
from tap_ujet.schema import get_schemas
from singer import metadata


class TestMinimalParentTapStreamId(unittest.TestCase):
    """Minimal test cases for parent-tap-stream-id functionality."""
    
    def test_agent_activity_logs_has_agents_parent(self):
        """Test: agent_activity_logs should have agents as parent."""
        self.assertEqual(STREAMS['agent_activity_logs'].get('parent'), 'agents')
        print("✓ agent_activity_logs has correct parent: agents")
    
    def test_menu_tree_has_menus_parent(self):
        """Test: menu_tree should have menus as parent."""
        self.assertEqual(STREAMS['menu_tree'].get('parent'), 'menus')
        print("✓ menu_tree has correct parent: menus")
    
    def test_team_tree_has_teams_parent(self):
        """Test: team_tree should have teams as parent."""
        self.assertEqual(STREAMS['team_tree'].get('parent'), 'teams')
        print("✓ team_tree has correct parent: teams")
    
    def test_parent_streams_exist(self):
        """Test: All referenced parent streams exist in configuration."""
        parent_streams = {'agents', 'menus', 'teams'}
        for parent_stream in parent_streams:
            self.assertIn(parent_stream, STREAMS, 
                         f"Parent stream '{parent_stream}' missing from STREAMS")
        print("✓ All parent streams exist in STREAMS configuration")
    
    def test_independent_streams_have_no_parent(self):
        """Test: Independent streams should not have parent fields."""
        independent_streams = ['agents', 'calls', 'chats', 'menus', 'teams', 'user_statuses']
        for stream_name in independent_streams:
            stream_config = STREAMS[stream_name]
            self.assertNotIn('parent', stream_config,
                           f"Independent stream '{stream_name}' should not have parent field")
        print("✓ Independent streams have no parent fields")


class TestParentTapStreamIdMetadata(unittest.TestCase):
    """Test parent-tap-stream-id metadata generation."""
    
    def test_catalog_generation_includes_parent_metadata(self):
        """Test: discover() should include parent-tap-stream-id in catalog."""
        try:
            catalog = discover()
            streams_by_id = {stream.tap_stream_id: stream for stream in catalog.streams}
            
            expected_relationships = {
                'agent_activity_logs': 'agents',
                'menu_tree': 'menus',
                'team_tree': 'teams'
            }
            
            for child_stream, expected_parent in expected_relationships.items():
                if child_stream in streams_by_id:
                    child_entry = streams_by_id[child_stream]
                    mdata_map = metadata.to_map(child_entry.metadata)
                    root_metadata = mdata_map.get((), {})
                    
                    self.assertIn('parent-tap-stream-id', root_metadata,
                                 f"parent-tap-stream-id missing from '{child_stream}' metadata")
                    
                    actual_parent = root_metadata['parent-tap-stream-id'] 
                    self.assertEqual(actual_parent, expected_parent,
                                   f"Expected parent '{expected_parent}' for '{child_stream}', "
                                   f"got '{actual_parent}'")
                    
                    print(f"✓ {child_stream} has parent-tap-stream-id: {actual_parent}")
                else:
                    print(f"⚠ {child_stream} not found in catalog (schema file might be missing)")
                    
        except Exception as e:
            self.fail(f"Catalog generation failed: {e}")


def run_tests():
    """Run the minimal parent-tap-stream-id tests."""
    print("🧪 Running Parent-Tap-Stream-ID Tests")
    print("=" * 50)
    
    # Create test suite
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestMinimalParentTapStreamId))
    suite.addTest(unittest.makeSuite(TestParentTapStreamIdMetadata))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=1, stream=sys.stdout)
    result = runner.run(suite)
    
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("✅ All parent-tap-stream-id tests passed!")
        return True
    else:
        print("❌ Some tests failed!")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
        return False


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)