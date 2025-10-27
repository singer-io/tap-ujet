#!/usr/bin/env python3
"""
Quick validation script for parent-tap-stream-id implementation.
This script can be run to verify the feature is working correctly.
"""
import json
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tap_ujet.discover import discover
from tap_ujet.streams import STREAMS
from singer import metadata


def validate_parent_relationships():
    """Validate that parent-tap-stream-id relationships are correctly implemented."""
    print("🔍 Validating Parent-Tap-Stream-ID Implementation")
    print("=" * 60)
    
    # Expected relationships
    expected_relationships = {
        'agent_activity_logs': 'agents',
        'menu_tree': 'menus',
        'team_tree': 'teams'
    }
    
    # 1. Check STREAMS configuration
    print("\n1. Checking STREAMS configuration...")
    for child, expected_parent in expected_relationships.items():
        if child in STREAMS:
            actual_parent = STREAMS[child].get('parent')
            if actual_parent == expected_parent:
                print(f"   ✅ {child} → {expected_parent}")
            else:
                print(f"   ❌ {child} → expected: {expected_parent}, got: {actual_parent}")
                return False
        else:
            print(f"   ❌ {child} not found in STREAMS")
            return False
    
    # 2. Check catalog generation
    print("\n2. Generating catalog and checking metadata...")
    try:
        catalog = discover()
        streams_by_id = {stream.tap_stream_id: stream for stream in catalog.streams}
        
        for child, expected_parent in expected_relationships.items():
            if child in streams_by_id:
                stream_entry = streams_by_id[child]
                mdata_map = metadata.to_map(stream_entry.metadata)
                root_metadata = mdata_map.get((), {})
                
                if 'parent-tap-stream-id' in root_metadata:
                    actual_parent = root_metadata['parent-tap-stream-id']
                    if actual_parent == expected_parent:
                        print(f"   ✅ {child} has parent-tap-stream-id: {expected_parent}")
                    else:
                        print(f"   ❌ {child} parent-tap-stream-id mismatch: expected {expected_parent}, got {actual_parent}")
                        return False
                else:
                    print(f"   ❌ {child} missing parent-tap-stream-id in metadata")
                    return False
            else:
                print(f"   ⚠️  {child} not found in catalog (schema file may be missing)")
        
        # 3. Check independent streams don't have parent metadata
        print("\n3. Verifying independent streams have no parent metadata...")
        independent_streams = ['agents', 'calls', 'chats', 'menus', 'teams', 'user_statuses']
        for stream_name in independent_streams:
            if stream_name in streams_by_id:
                stream_entry = streams_by_id[stream_name]
                mdata_map = metadata.to_map(stream_entry.metadata)
                root_metadata = mdata_map.get((), {})
                
                if 'parent-tap-stream-id' not in root_metadata:
                    print(f"   ✅ {stream_name} (no parent metadata)")
                else:
                    print(f"   ❌ {stream_name} should not have parent-tap-stream-id")
                    return False
        
        print(f"\n✅ All validations passed!")
        print(f"   - {len(expected_relationships)} parent-child relationships configured correctly")
        print(f"   - {len(independent_streams)} independent streams have no parent metadata")  
        print(f"   - Total streams in catalog: {len(catalog.streams)}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during catalog generation: {e}")
        return False


def export_catalog_with_parent_metadata():
    """Generate and export a catalog file showing the parent metadata."""
    print("\n📄 Generating catalog with parent metadata...")
    try:
        catalog = discover()
        catalog_dict = catalog.to_dict()
        
        # Save to file
        output_file = "catalog_with_parent_metadata.json"
        with open(output_file, 'w') as f:
            json.dump(catalog_dict, f, indent=2)
        
        print(f"   ✅ Catalog saved to: {output_file}")
        
        # Count parent relationships in the catalog
        parent_count = 0
        for stream_data in catalog_dict['streams']:
            for metadata_entry in stream_data['metadata']:
                if metadata_entry['breadcrumb'] == [] and 'parent-tap-stream-id' in metadata_entry['metadata']:
                    parent_count += 1
        
        print(f"   📊 Found {parent_count} streams with parent-tap-stream-id metadata")
        return True
        
    except Exception as e:
        print(f"   ❌ Error generating catalog: {e}")
        return False


if __name__ == '__main__':
    print("🧪 Parent-Tap-Stream-ID Feature Validation")
    print("=" * 60)
    
    success = validate_parent_relationships()
    
    if success:
        export_success = export_catalog_with_parent_metadata()
        if export_success:
            print("\n🎉 Parent-tap-stream-id implementation is working correctly!")
            print("\nKey features implemented:")
            print("- Parent fields added to child streams in streams.py")
            print("- Schema.py generates parent-tap-stream-id metadata")
            print("- Discover.py includes parent metadata in catalog")
            print("- Catalog can be serialized with parent relationships")
            
        else:
            success = False
    
    if not success:
        print("\n❌ Validation failed. Please check the implementation.")
        sys.exit(1)
    else:
        sys.exit(0)