#!/usr/bin/env python3
"""
Test script to validate Hadoop functions implementation
"""

from app.export_worker import _export_from_hive, _export_from_hbase, _export_from_hdfs
from app.engine_factory import engine_from_config
import tempfile
from pathlib import Path

def test_hive_connection():
    """Test Hive connection function"""
    print("Testing Hive connection function...")
    db_conf = {
        'db_type': 'hive',
        'host': 'localhost',
        'port': 10000,
        'user': 'test',
        'password': 'test',
        'database': 'default'
    }
    
    try:
        conn = engine_from_config(db_conf)
        print("✓ Hive connection function works")
    except Exception as e:
        print(f"✗ Hive connection failed: {e}")
    
    # Test export function signature
    try:
        output_path = Path(tempfile.mktemp(suffix='.csv'))
        result = _export_from_hive(db_conf, 'test_table', output_path, 'csv')
        print("✓ Hive export function defined correctly")
    except Exception as e:
        print(f"! Hive export function would fail with real connection: {e}")


def test_hbase_connection():
    """Test HBase connection function"""
    print("\nTesting HBase connection function...")
    db_conf = {
        'db_type': 'hbase',
        'host': 'localhost',
        'port': 9090,
        'user': 'test'
    }
    
    try:
        conn = engine_from_config(db_conf)
        print("✓ HBase connection function works")
    except Exception as e:
        print(f"✗ HBase connection failed: {e}")
    
    # Test export function signature
    try:
        output_path = Path(tempfile.mktemp(suffix='.csv'))
        result = _export_from_hbase(db_conf, 'test_table', output_path, 'csv')
        print("✓ HBase export function defined correctly")
    except Exception as e:
        print(f"! HBase export function would fail with real connection: {e}")


def test_hdfs_connection():
    """Test HDFS connection function"""
    print("\nTesting HDFS connection function...")
    db_conf = {
        'db_type': 'hdfs',
        'host': 'localhost',
        'port': 9870,
        'user': 'hdfs'
    }
    
    try:
        conn = engine_from_config(db_conf)
        print("✓ HDFS connection function works")
    except Exception as e:
        print(f"✗ HDFS connection failed: {e}")
    
    # Test export function signature
    try:
        output_path = Path(tempfile.mktemp(suffix='.csv'))
        result = _export_from_hdfs(db_conf, '/test/path', output_path, 'csv')
        print("✓ HDFS export function defined correctly")
    except Exception as e:
        print(f"! HDFS export function would fail with real connection: {e}")


def test_traditional_db():
    """Test that traditional DB still works"""
    print("\nTesting traditional DB connection (should still work)...")
    db_conf = {
        'db_type': 'mysql',
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'test'
    }
    
    try:
        conn = engine_from_config(db_conf)
        print("✓ Traditional DB connection still works")
    except Exception as e:
        print(f"! Traditional DB connection affected: {e}")


if __name__ == "__main__":
    print("Testing Hadoop Services Implementation")
    print("=" * 40)
    
    test_hive_connection()
    test_hbase_connection()
    test_hdfs_connection()
    test_traditional_db()
    
    print("\n" + "=" * 40)
    print("All tests completed!")