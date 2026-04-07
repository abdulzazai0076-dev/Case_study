#!/usr/bin/env python3
"""
Comprehensive test suite for the weather pipeline project
Tests: dependencies, structure, code quality, data integrity, end-to-end execution
"""

import os
import sys
import sqlite3
import subprocess
import ast

class TestSuite:
    def __init__(self):
        self.tests_passed = 0
        self.tests_failed = 0
        self.issues = []
    
    def print_header(self, test_name):
        print(f"\n{'='*70}")
        print(f"TEST: {test_name}")
        print('='*70)
    
    def pass_test(self, msg):
        print(f"  [PASS] {msg}")
        self.tests_passed += 1
    
    def fail_test(self, msg):
        print(f"  [FAIL] {msg}")
        self.tests_failed += 1
        self.issues.append(msg)
    
    def run_all_tests(self):
        print("\n" + "="*70)
        print("COMPREHENSIVE PROJECT TEST SUITE")
        print("="*70)
        
        self.test_python_version()
        self.test_no_external_deps()
        self.test_builtin_libs()
        self.test_file_structure()
        self.test_no_requests_in_code()
        self.test_code_syntax()
        self.test_database_schema()
        self.test_ingest_script()
        self.test_transform_script()
        self.test_data_integrity()
        self.test_requirements()
        
        self.print_summary()
    
    def test_python_version(self):
        self.print_header("Python Version")
        version = sys.version_info
        if version >= (3, 7):
            self.pass_test(f"Python {version.major}.{version.minor} (3.7+ required)")
        else:
            self.fail_test(f"Python {version.major}.{version.minor} (need 3.7+)")
    
    def test_no_external_deps(self):
        self.print_header("No External Dependencies")
        dangerous_imports = ['requests', 'pandas', 'numpy', 'dbt']
        
        for lib in dangerous_imports:
            try:
                __import__(lib)
                self.fail_test(f"Found external dependency: {lib} (should use built-ins)")
            except ImportError:
                self.pass_test(f"{lib} not installed")
    
    def test_builtin_libs(self):
        self.print_header("Built-in Libraries")
        required = ['sqlite3', 'urllib', 'json', 'logging']
        
        for lib in required:
            try:
                __import__(lib)
                self.pass_test(f"{lib} available")
            except ImportError:
                self.fail_test(f"{lib} NOT FOUND")
    
    def test_file_structure(self):
        self.print_header("File Structure")
        required_files = {
            'ingest.py': 'Ingestion script',
            'transform.py': 'Transformation script',
            'weather.db': 'SQLite database',
            'requirements.txt': 'Dependencies file',
            'README.md': 'Main documentation',
            'ANSWERS.md': 'Task 3 answers',
            'TECH_CHOICES.md': 'Technology explanation'
        }
        
        for fname, desc in required_files.items():
            if os.path.isfile(fname):
                self.pass_test(f"{fname:25} ({desc})")
            else:
                self.fail_test(f"{fname:25} MISSING ({desc})")
        
        # Check models
        models_required = {
            'models/staging/stg_weather.sql': 'Staging model',
            'models/marts/fct_weather_summary.sql': 'Fact model',
            'models/schema.yml': 'Schema definition'
        }
        
        for fname, desc in models_required.items():
            if os.path.isfile(fname):
                self.pass_test(f"{fname:45} ({desc})")
            else:
                self.fail_test(f"{fname:45} MISSING ({desc})")
    
    def test_no_requests_in_code(self):
        self.print_header("No 'requests' Library Used")
        
        # Check ingest.py
        with open('ingest.py', 'r') as f:
            ingest_code = f.read()
        
        if 'import requests' in ingest_code or 'from requests' in ingest_code:
            self.fail_test("ingest.py still uses 'requests' library")
        else:
            self.pass_test("ingest.py uses urllib (no requests)")
        
        if 'urllib.request' in ingest_code:
            self.pass_test("ingest.py correctly uses urllib.request")
        else:
            self.fail_test("ingest.py doesn't use urllib.request")
    
    def test_code_syntax(self):
        self.print_header("Python Syntax Check")
        
        python_files = ['ingest.py', 'transform.py']
        
        for fname in python_files:
            try:
                with open(fname, 'r') as f:
                    ast.parse(f.read())
                self.pass_test(f"{fname:20} syntax valid")
            except SyntaxError as e:
                self.fail_test(f"{fname:20} SYNTAX ERROR: {e}")
    
    def test_database_schema(self):
        self.print_header("Database Schema")
        
        try:
            conn = sqlite3.connect('weather.db')
            cursor = conn.cursor()
            
            # Check tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            
            required_tables = ['raw_weather', 'stg_weather', 'fct_weather_summary']
            for table in required_tables:
                if table in tables:
                    self.pass_test(f"Table '{table}' exists")
                else:
                    self.fail_test(f"Table '{table}' NOT FOUND")
            
            # Check raw_weather schema
            cursor.execute("PRAGMA table_info(raw_weather)")
            raw_cols = {row[1] for row in cursor.fetchall()}
            expected_raw = {'city', 'date', 'temp_max_c', 'temp_min_c', 'precipitation_mm', 'ingested_at'}
            
            if expected_raw.issubset(raw_cols):
                self.pass_test("raw_weather schema correct")
            else:
                missing = expected_raw - raw_cols
                self.fail_test(f"raw_weather missing columns: {missing}")
            
            conn.close()
        except Exception as e:
            self.fail_test(f"Database check failed: {e}")
    
    def test_ingest_script(self):
        self.print_header("Ingest Script Execution")
        
        try:
            # Check if script runs without errors
            result = subprocess.run(
                ['python3', 'ingest.py'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                if 'Ingestion complete' in result.stdout:
                    self.pass_test("ingest.py runs successfully")
                    
                    # Check row count
                    conn = sqlite3.connect('weather.db')
                    count = conn.execute("SELECT COUNT(*) FROM raw_weather").fetchone()[0]
                    if count == 28:
                        self.pass_test(f"raw_weather has 28 rows")
                    else:
                        self.fail_test(f"raw_weather has {count} rows (expected 28)")
                    conn.close()
                else:
                    self.fail_test("ingest.py output unexpected")
            else:
                self.fail_test(f"ingest.py failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            self.fail_test("ingest.py timed out")
        except Exception as e:
            self.fail_test(f"Error running ingest.py: {e}")
    
    def test_transform_script(self):
        self.print_header("Transform Script Execution")
        
        try:
            result = subprocess.run(
                ['python3', 'transform.py'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                if 'Transformations complete' in result.stdout or 'created' in result.stdout:
                    self.pass_test("transform.py runs successfully")
                else:
                    self.fail_test("transform.py output unexpected")
            else:
                self.fail_test(f"transform.py failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            self.fail_test("transform.py timed out")
        except Exception as e:
            self.fail_test(f"Error running transform.py: {e}")
    
    def test_data_integrity(self):
        self.print_header("Data Integrity Checks")
        
        try:
            conn = sqlite3.connect('weather.db')
            
            # Test 1: No null dates in staging
            null_dates = conn.execute(
                "SELECT COUNT(*) FROM stg_weather WHERE date IS NULL"
            ).fetchone()[0]
            if null_dates == 0:
                self.pass_test("No null dates in stg_weather")
            else:
                self.fail_test(f"Found {null_dates} null dates in stg_weather")
            
            # Test 2: No duplicates on (city, date)
            duplicates = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT city, date FROM stg_weather 
                    GROUP BY city, date HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            if duplicates == 0:
                self.pass_test("No duplicate (city, date) pairs in stg_weather")
            else:
                self.fail_test(f"Found {duplicates} duplicate pairs")
            
            # Test 3: Fact table has one row per city
            cities = conn.execute("SELECT COUNT(*) FROM fct_weather_summary").fetchone()[0]
            if cities == 4:
                self.pass_test("fct_weather_summary has 4 rows (one per city)")
            else:
                self.fail_test(f"fct_weather_summary has {cities} rows (expected 4)")
            
            # Test 4: Aggregations are not null
            null_aggs = conn.execute("""
                SELECT COUNT(*) FROM fct_weather_summary 
                WHERE avg_temp_max_c IS NULL 
                OR avg_temp_min_c IS NULL
            """).fetchone()[0]
            if null_aggs == 0:
                self.pass_test("All aggregations are non-null")
            else:
                self.fail_test(f"Found {null_aggs} rows with null aggregations")
            
            # Test 5: Temperature logic (max >= min)
            bad_temps = conn.execute("""
                SELECT COUNT(*) FROM stg_weather 
                WHERE temp_max_c < temp_min_c AND temp_max_c IS NOT NULL
            """).fetchone()[0]
            if bad_temps == 0:
                self.pass_test("Temperature logic valid (max >= min)")
            else:
                self.fail_test(f"Found {bad_temps} rows with temp_max < temp_min")
            
            conn.close()
        except Exception as e:
            self.fail_test(f"Data integrity check failed: {e}")
    
    def test_requirements(self):
        self.print_header("Requirements File")
        
        try:
            with open('requirements.txt', 'r') as f:
                content = f.read()
            
            # Check for comments about no dependencies
            if 'built-in' in content.lower() or 'no external' in content.lower():
                self.pass_test("requirements.txt documents no external deps")
            
            # Check no actual packages are listed (comments don't count)
            lines = [l.strip() for l in content.split('\n') if l.strip() and not l.startswith('#')]
            if not lines:
                self.pass_test("requirements.txt is clean (no packages listed)")
            else:
                self.fail_test(f"requirements.txt still lists: {lines}")
        except Exception as e:
            self.fail_test(f"Requirements check failed: {e}")
    
    def print_summary(self):
        total = self.tests_passed + self.tests_failed
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print(f"Passed: {self.tests_passed}")
        print(f"Failed: {self.tests_failed}")
        print(f"Total:  {total}")
        
        if self.tests_failed == 0:
            print("\nALL TESTS PASSED! Project is fully refactored and tested.")
        else:
            print(f"\n{self.tests_failed} test(s) failed. See above for details.")
            if self.issues:
                print("\nIssues to fix:")
                for i, issue in enumerate(self.issues, 1):
                    print(f"  {i}. {issue}")
        print("="*70 + "\n")

if __name__ == "__main__":
    suite = TestSuite()
    suite.run_all_tests()
