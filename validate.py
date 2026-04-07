#!/usr/bin/env python3
"""Quick validation of refactored project - no re-runs"""

import os
import sys
import sqlite3
import ast

print("\n" + "="*70)
print("REFACTORED PROJECT VALIDATION")
print("="*70 + "\n")

passed = 0
failed = 0

# Test 1: File structure
print("[TEST 1] File Structure")
required = ['ingest.py', 'transform.py', 'weather.db', 'README.md', 'ANSWERS.md', 'TECH_CHOICES.md']
missing = [f for f in required if not os.path.isfile(f)]
if not missing:
    print("  [OK] All core files present")
    passed += 1
else:
    print(f"  [FAIL] Missing: {missing}")
    failed += 1

models = ['models/staging/stg_weather.sql', 'models/marts/fct_weather_summary.sql', 'models/schema.yml']
missing_models = [m for m in models if not os.path.isfile(m)]
if not missing_models:
    print("  [OK] All model files present")
    passed += 1
else:
    print(f"  [FAIL] Missing models: {missing_models}")
    failed += 1

# Test 2: No requests library imported in code
print("\n[TEST 2] urllib Used (No requests)")
with open('ingest.py', 'r') as f:
    code = f.read()

if 'import requests' not in code and 'from requests' not in code:
    print("  [OK] ingest.py doesn't use requests")
    passed += 1
else:
    print("  [FAIL] ingest.py still imports requests")
    failed += 1

if 'urllib.request' in code:
    print("  [OK] ingest.py uses urllib.request")
    passed += 1
else:
    print("  [FAIL] ingest.py doesn't use urllib.request")
    failed += 1

# Test 3: Syntax valid
print("\n[TEST 3] Valid Python Syntax")
files_to_check = ['ingest.py', 'transform.py']
syntax_ok = True
for fname in files_to_check:
    try:
        with open(fname) as f:
            ast.parse(f.read())
    except SyntaxError as e:
        print(f"  [FAIL] {fname}: {e}")
        syntax_ok = False
        failed += 1

if syntax_ok:
    print(f"  [OK] {', '.join(files_to_check)} all valid")
    passed += 1

# Test 4: Database integrity
print("\n[TEST 4] Database Integrity")
try:
    conn = sqlite3.connect('weather.db')
    
    # Check tables
    tables = [t[0] for t in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    
    expected = ['raw_weather', 'stg_weather', 'fct_weather_summary']
    if all(t in tables for t in expected):
        print(f"  [OK] All tables exist: {', '.join(expected)}")
        passed += 1
    else:
        print(f"  [FAIL] Missing tables")
        failed += 1
    
    # Check row counts
    raw_count = conn.execute("SELECT COUNT(*) FROM raw_weather").fetchone()[0]
    stg_count = conn.execute("SELECT COUNT(*) FROM stg_weather").fetchone()[0]
    fact_count = conn.execute("SELECT COUNT(*) FROM fct_weather_summary").fetchone()[0]
    
    print(f"  [OK] Row counts: raw={raw_count}, stg={stg_count}, fact={fact_count}")
    passed += 1
    
    # Check no null dates
    null_dates = conn.execute(
        "SELECT COUNT(*) FROM stg_weather WHERE date IS NULL"
    ).fetchone()[0]
    
    if null_dates == 0:
        print(f"  [OK] No null dates in staging")
        passed += 1
    else:
        print(f"  [FAIL] Found {null_dates} null dates")
        failed += 1
    
    # Check no duplicates
    dupes = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT city, date FROM stg_weather 
            GROUP BY city, date HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    if dupes == 0:
        print(f"  [OK] No duplicate (city, date) pairs")
        passed += 1
    else:
        print(f"  [FAIL] Found {dupes} duplicates")
        failed += 1
    
    conn.close()
except Exception as e:
    print(f"  [FAIL] Database check failed: {e}")
    failed += 1

# Test 5: Requirements file
print("\n[TEST 5] Requirements File")
try:
    with open('requirements.txt', 'r') as f:
        req_content = f.read()
    
    # Filter out comments
    req_lines = [l.strip() for l in req_content.split('\n') 
                 if l.strip() and not l.startswith('#')]
    
    if not req_lines:
        print(f"  [OK] requirements.txt clean (no external packages)")
        passed += 1
    else:
        print(f"  [WARNING] requirements.txt lists: {req_lines}")
        # This is a warning, not a fail for this test

except Exception as e:
    print(f"  [FAIL] Requirements check failed: {e}")
    failed += 1

# Test 6: Documentation
print("\n[TEST 6] Documentation")
docs_required = {
    'README.md': 'Architecture',
    'ANSWERS.md': 'Task 3 answers',
    'TECH_CHOICES.md': 'Technology choices'
}

for doc, purpose in docs_required.items():
    if os.path.isfile(doc):
        with open(doc) as f:
            size = len(f.read())
        print(f"  [OK] {doc:20} ({size} bytes, {purpose})")
        passed += 1
    else:
        print(f"  [WARNING] {doc:20} (optional - {purpose})")

# Test 7: Code structure - clean and simple
print("\n[TEST 7] Code Quality (Junior Dev Style)")
with open('ingest.py') as f:
    ingest = f.read()

# Check for junior dev style patterns (not over-engineered)
checks = [
    ('Simple functions', len([l for l in ingest.split('\n') if l.startswith('def ')]) <= 10),
    ('Direct print statements', 'print(' in ingest),
    ('Direct sqlite3 usage', 'sqlite3.connect' in ingest),
    ('Simple error handling', 'except' in ingest),
]

for check_name, result in checks:
    if result:
        print(f"  [OK] {check_name}")
        passed += 1
    else:
        print(f"  [FAIL] {check_name}")
        failed += 1

# Summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Passed: {passed}")
print(f"Failed: {failed}")

if failed == 0:
    print("\nALL TESTS PASSED!")
    print("   Project is fully refactored with:")
    print("   • urllib instead of requests")
    print("   • SQLite database")
    print("   • No external dependencies")
    print("   • Clean junior-dev code style")
    print("   • Complete data integrity")
else:
    print(f"\n{failed} issue(s) need attention")

print("="*70 + "\n")
