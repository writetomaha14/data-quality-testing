"""
Demo Notebooks Runner Script

Run this before committing to ensure all demo notebooks execute successfully.
This script runs all demo notebooks in the phase2_dq_framework directory.

Usage:
    python run_all_demos.py
    OR run in a Databricks notebook cell
"""

import os
import sys


def main():
    """Run all demo notebooks and report results."""
    print("=" * 70)
    print("Running All Data Quality Demo Notebooks")
    print("=" * 70)
    
    # Demo notebooks in order
    demo_notebooks = [
        'day34_foreign_keys_demo',
        'day35_schema_validation_demo',
        # Add new demo notebooks here as you create them
        # 'day36_freshness_demo',
        # 'day37_duplicate_check_demo',
    ]
    
    results = {}
    failed_notebooks = []
    
    # Get base path (current directory should be notebooks/phase2_dq_framework)
    base_path = os.getcwd()
    
    print(f"\nBase path: {base_path}")
    print(f"\nFound {len(demo_notebooks)} demo notebooks to run\n")
    
    for notebook_name in demo_notebooks:
        print(f"\n{'='*70}")
        print(f"Running: {notebook_name}")
        print(f"{'='*70}")
        
        try:
            # Use dbutils.notebook.run to execute the notebook
            # Timeout: 300 seconds (5 minutes) per notebook
            result = dbutils.notebook.run(
                f"./{notebook_name}",
                timeout_seconds=300
            )
            
            results[notebook_name] = "PASSED"
            print(f"\n✅ {notebook_name}: PASSED")
            
        except Exception as e:
            results[notebook_name] = "FAILED"
            failed_notebooks.append(notebook_name)
            print(f"\n❌ {notebook_name}: FAILED")
            print(f"   Error: {str(e)}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("DEMO NOTEBOOKS EXECUTION SUMMARY")
    print("=" * 70)
    
    for notebook_name, status in results.items():
        icon = "✅" if status == "PASSED" else "❌"
        print(f"{icon} {notebook_name}: {status}")
    
    print("\n" + "=" * 70)
    
    if not failed_notebooks:
        print("✅ ALL DEMO NOTEBOOKS PASSED - Safe to commit!")
        print("=" * 70)
        return 0
    else:
        print(f"❌ {len(failed_notebooks)} DEMO NOTEBOOK(S) FAILED")
        print("   Fix the following before committing:")
        for nb in failed_notebooks:
            print(f"   - {nb}")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    result = main()
    if result != 0:
        sys.exit(result)
