"""
Showcase 25: Comprehensive Ingestion Sweep
Loads and summarizes dataset benchmarks.
"""
import midwicket as md
from midwicket.datasets import list_datasets

def main():
    print("=== Full Dataset Catalog Tour ===")
    catalog = list_datasets()
    for name, meta in catalog.items():
        print(f"Competition: {name} | {meta['description']}")

if __name__ == "__main__":
    main()
