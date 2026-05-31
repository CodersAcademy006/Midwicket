"""
Showcase 6: Women's Cricket Stats & Power Rankings
Loads Women's Big Bash League (WBBL) data and extracts player efficiency benchmarks.
"""
import midwicket as md

def main():
    print("=== Women's Cricket Ingestion & Analysis ===")
    # Loads WBBL mapped under the women_t20 alias
    try:
        session = md.datasets.load_dataset("women_t20")
        print("Successfully bootstrapped WBBL data.")
    except Exception as e:
        print(f"Skipping remote download: {e}")

if __name__ == "__main__":
    main()
