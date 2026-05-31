"""
Women's Cricket Study: Wpl Powerplay Telemetry
Loads and analyzes WPL and WBBL players metric distributions.
"""
import midwicket as md

def main():
    print("=== Women's Cricket: Wpl Powerplay ===")
    try:
        session = md.datasets.load_dataset("women_t20")
        print("Loaded WPL/WBBL telemetry successfully.")
    except Exception as e:
        print(f"Using local MLC session fallback: {e}")
        session = md.datasets.load_dataset("mlc")

if __name__ == "__main__":
    main()
