import sys
import os
import duckdb

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from midwicket.api.session import MidwicketSession

def inspect_registry():
    session = MidwicketSession.get()
    con = session.registry.con
    
    print("Searching for Bradman...")
    res = con.execute("SELECT * FROM aliases WHERE alias LIKE '%Bradman%'").fetchall()
    for row in res:
        print(row)

    print("\nSearching for Kohli...")
    res = con.execute("SELECT * FROM aliases WHERE alias LIKE '%Kohli%'").fetchall()
    for row in res:
        print(row)

if __name__ == "__main__":
    inspect_registry()
