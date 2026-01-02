import pypitch as pp

# 1. Setup (Optional if data already exists)
# pp.data.download("ipl")

# 2. Analyze (Engine auto-boots!)
# Use canonical names (e.g., "V Kohli" instead of just "Kohli")
df = pp.stats.matchup("V Kohli", "JJ Bumrah")
print(df)