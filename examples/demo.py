import midwicket as md

# 1. Setup (Optional if data already exists)
# md.data.download("ipl")

# 2. Analyze (Engine auto-boots!)
# Use canonical names (e.g., "V Kohli" instead of just "Kohli")
df = md.stats.matchup("V Kohli", "JJ Bumrah")
print(df)