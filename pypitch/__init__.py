# pypitch/__init__.py

# 1. Expose the Core Session & Init
# This lets users do: pp.init() or pp.PyPitchSession
from pypitch.api.session import PyPitchSession, init

# 2. Expose the Data Module
# This lets users do: pp.data.download()
import pypitch.data as data

# 3. Expose the Stats API
# This lets users do: pp.stats.matchup()
import pypitch.api.stats as stats

# 4. Expose Common Query Objects (Optional but nice)
# This lets users do: q = pp.MatchupQuery(...)
from pypitch.query.matchups import MatchupQuery

# Version info
__version__ = "0.1.0"

# Cleanup namespace (optional, keeps dir(pp) clean)
del api
del query
