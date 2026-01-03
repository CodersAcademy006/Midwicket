# Expose sources for direct import
from .sources import *

# 1. Expose the Core Session & Init
# This lets users do: pp.init() or pp.PyPitchSession
from .api.session import PyPitchSession, init

# 2. Expose the Data Module
# This lets users do: pp.data.download()
from . import data

# 3. Expose the API Module
# This lets users do: pp.api.session
from . import api

# 4. Expose the Visuals Module
# This lets users do: pp.visuals.plot_worm_graph
from . import visuals

# 5. Expose the Serve Module (lazy import to avoid dependency issues)
# This lets users do: pp.serve()
def serve(*args, **kwargs):
    """Lazy import of serve function to avoid circular imports."""
    from .serve import serve as _serve
    return _serve(*args, **kwargs)

# 5. Expose Debug Mode
from .runtime.modes import set_debug_mode

# 6. Expose Models
from .models.win_predictor import WinPredictor

# 7. Expose Win Probability Functions
from .compute.winprob import win_probability, set_win_model

# 8. Expose Match Configuration
from .core.match_config import MatchConfig

# 3. Expose the Stats API
# This lets users do: pp.stats.matchup()
import pypitch.api.stats as stats

# 5. Expose the Fantasy API
# This lets users do: pp.fantasy.cheat_sheet()
import pypitch.api.fantasy as fantasy

# 6. Expose the Sim API
# This lets users do: pp.sim.predict_win()
import pypitch.api.sim as sim

# 4. Expose Common Query Objects (Optional but nice)
# This lets users do: q = pp.MatchupQuery(...)
from pypitch.query.matchups import MatchupQuery

# 9. Expose Express Module
# This lets users do: pp.express.load_competition()
import pypitch.express as express

# Version info
__version__ = "0.1.0"

# Cleanup namespace (optional, keeps dir(pp) clean)
del api
