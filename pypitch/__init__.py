# Expose sources for direct import
from .sources import *  # noqa: F401, F403

from typing import Any
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
def serve(*args: Any, **kwargs: Any) -> Any:
    """Lazy import of serve function to avoid circular imports."""
    from .serve import serve as _serve
    return _serve(*args, **kwargs)

# 6. Expose Debug Mode
from .runtime.modes import set_debug_mode

# 7. Expose Models
from .models.win_predictor import WinPredictor

# 8. Expose Win Probability Functions
from .compute.winprob import win_probability, set_win_model

# 9. Expose Match Configuration
from .core.match_config import MatchConfig

# 10. Expose the Stats API
# This lets users do: pp.stats.matchup()
import pypitch.api.stats as stats

# 11. Expose the Fantasy API
# This lets users do: pp.fantasy.cheat_sheet()
import pypitch.api.fantasy as fantasy

# 12. Expose the Sim API
# This lets users do: pp.sim.predict_win()
import pypitch.api.sim as sim

# 13. Expose Common Query Objects (Optional but nice)
# This lets users do: q = pp.MatchupQuery(...)
from pypitch.query.matchups import MatchupQuery

# 14. Expose Express Module
# This lets users do: pp.express.load_competition()
import pypitch.express as express

# Version info
__version__ = "0.1.0"
