"""Fantasy cheat sheet for a venue - top picks + averages."""

from midwicket.api.fantasy import cheat_sheet

print(cheat_sheet("Wankhede Stadium", last_n_years=3).head(10))
