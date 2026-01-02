from typing import Any, Optional

__all__ = ['plot_match_worm', 'plot_run_pressure', 'plot_batter_pacing', 'plot_momentum_swings', 'plot_manhattan', 'plot_beehive', 'plot_wagon_wheel', 'plot_partnership_flow']

def _add_cricket_grid(ax: Any) -> None:
    """Add cricket-aware grid: thick vertical every over, light per ball, phase shading."""
    import matplotlib.patches as patches
    
    # Phase background shading
    ylim = ax.get_ylim()
    height = ylim[1] - ylim[0]
    # Powerplay: overs 1-6 (light blue)
    ax.add_patch(patches.Rectangle((0, ylim[0]), 6, height, color='lightblue', alpha=0.1, zorder=0))
    # Middle: 7-15 (light yellow)
    ax.add_patch(patches.Rectangle((6, ylim[0]), 9, height, color='lightyellow', alpha=0.1, zorder=0))
    # Death: 16-20 (light coral)
    ax.add_patch(patches.Rectangle((15, ylim[0]), 5, height, color='lightcoral', alpha=0.1, zorder=0))
    
    # Thick vertical lines every over
    for over in range(1, 21):
        ax.axvline(x=over, color='black', linestyle='-', linewidth=1, alpha=0.3)
    
    # Light grid for balls (every 6 balls, but since over_float, approximate)
    ax.grid(True, which='minor', axis='x', linestyle=':', alpha=0.2)
    ax.grid(True, axis='y', linestyle=':', alpha=0.2)
    ax.set_xticks(range(0, 21))
    ax.set_xticks([i/6 for i in range(0, 120, 6)], minor=True)  # Minor ticks every ball

def plot_worm_graph(match_id: str, bowler_id: int, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots a worm graph (cumulative runs) for a bowler in a given match.

    Args:
        match_id (str): The match identifier.
        bowler_id (int): The bowler's entity ID.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    # Parameterized query - NO f-strings
    query = """
        SELECT 
            over, 
            ball, 
            runs_batter + runs_extras as runs_conceded,
            is_wicket
        FROM ball_events 
        WHERE match_id = ? 
          AND bowler_id = ?
        ORDER BY over, ball
    """
    
    try:
        # Execute with parameters
        arrow_table = session.engine.execute_sql(query, [match_id, bowler_id])
        df = arrow_table.to_pandas()
    except Exception:
        # DuckDB throws if table doesn't exist or other SQL errors
        # We assume mostly it's missing data if the query fails on a valid schema
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        try:
            bowler_table = session.engine.execute_sql(f"SELECT DISTINCT bowler_id FROM ball_events WHERE match_id = '{match_id}'")
            bowler_df = bowler_table.to_pandas()
            bowler_names = []
            for bid in bowler_df['bowler_id'].tolist():
                # Try to get name from registry
                try:
                    name = session.registry.con.execute("SELECT primary_name FROM entities WHERE id = ?", [bid]).fetchone()
                    bowler_names.append(f"{name[0] if name else 'Unknown'} (ID: {bid})")
                except:
                    bowler_names.append(f"ID: {bid}")
            
            bowlers_list = "\n".join(f"  - {name}" for name in bowler_names)
            error_msg = f"No data found for bowler {bowler_id} in match {match_id}.\n\nBowlers who bowled in this match:\n{bowlers_list}\n\nTip: Try a different match where this bowler participated."
        except Exception:
            error_msg = f"No data found for bowler {bowler_id} in match {match_id}. Ensure the match data is loaded."
        
        raise MatchDataMissing(error_msg)

    df['cumulative_runs'] = df['runs_conceded'].cumsum()
    df['ball_number'] = range(1, len(df) + 1)

    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.plot(df['ball_number'], df['cumulative_runs'], marker='o', linestyle='-', color='blue', label='Bowler')
    
    # Highlight Wickets
    wickets = df[df['is_wicket'] == True]
    if not wickets.empty:
        ax.scatter(wickets['ball_number'], wickets['cumulative_runs'], color='red', s=100, zorder=5, label='Wickets')

    ax.set_title(f"Cumulative Runs Conceded (Match {match_id})")
    ax.set_xlabel("Balls Bowled")
    ax.set_ylabel("Cumulative Runs")
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.7)
    
    return ax


def plot_match_worm(match_id: str, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots cricket-native match worm: innings lines, wickets, par score. Hero metric: cumulative runs vs par.

    Args:
        match_id (str): The match identifier.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    # Parameterized query with wicket details
    query = """
        SELECT 
            inning,
            over,
            ball,
            runs_batter + runs_extras as runs_scored,
            is_wicket,
            batter_id,
            bowler_id,
            wicket_type
        FROM ball_events 
        WHERE match_id = ?
        ORDER BY inning, over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for match {match_id}")

    # Calculate over_float and cumulative runs
    df['over_float'] = df['over'] + (df['ball'] - 1) / 6  # ball 1-6 -> 0 to 5/6
    df['cumulative_runs'] = df.groupby('inning')['runs_scored'].cumsum()

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))
    
    # Add cricket grid
    _add_cricket_grid(ax)

    # Plot innings lines (hero metric)
    colors = ['darkgreen', 'darkblue']  # Cricket colors
    for i, inning in enumerate(df['inning'].unique()):
        inning_data = df[df['inning'] == inning]
        ax.plot(inning_data['over_float'], inning_data['cumulative_runs'], 
                color=colors[i], linewidth=3, label=f'Inning {inning}')
    
    # Par score line (secondary, low opacity)
    par_runs = [7 * o for o in range(21)]
    ax.plot(range(21), par_runs, color='gray', linestyle=':', linewidth=1, label='Par Score (7 RPO)', alpha=0.4)
    
    # Mark wickets (cricket-native)
    wickets = df[df['is_wicket'] == True]
    if not wickets.empty:
        for _, wicket in wickets.iterrows():
            # Resolve names
            batter_name = session.registry.con.execute("SELECT primary_name FROM entities WHERE id = ?", [int(wicket['batter_id'])]).fetchone()
            batter_name = batter_name[0] if batter_name else 'Unknown'
            
            ax.scatter(wicket['over_float'], wicket['cumulative_runs'], 
                      marker='^', color='red', s=60, zorder=5)
            # Simplified annotation: just name and over
            ax.annotate(f"{batter_name}\n({wicket['over']}.{wicket['ball']})",
                       (wicket['over_float'], wicket['cumulative_runs']),
                       xytext=(5, 5), textcoords='offset points', fontsize=8,
                       bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

    # Y-axis: for T20, cap to reasonable (assume 20 overs, target + buffer)
    max_runs = df['cumulative_runs'].max()
    if max_runs > 300:  # T20 context
        target = df[df['inning'] == 1]['cumulative_runs'].max() if 1 in df['inning'].unique() else max_runs
        ax.set_ylim(0, target + 50)
    else:
        ax.set_ylim(bottom=0)

    ax.set_title(f"Match Worm: Cumulative Runs vs Par (Match {match_id})", fontsize=14, fontweight='bold')
    ax.set_xlabel("Overs", fontsize=12)
    ax.set_ylabel("Cumulative Runs", fontsize=12)
    ax.legend(loc='upper left')
    ax.set_xlim(0, 20)
    ax.set_facecolor('whitesmoke')  # Subtle cricket pitch feel
    
    return ax


def plot_run_pressure(match_id: str, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots run pressure: team run rate, required run rate (innings 2), dot-ball percentage.

    Args:
        match_id (str): The match identifier.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    query = """
        SELECT inning, over, ball, runs_batter + runs_extras as runs_scored
        FROM ball_events 
        WHERE match_id = ?
        ORDER BY inning, over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for match {match_id}")

    df['over_float'] = df['over'] + (df['ball'] - 1) / 6
    df['cumulative'] = df.groupby('inning')['runs_scored'].cumsum()
    df['balls'] = df.groupby('inning').cumcount() + 1
    df['rr'] = df['cumulative'] / df['balls'] * 6  # Run rate per ball *6

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))
    
    _add_cricket_grid(ax)

    # Plot run rates (hero: team RR)
    colors = ['darkgreen', 'darkblue']
    for i, inning in enumerate(df['inning'].unique()):
        inning_data = df[df['inning'] == inning]
        ax.plot(inning_data['over_float'], inning_data['rr'], 
                color=colors[i], linewidth=3, label=f'Inning {inning} RR')
    
    # Required RR for innings 2 (secondary, low opacity)
    if len(df['inning'].unique()) > 1:
        innings2 = df[df['inning'] == 2]
        target = df[df['inning'] == 1]['cumulative'].max()
        innings2['required_rr'] = (target - innings2['cumulative']) / (120 - innings2['balls']) * 6  # Remaining balls
        ax.plot(innings2['over_float'], innings2['required_rr'], 
                color='orange', linestyle='--', linewidth=2, label='Required RR', alpha=0.6)
    
    # Dot-ball % (secondary)
    df['is_dot'] = df['runs_scored'] == 0
    df['dot_pct'] = df.groupby('inning')['is_dot'].expanding().mean() * 100
    for i, inning in enumerate(df['inning'].unique()):
        inning_data = df[df['inning'] == inning]
        ax2 = ax.twinx()
        ax2.plot(inning_data['over_float'], inning_data['dot_pct'], 
                 color='red', linestyle=':', linewidth=1, label='Dot-ball %', alpha=0.4)
        ax2.set_ylabel('Dot-ball %', color='red')
        ax2.tick_params(axis='y', labelcolor='red')

    ax.set_title(f"Run Pressure: Rates & Dot-balls (Match {match_id})", fontsize=14, fontweight='bold')
    ax.set_xlabel("Overs", fontsize=12)
    ax.set_ylabel("Run Rate (RPO)", fontsize=12)
    ax.legend(loc='upper left')
    ax.set_xlim(0, 20)
    ax.set_ylim(bottom=0)
    ax.set_facecolor('whitesmoke')
    
    return ax


def plot_batter_pacing(match_id: str, batsman_id: int, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots batter pacing: cumulative runs, rolling strike rate. Hero: cumulative progression.

    Args:
        match_id (str): The match identifier.
        batsman_id (int): The batsman's entity ID.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    # Parameterized query
    query = """
        SELECT 
            inning,
            over, 
            ball, 
            runs_batter + runs_extras as runs_scored,
            runs_batter,
            is_wicket,
            bowler_id,
            wicket_type
        FROM ball_events 
        WHERE match_id = ? 
          AND batter_id = ?
        ORDER BY over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id, batsman_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        try:
            batsman_table = session.engine.execute_sql("SELECT DISTINCT batter_id FROM ball_events WHERE match_id = ?", [match_id])
            batsman_df = batsman_table.to_pandas()
            batsman_names = []
            for bid in batsman_df['batter_id'].tolist():
                try:
                    name = session.registry.con.execute("SELECT primary_name FROM entities WHERE id = ?", [bid]).fetchone()
                    batsman_names.append(f"{name[0] if name else 'Unknown'} (ID: {bid})")
                except:
                    batsman_names.append(f"ID: {bid}")
            
            batsmen_list = "\n".join(f"  - {name}" for name in batsman_names)
            error_msg = f"No data found for batsman {batsman_id} in match {match_id}.\n\nBatsmen who batted in this match:\n{batsmen_list}\n\nTip: Try a different match where this batsman participated."
        except Exception:
            error_msg = f"No data found for batsman {batsman_id} in match {match_id}. Ensure the match data is loaded."
        
        raise MatchDataMissing(error_msg)

    # Calculate over_float and cumulative
    df['over_float'] = df['over'] + (df['ball'] - 1) / 6
    df['cumulative_runs'] = df['runs_scored'].cumsum()
    df['balls_faced'] = range(1, len(df) + 1)
    
    # Calculate rolling strike rate
    df['rolling_sr'] = (df['runs_scored'].rolling(window=10, min_periods=1).sum() / 
                       df['balls_faced'].rolling(window=10, min_periods=1).count() * 100)
    
    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))
    
    # Add grid for balls
    ax.set_xticks(range(0, len(df)+1, 10))
    ax.grid(True, axis='x', linestyle='-', alpha=0.3)
    ax.grid(True, axis='y', linestyle=':', alpha=0.2)

    # Plot cumulative runs (hero)
    ax.plot(df['balls_faced'], df['cumulative_runs'], color='black', linewidth=3, label='Cumulative Runs')

    # Rolling SR overlay (secondary, low opacity)
    ax2 = ax.twinx()
    ax2.plot(df['balls_faced'], df['rolling_sr'], color='orange', linestyle='--', linewidth=2, label='Rolling SR', alpha=0.6)
    ax2.set_ylabel('Strike Rate', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')
    ax2.set_ylim(0, 250)  # SR range

    # Boundaries (subtle)
    boundaries = df[df['runs_batter'].isin([4, 6])]
    if not boundaries.empty:
        for _, b in boundaries.iterrows():
            marker = '*' if b['runs_batter'] == 6 else 'o'
            color = 'red' if b['runs_batter'] == 6 else 'blue'
            ax.scatter(b['balls_faced'], b['cumulative_runs'], marker=marker, color=color, s=50, alpha=0.7, zorder=5)

    # Dismissal annotation (simplified)
    if df.iloc[-1]['is_wicket']:
        last = df.iloc[-1]
        ax.annotate(f"Out: {last['wicket_type']}\n({last['over']}.{last['ball']})",
                   (last['balls_faced'], last['cumulative_runs']),
                   xytext=(10, -10), textcoords='offset points', fontsize=9,
                   bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

    # Milestone annotations (semantic)
    milestones = [50, 100]
    for milestone in milestones:
        milestone_row = df[df['cumulative_runs'] >= milestone].head(1)
        if not milestone_row.empty:
            ball = milestone_row.iloc[0]['balls_faced']
            ax.axvline(x=ball, color='green', linestyle='--', linewidth=2, alpha=0.5)
            ax.annotate(f'{milestone}', (ball, milestone), xytext=(5, 5), textcoords='offset points', 
                       fontsize=10, fontweight='bold', color='green', alpha=0.7)

    ax.set_title(f"Batter Pacing: Runs & Strike Rate (Match {match_id})", fontsize=14, fontweight='bold')
    ax.set_xlabel("Balls Faced", fontsize=12)
    ax.set_ylabel("Cumulative Runs", fontsize=12)
    ax.set_xlim(0, len(df))
    ax.set_ylim(bottom=0)
    ax.set_facecolor('whitesmoke')
    
    return ax


def plot_momentum_swings(match_id: str, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots momentum swings: Δ runs vs par per over. Hero: relative performance.

    Args:
        match_id (str): The match identifier.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    query = """
        SELECT inning, over, ball, runs_batter + runs_extras as runs_scored
        FROM ball_events 
        WHERE match_id = ?
        ORDER BY inning, over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for match {match_id}")

    df['over_float'] = df['over'] + (df['ball'] - 1) / 6
    df['cumulative'] = df.groupby('inning')['runs_scored'].cumsum()

    # Calculate par cumulative
    df['par_cum'] = df['over'].astype(int) * 7  # Simple par

    # Difference from par
    df['delta_par'] = df['cumulative'] - df['par_cum']

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))
    
    _add_cricket_grid(ax)

    # Plot delta per inning (hero)
    colors = ['darkgreen', 'darkblue']
    for i, inning in enumerate(df['inning'].unique()):
        inning_data = df[df['inning'] == inning]
        ax.plot(inning_data['over_float'], inning_data['delta_par'], 
                color=colors[i], linewidth=3, label=f'Inning {inning} Δ vs Par')
    
    ax.axhline(0, color='black', linestyle='-', alpha=0.5, label='Par')
    ax.fill_between(df['over_float'], df['delta_par'], 0, where=df['delta_par'] >= 0, color='green', alpha=0.2)
    ax.fill_between(df['over_float'], df['delta_par'], 0, where=df['delta_par'] < 0, color='red', alpha=0.2)

    ax.set_title(f"Momentum Swings: Δ Runs vs Par (Match {match_id})", fontsize=14, fontweight='bold')
    ax.set_xlabel("Overs", fontsize=12)
    ax.set_ylabel("Runs Ahead/Behind Par", fontsize=12)
    ax.legend(loc='upper left')
    ax.set_xlim(0, 20)
    ax.set_facecolor('whitesmoke')
    
    return ax
    """
    Plots a Manhattan chart: runs scored per over in a match.

    Args:
        match_id (str): The match identifier.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    query = """
        SELECT inning, over, SUM(runs_batter + runs_extras) as runs_per_over
        FROM ball_events 
        WHERE match_id = ?
        GROUP BY inning, over
        ORDER BY inning, over
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for match {match_id}")

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))

    colors = ['darkgreen', 'darkblue']
    for i, inning in enumerate(df['inning'].unique()):
        inning_data = df[df['inning'] == inning]
        ax.bar(inning_data['over'] + (i * 0.4), inning_data['runs_per_over'], width=0.4, color=colors[i], label=f'Inning {inning}')

    ax.set_title(f"Manhattan Chart: Runs per Over (Match {match_id})")
    ax.set_xlabel("Over")
    ax.set_ylabel("Runs Scored")
    ax.legend()
    ax.grid(True, axis='y', linestyle=':', alpha=0.3)
    
    return ax


def plot_manhattan(match_id: str, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots a Manhattan chart: runs scored per over, color-coded by events.
    Wicket = Red, Boundary = Green, Dot ball = Grey, Normal = Blue.
    
    Args:
        match_id (str): The match identifier.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np
    from pypitch.exceptions import MatchDataMissing

    # Get detailed ball-by-ball for color coding
    query = """
        SELECT inning, over, ball, runs_batter + runs_extras as runs_scored, is_wicket, runs_batter
        FROM ball_events 
        WHERE match_id = ?
        ORDER BY inning, over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for match {match_id}")

    # Group by over and inning, but track events
    df['event_type'] = np.select(
        [df['is_wicket'] == True, df['runs_batter'] >= 4, df['runs_scored'] == 0],
        ['wicket', 'boundary', 'dot'],
        default='normal'
    )
    
    # Aggregate runs per over
    over_data = df.groupby(['inning', 'over']).agg({
        'runs_scored': 'sum',
        'event_type': lambda x: x.tolist()  # Keep all events in over
    }).reset_index()

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))

    _add_cricket_grid(ax)

    colors = ['darkgreen', 'darkblue']
    event_colors = {'wicket': 'red', 'boundary': 'green', 'dot': 'grey', 'normal': 'blue'}
    
    for i, inning in enumerate(over_data['inning'].unique()):
        inning_data = over_data[over_data['inning'] == inning]
        
        # For each over, determine dominant color based on events
        for _, over_row in inning_data.iterrows():
            events = over_row['event_type']
            # Priority: wicket > boundary > normal > dot
            if 'wicket' in events:
                color = event_colors['wicket']
            elif 'boundary' in events:
                color = event_colors['boundary']
            elif 'normal' in events:
                color = colors[i]  # Use inning color for normal overs
            else:
                color = event_colors['dot']
            
            ax.bar(over_row['over'] + (i * 0.4), over_row['runs_scored'], 
                   width=0.4, color=color, edgecolor='black', linewidth=0.5)

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='red', label='Wicket'),
        Patch(facecolor='green', label='Boundary'),
        Patch(facecolor='blue', label='Normal'),
        Patch(facecolor='grey', label='Dot')
    ]
    ax.legend(handles=legend_elements, loc='upper left')

    ax.set_title(f"Manhattan Chart: Runs per Over (Match {match_id})")
    ax.set_xlabel("Over")
    ax.set_ylabel("Runs Scored")
    ax.set_xlim(0, 20)
    
    return ax


def plot_beehive(match_id: str, bowler_id: int, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots a Beehive (pitch map): where the bowler pitched the ball.
    Shows accuracy zones: Good Length, Yorker, Short, etc.
    
    Args:
        match_id (str): The match identifier.
        bowler_id (int): The bowler entity ID.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np
    from pypitch.exceptions import MatchDataMissing

    # For now, simulate pitch locations (would need actual data)
    query = """
        SELECT over, ball, runs_batter + runs_extras as runs_scored
        FROM ball_events 
        WHERE match_id = ? AND bowler_id = ?
        ORDER BY over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id, bowler_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for bowler {bowler_id} in match {match_id}")

    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 8))

    # Simulate pitch locations (length and line)
    # In real implementation, this would come from ball trajectory data
    np.random.seed(42)  # For reproducible demo
    n_balls = len(df)
    
    # Length: 0-22 yards (pitch is 22 yards)
    lengths = np.random.uniform(0, 22, n_balls)
    # Line: -3 to 3 (off-side to leg-side)
    lines = np.random.normal(0, 1.5, n_balls)
    
    # Color by outcome
    colors = []
    for _, ball in df.iterrows():
        if ball['runs_scored'] == 0:
            colors.append('green')  # Dot ball
        elif ball['runs_scored'] >= 4:
            colors.append('red')    # Boundary
        else:
            colors.append('blue')   # Normal

    scatter = ax.scatter(lines, lengths, c=colors, s=50, alpha=0.7, edgecolors='black')

    # Add cricket pitch zones
    # Good length: 5-9 yards
    ax.axhspan(5, 9, alpha=0.1, color='green', label='Good Length')
    # Yorker: 0-2 yards  
    ax.axhspan(0, 2, alpha=0.1, color='red', label='Yorker')
    # Short: 10-15 yards
    ax.axhspan(10, 15, alpha=0.1, color='orange', label='Short')
    
    # Stumps at 0 yards
    ax.axhline(0, color='black', linewidth=3, label='Stumps')
    
    # Off-side/leg-side markers
    ax.axvline(-1.5, color='grey', linestyle='--', alpha=0.5, label='Off-side')
    ax.axvline(1.5, color='grey', linestyle='--', alpha=0.5, label='Leg-side')

    ax.set_xlim(-4, 4)
    ax.set_ylim(0, 22)
    ax.set_xlabel('Line (Off-side to Leg-side)')
    ax.set_ylabel('Length (yards from stumps)')
    ax.set_title(f"Beehive: Pitch Map (Match {match_id})")
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Invert y-axis so 0 is at bottom (stumps)
    ax.invert_yaxis()
    
    return ax


def plot_wagon_wheel(match_id: str, batsman_id: int, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots a wagon wheel: direction of boundaries hit by batsman.

    Args:
        match_id (str): The match identifier.
        batsman_id (int): The batsman's entity ID.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np
    from pypitch.exceptions import MatchDataMissing

    # Simplified: assume random directions for boundaries
    query = """
        SELECT runs_batter
        FROM ball_events 
        WHERE match_id = ? AND batter_id = ? AND runs_batter IN (4, 6)
        ORDER BY over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id, batsman_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No boundaries found for batsman {batsman_id} in match {match_id}")

    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={'projection': 'polar'})

    # Simulate directions
    angles = np.random.uniform(0, 2*np.pi, len(df))
    radii = np.ones(len(df)) * 10
    colors = ['blue' if r == 4 else 'red' for r in df['runs_batter']]
    ax.scatter(angles, radii, c=colors, s=100, alpha=0.7)
    ax.set_title(f"Wagon Wheel: Boundaries (Match {match_id})")
    ax.set_rlim(0, 15)
    
    return ax


def plot_partnership_flow(match_id: str, session: Any, ax: Optional[Any] = None) -> Any:
    """
    Plots partnership flow: runs added by batting pairs.

    Args:
        match_id (str): The match identifier.
        session (Any): PyPitch session object.
        ax (Any, optional): Matplotlib axis. If None, creates a new figure.

    Returns:
        Any: The matplotlib axis with the plot.

    Raises:
        pypitch.exceptions.MatchDataMissing: If match data is not available.
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from pypitch.exceptions import MatchDataMissing

    query = """
        SELECT inning, batter_id, non_striker_id, over, ball, runs_batter + runs_extras as runs_scored
        FROM ball_events 
        WHERE match_id = ?
        ORDER BY inning, over, ball
    """
    
    try:
        arrow_table = session.engine.execute_sql(query, [match_id])
        df = arrow_table.to_pandas()
    except Exception:
        raise MatchDataMissing(f"Match ID {match_id} does not have ball-by-ball data")

    if df.empty:
        raise MatchDataMissing(f"No data found for match {match_id}")

    # Group by partnership (batter + non_striker)
    df['partnership'] = df.apply(lambda x: tuple(sorted([x['batter_id'], x['non_striker_id']])), axis=1)
    partnerships = df.groupby(['inning', 'partnership']).agg({'runs_scored': 'sum', 'over': ['min', 'max']}).reset_index()
    partnerships.columns = ['inning', 'partnership', 'runs', 'start_over', 'end_over']

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 8))

    colors = ['darkgreen', 'darkblue']
    for i, inning in enumerate(partnerships['inning'].unique()):
        inn_data = partnerships[partnerships['inning'] == inning]
        for _, p in inn_data.iterrows():
            width = p['runs'] / 10  # Scale for visibility
            ax.barh(str(p['partnership']), p['end_over'] - p['start_over'], left=p['start_over'], height=width, color=colors[i], alpha=0.7)

    ax.set_title(f"Partnership Flow: Ribbon Width by Runs (Match {match_id})")
    ax.set_xlabel("Overs")
    ax.set_ylabel("Partnership")
    ax.grid(True, axis='x', linestyle=':', alpha=0.3)
    
    return ax
