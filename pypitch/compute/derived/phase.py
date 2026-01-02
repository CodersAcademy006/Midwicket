import pyarrow as pa
import pyarrow.compute as pc

def build_phase_stats(ball_events: pa.Table) -> pa.Table:
    """
    Transforms Raw Events -> Aggregated Phase Stats.
    
    Input Schema: BALL_EVENT_SCHEMA (v1)
    Output Schema: 
        batter_id (int32)
        phase (string)
        runs (int64)
        balls (int64)
        outs (int64)
    """
    # 1. Group By (Batter + Phase)
    # PyArrow's native grouping is incredibly fast
    aggregated = ball_events.group_by(['batter_id', 'phase']).aggregate([
        ('runs_batter', 'sum'),
        ('ball', 'count'),      # Count rows = balls faced
        ('is_wicket', 'sum')    # Sum booleans = count of outs
    ])

    # 2. Rename Columns for Clarity
    # The aggregation output usually names them "runs_batter_sum", etc.
    final_table = aggregated.select(['batter_id', 'phase', 'runs_batter_sum', 'ball_count', 'is_wicket_sum'])
    
    final_table = final_table.rename_columns([
        'batter_id', 'phase', 'runs', 'balls', 'outs'
    ])

    return final_table