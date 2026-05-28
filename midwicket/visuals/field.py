"""
Cricket Field Drawing Utility for custom backgrounds in visualizations.
"""
import plotly.graph_objects as go

def add_cricket_pitch_layout(fig: go.Figure) -> go.Figure:
    # Draw the pitch rectangle (22 yards long)
    fig.add_shape(type="rect", x0=-1.5, y0=0, x1=1.5, y1=20.12, fillcolor="#E3D0A8")
    # Draw the stumps
    fig.add_shape(type="line", x0=-0.11, y0=0, x1=0.11, y1=0, line=dict(color="black", width=5))
    # Add more field elements as needed
    return fig
