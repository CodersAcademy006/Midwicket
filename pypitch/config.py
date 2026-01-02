"""
Global configuration and debug mode for PyPitch.
"""

debug = False

def set_debug(value: bool = True):
    """
    Set debug mode. If True, forces eager execution and verbose errors.
    """
    global debug
    debug = value
    if debug:
        print("[PyPitch] Debug mode ON: Forcing eager execution and verbose errors.")
    else:
        print("[PyPitch] Debug mode OFF.")

def is_debug():
    return debug
