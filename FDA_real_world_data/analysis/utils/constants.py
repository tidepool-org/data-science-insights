"""
Shared constants for FDA 510(k) RWD analysis scripts.
"""

# =============================================================================
# Font sizes — centralized so every figure stays consistent
# =============================================================================
FONT = {
    "suptitle":   18,
    "title":      15,
    "axis_label": 14,
    "tick":       12,
    "legend":     11,
    "annotation": 13,
}

# =============================================================================
# Color scheme
# =============================================================================
COLORS_PRIMARY   = "#607cff"
COLORS_SECONDARY = "#4f59be"
COLORS_ACCENT    = "#241144"

COLORS_STACKED_BAR = {
    # [temp_basal, autobolus] — lighter shade for TB, darker for AB
    "<54":     ["#FC7A74", "#E03830"],
    "54-70":   ["#FFA99D", "#FF6D5C"],
    "70-180":  ["#92E0BA", "#5AC692"],
    "180-250": ["#CCAFF0", "#AA85DE"],
    ">250":    ["#A384E0", "#7046CC"],
}
