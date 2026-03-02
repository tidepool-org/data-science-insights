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
    "<54":     ["#8C65D6", "#8C65D6"],
    "54-70":   ["#BB9AE7", "#BB9AE7"],
    "70-180":  ["#76D3A6", "#76D3A6"],
    "180-250": ["#FF8B7C", "#FF8B7C"],
    ">250":    ["#FB5951", "#FB5951"],
}
