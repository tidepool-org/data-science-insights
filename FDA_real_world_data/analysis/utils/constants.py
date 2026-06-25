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

# One canonical shade per glycemic range — Temp Basal and Autobolus bars share it (the two
# bars are distinguished by x-position + axis labels, not by shade). Matches NMA's RANGE_COLORS
# so the two projects' stacked range bars read identically.
COLORS_STACKED_BAR = {
    "<54":     "#E03830",
    "54-70":   "#FF6D5C",
    "70-180":  "#5AC692",
    "180-250": "#AA85DE",
    ">250":    "#7046CC",
}

# Decimal places on stacked-bar segment % labels (figure 8.1c).
STACKED_BAR_PCT_DECIMALS = 1

# =============================================================================
# Inclusion criteria — starting glucose at the time an override is activated.
# Used by Analysis 8-2 (preset glycemic outcomes) and 8-3 (preset parameter
# changes) to restrict to activations that begin in a normoglycemic state.
# =============================================================================
STARTING_GLUCOSE_LOW  = 70    # mg/dL
STARTING_GLUCOSE_HIGH = 180   # mg/dL
