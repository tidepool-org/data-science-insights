"""Validate LoopFullForecaster against the Swift LoopAlgorithm port (LoopAlgorithmToPython) on synthetic cases.

Run in the swift environment (the dylib is macOS/arm64):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-swift/bin/python tests/validate_loop_full_against_port.py

Expected (2026-09-04, port pinned to LoopAlgorithm d4c674f): every case WITHOUT carbs matches to the decimal at
every horizon (momentum, retrospective correction and their blend). Cases WITH carbs differ because the port
uses Loop's dynamic carb absorption (observed absorption slows or speeds the modelled curve), which
LoopFullForecaster does not implement -- carbs absorb on the static curve.
"""
