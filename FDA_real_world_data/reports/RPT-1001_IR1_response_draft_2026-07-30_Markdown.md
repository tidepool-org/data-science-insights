# RPT-1001 — Interactive Review Response: Characterization of Configurable Presets (Analysis IR-1)

---

## 1. FDA question (verbatim, interactive review, received 2026-07-30)

> In PLN-1001/RPT-1001, you present real-world data from the Tidepool Big Data Donation Project (TBDDP) analyzing the transition from temporary basal dosing to the autobolus (40% PAF) delivery strategy. You mention throughout the different analyses that data was analyzed comparing temporary basal dosing with presets to autobolus with presets. Please provide a characterization of the configurable presets used in the TBDDP analyses (8.1, 8.2, and 8.4), including: (a) the preset settings used to the extent this information is available in the dataset; (b) the distribution of preset parameter values (mean, standard deviation, and range) for each parameter during both the temporary basal and autobolus periods; and (c) the frequency and duration of preset activations per user during each period. If the known preset settings are the same as what is analyzed in 8.3, please indicate this. Please also clarify if there are other configurable settings beyond basal rate, CR/ISF, and glucose target (mentioned in analysis 8.3) available to users in your to-be-marketed version. This information is needed to confirm that the preset configurations evaluated in the TBDDP analysis are representative of the configurable preset use expected in the intended Tidepool Loop 2.0 user population.

## 2. Summary of response

We characterized every preset activation recorded for eligible transition-cohort users during the temporary basal (TB) and autobolus (AB) analysis windows underlying Analyses 8.1, 8.2, and 8.4 — 2,093 activations by 51 of the 351 eligible transition users (14.5%) — with no filtering beyond the cohort gates those analyses share. In brief:

- (a) The dataset stores five preset parameters — basal-rate, carbohydrate-ratio (CR), and insulin-sensitivity (ISF) scale factors, expressed relative to the user's scheduled therapy settings, plus a glucose-target range (low/high). In practice the presets observed here collapse to a simpler structure: in 100% of factor-bearing activations the CR factor equals the ISF factor and equals the reciprocal of the basal-rate factor — i.e., a single "overall insulin needs" adjustment — optionally combined with a temporary glucose-target range (Section 5.1).
- (b) Per-parameter distributions (N, mean ± SD, min–max, median [IQR]) during the TB window and both AB windows, at the activation grain, are given in Table IR-1a (Section 5.2).
- (c) Activation frequency and duration per user in each window, among users with ≥ 1 preset activation in that window (N = 39 per window), are given in Tables IR-1b and IR-1d (Section 5.3).
- (8.3) The presets characterized here are recorded in the same source records with the same parameter definitions as those analyzed in Analysis 8.3; Analysis 8.3 analyzes a gated subset of these activations (Section 6).
- (TL 2.0) Configurable settings in the to-be-marketed version: [INPUT REQUIRED — Product/Regulatory] (Section 7).

## 3. Data source and cohort

The characterization draws on the same TBDDP extract as RPT-1001 and applies exactly the cohort gates shared by Analyses 8.3 and 8.4: the analysis-cohort predicate (Loop version and age ≥ 6), pump-settings guardrail exclusion, and the confirmed type-1-diabetes restriction. Within each eligible user's selected (rank-1) TB→AB transition segment, every preset activation in the three 14-day analysis windows is characterized — there is no filtering on pairing validity or starting glucose (those gates belong to the inferential analyses; Section 6). Per-user summaries (Section 5.3) are reported among users with ≥ 1 preset activation in the given window; the eligible transition cohort (N = 351) provides the prevalence context.

Primary numbers in this response come from the report's primary (0.80 validity-box) build; the production (0.70) and 0.90 sensitivity builds are available on request.

## 4. Methods

**Analysis windows.** The FDA question asks for "the temporary basal and autobolus periods." To align with the report's paired designs (Tables 8.2b/8.2c), the AB period is reported as two consecutive 14-day windows: the temporary basal window (seg1), the initial autobolus window (seg2, days 0–14 post-transition), and the second autobolus window (seg3, days 14–28). A pooled AB view can be provided if preferred.

**Grain.** Distributions are reported per activation: each activation contributes once, so parameter distributions are weighted by how often each configuration was used. A per-distinct-configuration view (one row per unique user × preset name × exact parameter set per window — the settings in use, unweighted by activation frequency) was also generated and is available on request.

**Durations.** Two duration definitions are reported. The effective duration is the in-window exposure: the programmed duration bounded by the gap to the next override and the end of the analysis window (durations reported elsewhere in RPT-1001 are effective durations). The programmed duration is the as-entered value. An indefinite override (no programmed end) has no programmed duration but still receives a bounded effective duration; this affects 26 of 2,093 characterized activations (1.2%).

**Units.** Glucose values are reported in mg/dL. Some users configure their devices in mmol/L, so converted values are not round numbers in mg/dL (e.g., 108.1 mg/dL = 6.0 mmol/L); observed minima/maxima reflect this.

## 5. Results

### 5.1 (a) Preset settings available in the dataset

The TBDDP records preset (override) events with up to five configurable parameters: three scale factors — basal rate, CR, and ISF — stored relative to scheduled therapy (a value of 0.80 means 80% of the scheduled setting; absolute settings are not part of the preset record), and a temporary glucose-target range (low/high bounds). Not every preset sets every parameter: of the 2,093 characterized activations, 1,695 (81%) carry the complete scale-factor trio (the three factors are always present or absent together), and 1,886 (90%) carry a target range.

Structurally, the observed presets are simpler than the five stored parameters suggest. In 100% of factor-bearing activations the CR factor equals the ISF factor, and in 100% the CR factor equals the reciprocal of the basal-rate factor. That is the signature of a single "overall insulin needs" dial — one setting that scales basal up and strengthens dosing (CR/ISF down) proportionally, or vice versa — rather than three independently tuned parameters. The presets observed in these analyses therefore reduce to: an overall insulin-needs adjustment, a temporary glucose-target range, or both.

### 5.2 (b) Distribution of preset parameter values

Table IR-1a reports each stored parameter (plus the derived target midpoint) by window: N (activations carrying the parameter), N users, mean ± SD, min–max (the requested range), and median [IQR].

**Table IR-1a — preset parameter distributions, per activation**

| Period | Parameter | N | N users | Mean ± SD | Min–Max | Median [IQR] |
|---|---|---|---|---|---|---|
| Temp basal period | Basal rate scale factor | 571 | 33 | 0.801 ± 0.423 | 0.100–2.000 | 0.800 [0.565, 1.200] |
| Temp basal period | Carb ratio scale factor | 571 | 33 | 2.411 ± 2.947 | 0.500–10.000 | 1.250 [0.833, 1.777] |
| Temp basal period | Insulin sensitivity scale factor | 571 | 33 | 2.411 ± 2.947 | 0.500–10.000 | 1.250 [0.833, 1.777] |
| Temp basal period | Glucose target low (mg/dL) | 654 | 35 | 117.8 ± 22.6 | 67.0–180.0 | 120.0 [108.1, 130.0] |
| Temp basal period | Glucose target high (mg/dL) | 654 | 35 | 129.8 ± 28.1 | 70.0–225.0 | 130.0 [108.1, 150.0] |
| Temp basal period | Glucose target midpoint (mg/dL) | 654 | 35 | 123.8 ± 24.4 | 70.0–202.5 | 127.5 [108.1, 137.5] |
| Initial autobolus period (days 0–14) | Basal rate scale factor | 539 | 37 | 0.757 ± 0.422 | 0.100–2.000 | 0.700 [0.500, 1.125] |
| Initial autobolus period (days 0–14) | Carb ratio scale factor | 539 | 37 | 2.586 ± 3.005 | 0.500–10.000 | 1.429 [0.889, 2.000] |
| Initial autobolus period (days 0–14) | Insulin sensitivity scale factor | 539 | 37 | 2.586 ± 3.005 | 0.500–10.000 | 1.429 [0.889, 2.000] |
| Initial autobolus period (days 0–14) | Glucose target low (mg/dL) | 599 | 35 | 116.1 ± 21.3 | 39.6–180.0 | 115.0 [108.1, 130.0] |
| Initial autobolus period (days 0–14) | Glucose target high (mg/dL) | 599 | 35 | 129.2 ± 28.4 | 70.0–225.0 | 130.0 [108.1, 140.0] |
| Initial autobolus period (days 0–14) | Glucose target midpoint (mg/dL) | 599 | 35 | 122.7 ± 23.4 | 70.0–202.5 | 120.0 [108.1, 135.1] |
| Second autobolus period (days 14–28) | Basal rate scale factor | 585 | 35 | 0.758 ± 0.432 | 0.100–2.000 | 0.700 [0.500, 1.200] |
| Second autobolus period (days 14–28) | Carb ratio scale factor | 585 | 35 | 2.537 ± 2.902 | 0.500–10.000 | 1.429 [0.833, 2.000] |
| Second autobolus period (days 14–28) | Insulin sensitivity scale factor | 585 | 35 | 2.537 ± 2.902 | 0.500–10.000 | 1.429 [0.833, 2.000] |
| Second autobolus period (days 14–28) | Glucose target low (mg/dL) | 633 | 37 | 115.9 ± 19.4 | 63.1–180.0 | 110.0 [108.1, 130.0] |
| Second autobolus period (days 14–28) | Glucose target high (mg/dL) | 633 | 37 | 129.3 ± 26.7 | 70.0–225.0 | 120.0 [108.1, 140.0] |
| Second autobolus period (days 14–28) | Glucose target midpoint (mg/dL) | 633 | 37 | 122.6 ± 21.8 | 70.0–202.5 | 117.1 [108.1, 135.0] |

Reading notes: (i) the CR and ISF rows are identical by construction (100% tie, Section 5.1); (ii) scale-factor and target Ns differ within a window because presets need not set both parameter groups; (iii) distributions are stable across the TB and both AB windows — users did not materially reconfigure presets around the transition; (iv) the glucose-target-low minimum of 39.6 mg/dL (= 2.2 mmol/L) in the initial AB window reflects a user-entered value in the DIY system, which imposes no target guardrails. **[TO CONFIRM: verify this configuration in the source records and agree on how to address it — likely a footnote noting DIY Loop's unconstrained targets vs. the bounded configuration ranges of the to-be-marketed system.]**

### 5.3 (c) Frequency and duration of preset activations per user, per window

**Table IR-1b — activation-level durations**

| Period | Outcome | N | N users | Mean ± SD | Min–Max | Median [IQR] |
|---|---|---|---|---|---|---|
| Temp basal period | Effective duration (hours) | 713 | 39 | 2.8 ± 17.1 | 0.0–321.3 | 1.0 [0.4, 2.0] |
| Temp basal period | Programmed duration (hours) | 704 | 39 | 1.9 ± 2.9 | 0.0–26.2 | 1.0 [0.4, 2.0] |
| Initial autobolus period (days 0–14) | Effective duration (hours) | 679 | 39 | 2.4 ± 7.5 | 0.0–121.5 | 1.1 [0.4, 2.0] |
| Initial autobolus period (days 0–14) | Programmed duration (hours) | 672 | 39 | 2.1 ± 4.9 | 0.0–91.9 | 1.1 [0.4, 2.0] |
| Second autobolus period (days 14–28) | Effective duration (hours) | 701 | 39 | 2.6 ± 7.6 | 0.0–148.0 | 1.2 [0.5, 2.2] |
| Second autobolus period (days 14–28) | Programmed duration (hours) | 691 | 39 | 2.3 ± 4.6 | 0.0–68.8 | 1.2 [0.5, 2.3] |

Activation-level durations are short and right-skewed: median 1.0–1.2 h [IQR ≈ 0.4–2.2] in every window. Effective duration can exceed programmed duration in aggregate because indefinite overrides (no programmed end) appear only in the effective row, where they are bounded by the next override or the window end (up to ~14 days); the N gap between the effective and programmed rows in each period is exactly that indefinite count (9 + 7 + 10 = 26 activations across the three windows).

**Table IR-1d — per-user frequency and preset time, among preset users (N = 39 per window)**

| Period | Outcome | N | N users | Mean ± SD | Min–Max | Median [IQR] |
|---|---|---|---|---|---|---|
| Temp basal period | Preset activations per user (n/14 days) | 39 | 39 | 18.3 ± 21.3 | 1.0–82.0 | 6.0 [3.0, 24.5] |
| Temp basal period | Total preset time per user (hours/14 days) | 39 | 39 | 52.1 ± 76.1 | 0.0–325.0 | 20.2 [7.1, 69.6] |
| Temp basal period | Mean duration per activation (hours) | 39 | 39 | 10.4 ± 35.7 | 0.0–162.5 | 1.6 [1.0, 3.3] |
| Initial autobolus period (days 0–14) | Preset activations per user (n/14 days) | 39 | 39 | 17.4 ± 19.9 | 1.0–80.0 | 9.0 [5.0, 21.5] |
| Initial autobolus period (days 0–14) | Total preset time per user (hours/14 days) | 39 | 39 | 41.6 ± 54.3 | 1.0–261.5 | 19.5 [7.1, 45.0] |
| Initial autobolus period (days 0–14) | Mean duration per activation (hours) | 39 | 39 | 3.6 ± 6.1 | 0.1–32.7 | 1.5 [1.1, 3.1] |
| Second autobolus period (days 14–28) | Preset activations per user (n/14 days) | 39 | 39 | 18.0 ± 24.6 | 1.0–111.0 | 10.0 [3.0, 18.0] |
| Second autobolus period (days 14–28) | Total preset time per user (hours/14 days) | 39 | 39 | 47.6 ± 56.1 | 0.0–212.7 | 22.1 [6.5, 73.4] |
| Second autobolus period (days 14–28) | Mean duration per activation (hours) | 39 | 39 | 3.9 ± 4.4 | 0.0–21.8 | 2.2 [1.3, 4.1] |

Among users who activated any preset in a window ("the average preset user"), the distribution is strongly right-skewed, so the median is the representative summary. A typical preset user activated presets a median of 6–10 times per 14-day window — the mean (17–18) is inflated by a minority of heavy users — for a median total of roughly 20 h of preset-active time (mean 42–52 h), with individual activations lasting a median of 1.5–2.2 h. Preset use is nonetheless a minority behavior in the analysis cohorts: 39 of the 351 eligible transition users (11.1%) activated a preset in any given window, and 51 (14.5%) in at least one of the three (the per-window count is 39 in each of the three windows; the 51 is the union across windows).


## 6. Relationship to Analysis 8.3

The presets characterized here are the same as those analyzed in Analysis 8.3, with one clarification of scope. Both come from the same source records with the same parameter definitions (the same five stored parameters and relative-to-schedule semantics). Analysis 8.3 analyzes a gated subset of these activations: it compares parameter values across the transition for preset pairs matched by name between the TB and initial AB windows, restricted to pairs passing validity and starting-glucose gates, and it excludes the second AB window (days 14–28). This characterization applies no such gates, so it is the superset view of preset use underlying Analyses 8.1, 8.2, and 8.4.

## 7. Configurable settings in the to-be-marketed version (TL 2.0)

**[INPUT REQUIRED — Product/Regulatory.]** State whether Tidepool Loop 2.0 presets expose any configurable settings beyond the basal-rate / CR-ISF scale factors and glucose-target range analyzed in 8.3, and describe the configuration bounds (guardrails) the marketed system applies. The TL 2.0 preset feature specification is not derivable from the analysis dataset, so this section must come from the product side. Note the interaction with Section 5.2's reading note (iv): bounded TL 2.0 target ranges would directly address the unconstrained DIY values observed in the dataset.

## 8. Representativeness of preset use in the TBDDP analyses

FDA's underlying question is whether the preset configurations evaluated in the TBDDP analyses are representative of configurable preset use expected in the intended TL 2.0 population. The characterization supports this in two respects. First, the observed presets exercise exactly the parameter space the marketed feature exposes — insulin-needs scaling and temporary glucose targets — with wide coverage of that space (basal scale factors 0.10–2.00; entered target bounds spanning 39.6–225.0 mg/dL — see Section 5.2, reading note (iv)). Second, preset use was a minority behavior (11.1% of users in any given window) of short typical duration, and Analysis 8.1 does not condition on preset use — preset-active time is background exposure within its 14-day windows (46 of the 322 users in the final 8.1 cohort, 14.3%, activated any preset in their analyzed window) — so the comparative TB-vs-AB findings are not driven by preset configuration. **[INPUT REQUIRED: final sentence tying observed ranges to TL 2.0's guardrailed configuration bounds, pending Section 7.]**

## 9. Caveats to preserve

- Scale factors are stored relative to scheduled therapy; absolute basal rates, CRs, and ISFs in effect during activations are not part of the preset record.
- Durations reported elsewhere in RPT-1001 are effective (in-window exposure); Table IR-1b reports programmed durations alongside for this response.
- Per-user frequency/duration summaries (Table IR-1d) condition on ≥ 1 activation in the window (N = 39). Table 8.4a of RPT-1001 reports the same quantities zero-filled over the full cohort (N = 351; Table IR-1c here); the presentations reconcile via the 11.1% prevalence.
- Analyses 8.1/8.2/8.4 relate to presets differently: 8.1 does not condition on presets (background exposure); 8.2 analyzes glycemic outcomes during gated activations; 8.4 analyzes per-user usage. This characterization is the ungated superset for all three.
- Preset names are user-entered free text and were reviewed but are not reported in this response (every individual name is used by fewer than 5 users).
- One transition segment (rank-1) per user, consistent with the report's one-observation-per-user design.
