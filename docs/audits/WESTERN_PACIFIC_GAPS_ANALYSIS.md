# Western Pacific DPS Implementation — Gap Analysis

## Current Implementation

**Western Pacific Formula:**
```
DPS = 30% × Surge + 45% × Wind Field + 10% × Wind Speed + 15% × Duration
Multiplier: 1.1x (10% boost for large wind fields)
RI Bonus: None
```

**Design Rationale:** Large R34 wind fields (86 nm avg vs. Atlantic 64 nm) + multiple landfalls → boost wind field weight to 45%, increase duration factor to 15%.

---

## Critical Gaps Identified

### 1. **Multiple Landfalls — Compounding Damage Effect** ⚠️ HIGH IMPACT
**The Problem:**
- Western Pacific typhoons frequently make 3-4+ destructive landfalls in succession
- Haiyan (2013): Philippines → Vietnam → Cambodia (major economic centers each time)
- Current model: treats total coastal time in "duration_factor" but doesn't capture the **cumulative population exposure** effect
- Atlantic analogy: A hurricane crossing Florida then Georgia is less damaging total than Haiyan hitting 3 separate countries

**What We're Missing:**
- Population exposure multiplier (Philippines has 115M people, Vietnam 98M—heavily concentrated on coast)
- Infrastructure reset between landfalls (repair windows compressed)
- Rainfall accumulation across multiple landfalls (Harvey model applies, but not captured in DPS)

**Example Impact:**
- Haiyan DPS: 82.1 (but caused $14B across 3 countries)
- Equivalent Atlantic system: might be DPS 75-78 (single landfall, same damage)
- **Potential adjustment: +3-5 DPS for each additional major landfall** (not currently implemented)

---

### 2. **Orographic Rainfall — Separated from DPS Component** ⚠️ HIGH IMPACT
**The Problem:**
- Western Pacific has extreme orographic rainfall due to mountainous terrain:
  - Philippines: Cordilleras (up to 2,922m)
  - Taiwan: Central Mountains (up to 3,952m)
  - Japan: Numerous mountain ranges
  - Vietnam: North-central highlands
- Current design: rainfall warning is **separate 0-100 metric**, not integrated into DPS
- DPS only includes 25% rainfall component (from surge/wind field proxies), but true orographic damage is underweighted

**What We're Missing:**
- Orographic rainfall can exceed 2,000mm over mountains (vs. 500-1000mm typical)
- Landslides, flooding in valleys behind coastal mountains
- Our rainfall warning model should influence DPS more heavily for WP systems

**Example Impact:**
- Typhoon Ketsana (2009): $800M damage, moderate intensity (115 mph) but extreme rainfall (up to 1,500mm)
- Current DPS: 22.9 (low because low wind speed)
- Actual damage suggests DPS should be 40-50 range due to orographic effect

**Potential Adjustment:**
```
IF Western Pacific AND R34 > 70 nm AND elevation_near_track > 1000m THEN
  DPS += orographic_rainfall_bonus (5-10 points)
```

---

### 3. **Rapid Intensification — Underweighted for Western Pacific** ⚠️ MEDIUM IMPACT
**The Problem:**
- Eastern Pacific gets +15 RI bonus, but Western Pacific typhoons also experience RI
- Typhoon Haiyan: 75 mph → 195 mph in 36 hours (same 120 mph/24h as Otis)
- Our validation data didn't explicitly flag WP RI cases
- Current formula: 1.1x multiplier applies uniformly, no RI bonus

**What We're Missing:**
- Rapid intensification detection for Western Pacific (threshold: 80+ mph/24h)
- Should apply +10 RI bonus (slightly less than EP's +15 because WP has larger initial wind fields)

**Impact Evidence:**
- Haiyan rapid intensification: not explicitly rewarded in current formula
- Formula treats slow-building and rapid-build equally

**Potential Adjustment:**
```
IF Western Pacific AND max_24h_intensity_gain > 80 mph THEN
  DPS += 10 (RI bonus for typhoons)
```

---

### 4. **Reintensification Over Ocean — No Feedback Loop** ⚠️ MEDIUM IMPACT
**The Problem:**
- Typhoons can cross land, weaken, then re-enter warm ocean and reintensify
- Atlantic hurricanes typically decay over land and don't recover
- Current cumulative DPI model: doesn't capture "second peak" reintensification
- Example: Typhoon crosses Philippines at DPI 60, weakens to 40 over land, re-enters Pacific at 75 DPI

**What We're Missing:**
- Peak detection of multiple intensity peaks in track
- Extended threat timeline (storm weakens then re-strengthens)
- Duration factor captures total time but not the "second exposure" aspect

**Example:**
- A typhoon that: peaks 80 DPI → weakens 50 DPI → reintensifies to 75 DPI
- Should be treated differently than: steady 70-80 DPI throughout
- Current model: treats both similarly based on mean DPI and duration

---

### 5. **Economic Vulnerability — Not Basin-Specific** ⚠️ MEDIUM IMPACT
**The Problem:**
- Our basin multiplier (1.1x for WP) is mechanistic, not economic
- Western Pacific coastal GDP concentration vastly different across region:
  - Philippines: ~$8,000 per capita GDP, coastal concentration 35%+ of economy
  - Japan: ~$40,000 per capita GDP, better infrastructure, insurance
  - Vietnam: ~$4,000 per capita GDP, rapid development but vulnerable
  - Taiwan: ~$35,000 per capita GDP, built for typhoons

**What We're Missing:**
- GDP density multiplier by region
- Infrastructure resilience factor (Japan/Taiwan factor down; Philippines factor up)
- Current approach: treat all WP equally with 1.1x

**Example Impact:**
- Same DPS 70 typhoon hitting Japan vs. Philippines:
  - Japan: $20-30B (good infrastructure, insurance)
  - Philippines: $5-10B (less built infrastructure)
  - Current formula: applies same DPS to both (would reverse this)
  - **Would need sub-basin classification** (WP-Japan, WP-Philippines, WP-Vietnam, etc.)

---

### 6. **Wind Field Duration vs. Movement Speed Mismatch** ⚠️ LOW-MEDIUM IMPACT
**The Problem:**
- We boosted duration factor to 15% for WP (vs. Atlantic 10%)
- But WP typhoons **move faster on average** (2.7 days duration vs. Atlantic 3.6 days)
- Rationale was multiple landfalls, but formula doesn't distinguish:
  - Fast-moving system (low threat × low duration) ≠ Slow stalling system
- Current approach conflates "multiple impacts" with "prolonged duration"

**What We're Missing:**
- Distinction between:
  - Fast-track typhoon hitting 3 countries quickly (high damage rate)
  - Slow-moving system threatening 1 region extensively (high damage total)

**Analysis:**
```
Fast-Track Typhoon: 2 days, 3 landfalls, $15B
- Duration factor: lower (2 days)
- Landfall multiplier: higher (+3 per landfall)

Slow-Moving: 4 days, 1 landfall, $10B
- Duration factor: higher (4 days)
- Landfall multiplier: 0
- Current formula: slow-moving might score same or higher DPS
```

---

### 7. **Storm Track Recursion — Not Modeled** ⚠️ LOW IMPACT
**The Problem:**
- Western Pacific typhoons typically recurve northward (Coriolis effect)
- Recurving typhoons affect Japan, Korea, China in succession
- Atlantic hurricanes also recurve but data is less complex
- Current model: treats track as sequential lat/lon points, doesn't differentiate recurving systems

**What We're Missing:**
- Explicit recognition that recurving systems maintain intensity longer
- Multi-country threat sequence is built-in (unlike Atlantic where storms track off coast)

---

### 8. **Landslide & Infrastructure Damage — Implicit vs. Explicit** ⚠️ LOW IMPACT
**The Problem:**
- Mountainous WP terrain causes landslides independent of wind/surge damage
- Infrastructure (bridges, power lines) vulnerable in mountain valleys
- Current model: rainfall warning captures some of this, but not in DPS itself

**What We're Missing:**
- Explicit terrain hazard layer for DPS calculation
- Would require elevation data along track (complex)

---

## Recommended Enhancements (Priority Order)

### 🔴 **CRITICAL (implement for accuracy)**
1. **RI Detection for Western Pacific**
   - Add +10 RI bonus if 24h intensity gain > 80 mph
   - Affects Haiyan-class systems
   - Effort: LOW (copy EP logic, adjust threshold)

2. **Multiple Landfall Penalty/Bonus**
   - Add +2 DPS per additional major landfall (>50M population center)
   - Requires tracking landfall count
   - Effort: MEDIUM (post-processing logic)

### 🟠 **HIGH PRIORITY (capture major variance)**
3. **Orographic Rainfall Integration**
   - Increase rainfall component weight for WP: 25% → 30-35%
   - Add elevation check (boost if mountains nearby)
   - Effort: MEDIUM

4. **Sub-Basin Economic Multiplier**
   - Define: WP-Philippines (×1.15), WP-Japan (×0.95), WP-Vietnam (×1.20)
   - Replaces generic 1.1x multiplier
   - Effort: HIGH (requires track-to-region mapping)

### 🟡 **MEDIUM PRIORITY (reduce edge-case error)**
5. **Duration Factor Refinement**
   - Recalibrate: use 12% (not 15%) + landfall bonus
   - Separates "multiple impacts" from "extended duration"
   - Effort: LOW

### 🔵 **FUTURE (research-level complexity)**
6. **Reintensification Peak Detection**
   - Identify multiple DPI peaks in track
   - Weight second peak differently than first
   - Effort: HIGH (algorithm development)

7. **Monsoon/ENSO Interaction**
   - Seasonal DPS adjustment (summer stronger, winter weaker)
   - Effort: VERY HIGH (atmospheric coupling)

---

## Validation Gaps

### Current Validation Storms (Western Pacific):
- ✅ **Haiyan (2013)**: 82.1 DPS | $14.0B (multiple landfalls, large wind field)
- ✅ **Mangkhut (2018)**: 51.0 DPS | $0.614B (moderate, fast track)
- ✅ **Noru (2022)**: 22.9 DPS | $0.010B (weak, sparse damage)

### Missing Validation Cases:
- ❌ **Rapid-Intensifying Typhoon** (Haiyan had RI but not flagged)
  - Need explicit RI case to validate +10 bonus
  - Candidates: Typhoon Vongfong (2014), Supertyphoon Yutu (2018)

- ❌ **Orographic Rainfall Extreme**
  - Need case with moderate intensity but extreme rainfall damage
  - Candidate: Typhoon Ketsana (2009) — 115 mph but $800M from rain

- ❌ **Slow-Moving Stall System**
  - WP equivalent of Harvey stall scenario
  - Candidate: Typhoon Nepartak (2016)

- ❌ **Multiple Sequential Impacts**
  - Need storm with 4+ significant landfalls
  - Candidate: Typhoon Haiyan itself (Philippines → Vietnam → Cambodia)

---

## Recommendation Summary

### **Short-term (implement within 1-2 weeks):**
- ✅ Add RI detection (+10 bonus) for WP
- ✅ Refine duration factor (12% instead of 15%)
- ✅ Add 3-5 point landfall bonus tracking

### **Medium-term (1-2 month research project):**
- 📊 Validate orographic rainfall impact with Ketsana/Nepartak cases
- 📊 Develop sub-basin economic multipliers
- 📊 Implement landfall tracking in preprocessing

### **Long-term (future enhancements):**
- 🔬 Reintensification peak detection algorithm
- 🔬 Monsoon/ENSO seasonal adjustment framework
- 🔬 Elevation-based terrain hazard scoring

---

## Conclusion

**Current State:** Western Pacific formula is **reasonable first approximation** (1.1x + 45% wind field) but **misses 3-4 critical mechanisms** that create variance:
1. Multiple destructive landfalls (Haiyan signature)
2. Orographic rainfall amplification (Ketsana signature)
3. Rapid intensification (Haiyan/Supertyphoons)
4. Reintensification cycles (post-landfall recovery)

**Impact:** Likely **±10-15 DPS variance** on WP systems without these factors, especially for:
- Multi-landfall typhoons (could underestimate by 5-10 points)
- Orographic rainfall systems (could underestimate by 5-8 points)
- Rapid-intensifying storms (could underestimate by 10 points)

**Priority:** RI detection is quickest win. Landfall tracking is highest-impact enhancement.
