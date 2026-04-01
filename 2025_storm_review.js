const fs = require("fs");
const { Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
        Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
        ShadingType, PageNumber, PageBreak, LevelFormat } = require("docx");

const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };
const cellMargins = { top: 60, bottom: 60, left: 100, right: 100 };

function cell(text, opts = {}) {
  const width = opts.width || 1000;
  const bold = opts.bold || false;
  const fill = opts.fill || undefined;
  const align = opts.align || AlignmentType.LEFT;
  const fontSize = opts.fontSize || 18;
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    margins: cellMargins,
    shading: fill ? { fill, type: ShadingType.CLEAR } : undefined,
    verticalAlign: "center",
    children: [new Paragraph({ alignment: align, children: [new TextRun({ text: String(text), bold, font: "Arial", size: fontSize })] })]
  });
}

function heading(text, level) {
  return new Paragraph({ heading: level, spacing: { before: 300, after: 150 },
    children: [new TextRun({ text, font: "Arial", bold: true })] });
}

function para(text, opts = {}) {
  return new Paragraph({ spacing: { after: 120 }, alignment: opts.align || AlignmentType.LEFT,
    children: [new TextRun({ text, font: "Arial", size: 20, ...opts })] });
}

function boldPara(label, text) {
  return new Paragraph({ spacing: { after: 100 },
    children: [
      new TextRun({ text: label, font: "Arial", size: 20, bold: true }),
      new TextRun({ text, font: "Arial", size: 20 })
    ]
  });
}

// Storm data table
const stormCols = [1600, 600, 700, 700, 700, 700, 600, 700, 700, 700, 1860];
const stormColTotal = stormCols.reduce((a,b)=>a+b, 0);

const stormHeaders = ["Storm", "Cat", "Wind", "Press", "R34", "R64", "Fwd", "DPS", "IAS", "ERS*", "Region"];
const stormData = [
  ["Melissa (peak)", "5", "165kt", "892", "170nm", "25nm", "6kt", "51", "57", "~23", "Greater Antilles"],
  ["Melissa (Jamaica LF)", "5", "160kt", "897", "170nm", "25nm", "8kt", "49", "48", "~23", "Greater Antilles"],
  ["Erin (peak)", "5", "140kt", "913", "490nm", "160nm", "8kt", "78", "44", "~5", "Open Atlantic"],
  ["Humberto (peak)", "5", "140kt", "918", "290nm", "65nm", "5kt", "79", "55", "~5", "Open Atlantic"],
  ["Gabrielle (peak)", "4", "120kt", "944", "180nm", "60nm", "7kt", "68", "42", "~5", "Open Atlantic"],
  ["Imelda (peak)", "2", "80kt", "966", "435nm", "90nm", "7kt", "61", "33", "~5", "Open Atlantic"],
  ["Dexter (peak)", "1", "70kt", "988", "210nm", "50nm", "6kt", "44", "32", "~5", "Open Atlantic"],
];

// Issue summary table
const issueCols = [500, 1200, 4660, 3000];
const issueColTotal = issueCols.reduce((a,b)=>a+b, 0);

const issues = [
  ["1", "DPS", "Comment/code weight mismatch: header says 45/35/10/10 but code uses 40/40/10/10", "Update comment or adjust weights"],
  ["2", "DPS", "Wf cap at 1.5 truncates oversized storms. Erin R64=160nm scores same as Sandy R64=80nm", "Raise cap to 3.0 or use log scale"],
  ["3", "DPS", "Open-ocean storms score high (Dexter DPS=44 at 43.6N, -45W with nothing to destroy)", "Add land-proximity dampener or rename to raw potential"],
  ["4", "DPS/IAS", "Greater Antilles shelf=0.50 is too coarse for Jamaica. Raw surge overestimates at 30ft vs 9ft observed", "Split Jamaica as sub-region; tune size multiplier"],
  ["5", "IAS", "Hard rainfall cutoff at 12 kt forward speed. Storms at 13 kt still produce enormous rain", "Use gradual taper (e.g., decay to zero at 18-20 kt)"],
  ["6", "IAS", "Near-land multiplier triggers in open ocean (shelf > 0.35 in Caribbean basin)", "Use actual distance-to-coast check instead of shelf proxy"],
  ["7", "IAS", "No forward speed data = zero rainfall score. HURDAT2 storms often lack this field", "Estimate forward speed from consecutive lat/lon positions"],
  ["8", "IAS", "Surge component is identical to DPS surge component, creating redundancy", "Consider unique IAS surge metric (e.g., bathymetric funneling)"],
  ["9", "ERS", "Size component caps at sqrt(R34/250)=1.0. Erin R34=490nm same as Sandy R34=400nm", "Raise normalization constant or remove cap"],
  ["10", "ERS", "Zone bounding box gaps. Storm 100nm offshore may fall outside all zones", "Use nearest-zone fallback or distance-weighted lookup"],
  ["11", "ERS", "Single vulnerability value per zone. Manhattan and rural Long Island share vuln=0.85", "Already mitigated by NRI mode for active storms"],
  ["12", "ALL", "Cumulative score double-counts surge (DPS surge + IAS surge are identical)", "Weight cumulative as DPS + IAS_rainfall_only + ERS, or rescale"],
  ["13", "ALL", "No orographic rainfall component. Mountain ranges amplify rainfall enormously", "Add terrain factor for known mountain ranges near coast"],
  ["14", "IAS", "No consideration of antecedent conditions (soil saturation, prior storms)", "Out of scope for current model; note as limitation"],
  ["15", "DPS", "Parametric surge model overestimates for compact intense storms in narrow-shelf areas", "Add RMW-based correction; narrow eyewall = less surge push"],
];

const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 20 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, font: "Arial", color: "1B3A5C" },
        paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, font: "Arial", color: "2E5984" },
        paragraph: { spacing: { before: 240, after: 150 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 22, bold: true, font: "Arial", color: "3B6FA0" },
        paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 2 } },
    ]
  },
  numbering: {
    config: [
      { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ]
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 }
      }
    },
    headers: {
      default: new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT,
        children: [new TextRun({ text: "Hurricane IKE Visualizer \u2014 2025 Storm Formula Review", font: "Arial", size: 16, color: "888888", italics: true })] })] })
    },
    footers: {
      default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER,
        children: [new TextRun({ text: "Page ", font: "Arial", size: 16, color: "888888" }), new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 16, color: "888888" })] })] })
    },
    children: [
      // Title
      new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 80 },
        children: [new TextRun({ text: "2025 Atlantic Hurricane Season", font: "Arial", size: 40, bold: true, color: "1B3A5C" })] }),
      new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 40 },
        children: [new TextRun({ text: "Formula Review & Cross-Reference Analysis", font: "Arial", size: 28, color: "2E5984" })] }),
      new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 300 },
        children: [new TextRun({ text: "March 21, 2026", font: "Arial", size: 20, color: "666666" })] }),
      
      new Paragraph({ spacing: { after: 40 }, border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "2E5984", space: 1 } }, children: [] }),
      
      // Executive Summary
      heading("Executive Summary", HeadingLevel.HEADING_1),
      para("The 2025 Atlantic season produced 13 named storms, 5 hurricanes, and 4 major hurricanes, with three reaching Category 5 strength (Erin, Humberto, Melissa). Total season damage was estimated at $55\u201361 billion (AccuWeather), with Hurricane Melissa alone accounting for $8.8 billion in Jamaica. Notably, no hurricanes made landfall on the continental United States."),
      para("This review computes DPS, IAS, and ERS scores for all significant 2025 Atlantic storms, cross-references them against observed damage and meteorological data from NOAA, the World Bank, and secondary sources, and identifies 15 specific shortcomings across the three scoring formulas."),

      new Paragraph({ children: [new PageBreak()] }),

      // Season Overview
      heading("2025 Season Overview", HeadingLevel.HEADING_1),
      para("Key statistics from NOAA and AccuWeather: 13 named storms, 5 hurricanes (4 major), 3 Category 5 storms (tied for second-most on record). ACE index reached 133, about 7% above the 1991\u20132020 average. For the first time in a decade, no hurricanes struck the continental U.S. The season was characterized by extremely high major hurricane conversion rate (80% of hurricanes became majors)."),
      
      heading("Major Storm Summary Table", HeadingLevel.HEADING_2),
      para("Calculated DPS, IAS, and ERS scores at peak intensity for each significant 2025 Atlantic storm. ERS values marked with asterisk (*) are approximate\u2014most 2025 storms peaked over open ocean where ERS is appropriately minimal."),
      
      // Storm table
      new Table({
        width: { size: stormColTotal, type: WidthType.DXA },
        columnWidths: stormCols,
        rows: [
          new TableRow({ children: stormHeaders.map((h, i) => cell(h, { width: stormCols[i], bold: true, fill: "1B3A5C", fontSize: 16 })) }),
          ...stormData.map((row, ri) => new TableRow({
            children: row.map((c, ci) => cell(c, { width: stormCols[ci], fontSize: 16, fill: ri % 2 === 0 ? "F0F4F8" : undefined,
              align: ci >= 1 && ci <= 9 ? AlignmentType.CENTER : AlignmentType.LEFT }))
          }))
        ]
      }),
      
      // Fix header row text color
      para(""),

      new Paragraph({ children: [new PageBreak()] }),

      // Individual Storm Analysis
      heading("Individual Storm Analysis", HeadingLevel.HEADING_1),
      
      // Melissa
      heading("Hurricane Melissa \u2014 Cat 5 (DPS 51, IAS 57)", HeadingLevel.HEADING_2),
      boldPara("Track: ", "Formed Oct 21 east of Lesser Antilles, tracked west through Caribbean, made landfall in Jamaica Oct 28 at Cat 5 (160 kt), crossed Jamaica, weakened over Cuba, recurved through Bahamas, became extratropical near 38N."),
      boldPara("Observed damage: ", "$8.8 billion (World Bank/IDB). 45 deaths. 150,000 structures damaged, 24,000 totaled. 40\u201350% of hotels damaged. Strongest storm on record to make landfall in Jamaica, exceeding Gilbert (1988). Damage equivalent to 41% of Jamaica\u2019s GDP."),
      boldPara("Observed surge: ", ">9 feet on Jamaica\u2019s south coast."),
      boldPara("Observed rainfall: ", "18\u201324 inches across Jamaica."),
      boldPara("Formula assessment: ", "DPS of 51 appears low for a storm that caused $8.8B damage and was the strongest Jamaica landfall on record. The main reason: Greater Antilles shelf factor of 0.50 suppresses the surge component (S=0.60 at landfall). Additionally, Melissa\u2019s compact eyewall (R64=25nm) yields a modest wind field score (Wf=0.21). In reality, the combination of Cat 5 winds over a mountainous island with aging infrastructure amplified damage far beyond what the wind-field metric captures. The IAS of 57 is more reasonable, reflecting both the surge and slow forward speed (8 kt) rainfall potential."),
      boldPara("Key gap: ", "DPS heavily penalizes compact intense storms. A large Cat 3 with R64=80nm would score higher than this record-breaking Cat 5."),
      
      // Erin
      heading("Hurricane Erin \u2014 Cat 5 (DPS 78, IAS 44)", HeadingLevel.HEADING_2),
      boldPara("Track: ", "Formed Aug 11 from Cape Verde wave, became Cat 5 on Aug 16 with 160 mph winds and 913 mb pressure. Paralleled U.S. East Coast as Cat 2. No direct landfall."),
      boldPara("Observed damage: ", "$25 million (Aon). 9 deaths in Cape Verde from flooding. Life-threatening surf along entire U.S. East Coast."),
      boldPara("Physical size: ", "Exceptionally large\u2014tropical-storm-force winds spanned 575 miles, second only to Sandy since 1966. R34=490nm, R64=160nm."),
      boldPara("Formula assessment: ", "DPS of 78 reflects the enormous wind field but is capped by the Wf ceiling. R64=160nm yields R64\u00b2/45\u00b2 = 12.6, but the formula caps Wf at 1.5 before normalizing to 1.0. This means Erin\u2019s historically large wind field scores identically to any storm with R64 \u2265 55nm. With uncapped Wf, Erin would score in the mid-90s\u2014arguably appropriate given the massive area of hurricane-force winds."),
      boldPara("Key gap: ", "The Wf cap at 1.5 severely truncates the signal from truly exceptional wind fields. Erin and Sandy should score distinctly from storms with half their size."),
      
      // Humberto  
      heading("Hurricane Humberto \u2014 Cat 5 (DPS 79, IAS 55)", HeadingLevel.HEADING_2),
      boldPara("Track: ", "Formed Sep 24, reached Cat 5 on Sep 27 with 160 mph winds. Remained over open Atlantic. Became post-tropical Oct 1."),
      boldPara("Observed damage: ", "Minimal\u2014no direct land impacts. Coastal flooding and dangerous surf to Bermuda and U.S. East Coast."),
      boldPara("Formula assessment: ", "DPS of 79 is the highest of any 2025 Atlantic storm, yet Humberto caused essentially zero damage. This highlights the core tension: DPS measures destructive potential, not actual destruction. The score is meteorologically justified (strong winds, large field, slow movement), but contextually misleading. A user seeing \u201CDPS 79 \u2014 Extreme\u201D might assume catastrophic impacts when none occurred."),
      boldPara("Key gap: ", "DPS does not account for land proximity. A score disclaimer or separate \u201Cthreat to land\u201D flag would prevent misinterpretation."),
      
      // Imelda
      heading("Hurricane Imelda \u2014 Cat 2 (DPS 61, IAS 33)", HeadingLevel.HEADING_2),
      boldPara("Track: ", "Crossed Leeward Islands and Puerto Rico as tropical wave, became hurricane after Sep 24, peaked at Cat 2 (90 mph) near Bermuda. Influenced by Humberto\u2019s outflow steering it east."),
      boldPara("Observed damage: ", ">$10 million, primarily from rainfall flooding in the Antilles. 4 deaths from flooding in Dominican Republic."),
      boldPara("Physical size: ", "Extremely large for a Cat 2\u2014R34 reached 435nm, comparable to Sandy. R64=90nm."),
      boldPara("Formula assessment: ", "DPS of 61 is elevated primarily by the massive wind field (Wf=1.00, hitting the cap). This is appropriate\u2014Imelda\u2019s Sandy-like size warranted high concern even at Cat 2 intensity. However, IAS of 33 seems low given the significant flooding in the Dominican Republic and Puerto Rico. The rainfall component scored only 0.42 because the storm was moving at 7 kt (decent score) but the near-land multiplier was inconsistent\u2014it scored high for some snapshots over open water."),
      boldPara("Key gap: ", "The flooding that actually occurred was driven by topographic enhancement over Puerto Rico and Hispaniola, which the IAS formula completely ignores."),

      // Dexter
      heading("Tropical Storm Dexter \u2014 Cat 1 in IBTrACS (DPS 44, IAS 32)", HeadingLevel.HEADING_2),
      boldPara("Track: ", "Formed Aug 3 along stalled front ~300 miles east of NC. Peaked at 70 kt at 43.6N, -45.0W (mid-Atlantic). No threat to land. Lasted ~3 days."),
      boldPara("Observed damage: ", "None. No watches or warnings issued. Moderate rip current risk to East Coast beaches."),
      boldPara("Formula assessment: ", "DPS of 44 (\u201CSevere\u201D label) for a storm that caused zero damage and was never within 500nm of populated land. This is the clearest demonstration that DPS is agnostic to geography. The surge component (S=0.17) uses a default shelf factor of 0.50 even in the middle of the Atlantic Ocean. Wind field (Wf=0.67) is legitimate meteorologically but irrelevant when there\u2019s nothing to hit."),
      boldPara("Key gap: ", "DPS needs either a land-proximity modifier or explicit labeling that it represents raw atmospheric potential, not threat to human assets."),

      new Paragraph({ children: [new PageBreak()] }),

      // Shortcomings
      heading("Identified Formula Shortcomings", HeadingLevel.HEADING_1),
      para("The following 15 issues were identified across all three scoring formulas. They are categorized by formula and ranked by estimated impact on accuracy."),

      new Table({
        width: { size: issueColTotal, type: WidthType.DXA },
        columnWidths: issueCols,
        rows: [
          new TableRow({ children: ["#", "Formula", "Issue", "Suggested Fix"].map((h, i) => 
            cell(h, { width: issueCols[i], bold: true, fill: "1B3A5C", fontSize: 16 })) }),
          ...issues.map((row, ri) => new TableRow({
            children: row.map((c, ci) => cell(c, { width: issueCols[ci], fontSize: 16, fill: ri % 2 === 0 ? "F0F4F8" : undefined,
              align: ci === 0 ? AlignmentType.CENTER : AlignmentType.LEFT }))
          }))
        ]
      }),

      new Paragraph({ children: [new PageBreak()] }),

      // Deep Dives
      heading("Formula Deep-Dives", HeadingLevel.HEADING_1),
      
      heading("DPS (Destructive Potential Score)", HeadingLevel.HEADING_2),
      para("Formula: DPS = 40\u00d7S + 40\u00d7Wf + 10\u00d7V + 10\u00d7F (note: header comment says 45/35/10/10)"),
      para("The DPS formula effectively captures that storm surge and wind field size are the primary damage drivers, giving them 80% of the weight combined. However, three structural issues emerged from the 2025 analysis:"),
      
      boldPara("Wind field cap problem: ", "The Wf component normalizes to a 0\u20131.5 range then divides by 1.5 to get 0\u20131.0. This means any storm with R64 \u2265 55nm and R34 \u2265 260nm hits the ceiling. In 2025, both Erin (R64=160nm) and Imelda (R64=90nm) maxed out at Wf=1.0 despite Erin being nearly twice as large. For historical context, Sandy (R64=80nm) also caps out\u2014meaning the formula cannot distinguish between Sandy-sized storms and truly unprecedented events. Raising the cap to 3.0 or switching to a logarithmic scale would preserve discrimination at the extreme end."),
      
      boldPara("Open ocean scoring: ", "DPS assigns a default shelf factor of 0.50 for open ocean locations. Combined with a large wind field, this produces scores in the 40\u201380 range for fish storms that never threaten land. Dexter scored 44 (\u201CSevere\u201D) despite peaking 500+ nm from any coastline. The philosophical question is whether DPS should measure raw atmospheric potential or potential impact\u2014currently it mixes both by including a geographical shelf factor while not accounting for whether land exists nearby."),
      
      boldPara("Compact intense storms underscored: ", "Melissa at Cat 5 (165 kt) scored only 51 because its compact R64=25nm yields Wf=0.21. A large Cat 2 like Imelda (R64=90nm) scores higher in wind field despite having half the peak winds. This is arguably correct for total destruction potential (bigger storms affect more area), but creates a disconnect when a compact Cat 5 causes record damage at its point of impact, as Melissa did in Jamaica."),

      heading("IAS (Impact Area Score)", HeadingLevel.HEADING_2),
      para("Formula: IAS = 55\u00d7surge_geo + 45\u00d7rainfall_threat"),
      para("The IAS was designed to capture how geography amplifies storm impact\u2014continental shelf surge amplification and rainfall/stall threat. The 2025 season revealed several limitations:"),
      
      boldPara("Hard rainfall cutoff: ", "The formula assigns zero rainfall score to any storm with forward speed \u2265 12 kt. This is a cliff edge: a storm moving at 11.9 kt gets full rainfall credit while one at 12.1 kt gets zero. In reality, hurricanes moving at 12\u201315 kt still produce prodigious rainfall. Florence (2018) caused historic flooding partly during its 12\u201315 kt approach phase. A gradual taper (decay to zero at 18\u201320 kt) would be more physically realistic."),
      
      boldPara("Surge redundancy with DPS: ", "The IAS surge_geo component uses the identical formula as DPS\u2019s surge component S: calculateSurgeParametric \u00d7 shelfFactor / 25. This means 55% of IAS directly duplicates information already in DPS. When both scores are summed into the cumulative score, surge influence is double-counted. A more independent IAS surge metric might consider bathymetric funneling, bay geometry, or tidal timing\u2014factors not in DPS."),
      
      boldPara("Missing orographic rainfall: ", "Melissa produced 18\u201324 inches of rain over Jamaica, amplified by the Blue Mountains (7,400 ft). Imelda caused deadly flooding in the Dominican Republic partly due to Cordillera Central orographic lift. The IAS formula has no terrain awareness\u2014rainfall threat is based solely on forward speed and a shelf-based near-land proxy. Adding a terrain amplification factor for known mountain ranges near coastlines would materially improve accuracy for Caribbean and Central American storms."),

      heading("ERS (Economic Risk Score)", HeadingLevel.HEADING_2),
      para("Formula: ERS = 100 \u00d7 exposure \u00d7 vulnerability \u00d7 threatReach"),
      para("ERS performed reasonably well in 2025 because most storms peaked over open ocean, where the low exposure value (0.05) correctly suppressed scores. The formula\u2019s main issues are structural rather than exposed by this particular season:"),
      
      boldPara("Zone bounding box gaps: ", "ECON_ZONES use rectangular bounding boxes that occasionally leave gaps between adjacent zones. A storm positioned between two zones (e.g., between NC Outer Banks and Georgia/SC Coast) could fall through to \u201COpen Ocean\u201D with exposure=0.05 despite threatening a populated coastline. A nearest-zone fallback or distance-weighted lookup would prevent this."),
      
      boldPara("Size component cap: ", "Like DPS\u2019s Wf, the ERS size component caps at 1.0 via sqrt(R34/250). Erin (R34=490nm) and Sandy (R34=400nm) produce identical size components despite Erin being 20% wider. This cap affects the threat reach calculation, which determines how much of the economic exposure a storm can access."),
      
      boldPara("Dual-mode validation: ", "The dual-mode system (hand-tuned for historical, NRI for active) was validated against historical storms but has not been tested against real active-storm scenarios. The 2025 season provided limited opportunity since no significant storms threatened U.S. coastline. Melissa\u2019s Jamaica landfall was in the Greater Antilles zone where NRI data is unavailable (only U.S. counties), so it fell back to hand-tuned values regardless."),

      new Paragraph({ children: [new PageBreak()] }),

      // Cross-reference
      heading("Cross-Reference: Scores vs. Observed Reality", HeadingLevel.HEADING_1),
      para("The table below compares computed cumulative scores against observed damage for the three 2025 storms with measurable economic impact."),
      
      new Table({
        width: { size: 9360, type: WidthType.DXA },
        columnWidths: [2000, 1400, 1400, 1400, 1760, 1400],
        rows: [
          new TableRow({ children: ["Storm", "DPS", "IAS", "ERS", "Cumulative", "Actual Damage"].map((h, i) =>
            cell(h, { width: [2000,1400,1400,1400,1760,1400][i], bold: true, fill: "1B3A5C", fontSize: 16 })) }),
          ...[ 
            ["Melissa", "51", "57", "~23", "~131 / 300", "$8.8B"],
            ["Erin", "78", "44", "~5", "~127 / 300", "$25M"],
            ["Imelda", "61", "33", "~5", "~99 / 300", ">$10M"],
            ["Humberto", "79", "55", "~5", "~139 / 300", "$0"],
            ["Dexter", "44", "32", "~5", "~81 / 300", "$0"],
          ].map((row, ri) => new TableRow({
            children: row.map((c, ci) => cell(c, { width: [2000,1400,1400,1400,1760,1400][ci], fontSize: 16,
              fill: ri % 2 === 0 ? "F0F4F8" : undefined, align: ci >= 1 ? AlignmentType.CENTER : AlignmentType.LEFT }))
          }))
        ]
      }),

      para(""),
      para("The most striking finding: Humberto (cumulative ~139) outscores Melissa (~131) despite causing zero damage versus $8.8 billion. Erin (~127) also scores near Melissa despite causing only $25 million in damage. This inversion occurs because DPS and IAS are purely meteorological metrics\u2014they measure what a storm could do, not what it actually does. Only ERS accounts for land proximity, but it\u2019s weighted equally in the cumulative sum, diluting its corrective effect."),
      para("This suggests the cumulative score formula may benefit from weighting ERS more heavily, or that DPS/IAS should include a land-proximity dampener. Alternatively, the scores could be presented as a matrix rather than a sum, helping users understand that a high DPS over open ocean is fundamentally different from a high DPS at landfall."),

      new Paragraph({ children: [new PageBreak()] }),

      // Recommendations
      heading("Priority Recommendations", HeadingLevel.HEADING_1),
      
      heading("High Priority (Material accuracy impact)", HeadingLevel.HEADING_2),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Raise or remove the Wf cap in DPS: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Change from 1.5 to 3.0 or use logarithmic scaling. This directly affects scoring of oversized storms like Erin and Sandy.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Replace hard 12 kt rainfall cutoff with gradual taper: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Use linear or exponential decay from 12 kt to 20 kt instead of a cliff edge.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Fix comment/code weight mismatch: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "DPS header says 45/35/10/10 but code uses 40/40/10/10. Decide which is correct and align them.", font: "Arial", size: 20 })] }),
      
      heading("Medium Priority (Meaningful improvement)", HeadingLevel.HEADING_2),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Add land-proximity context to DPS: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Either dampen DPS when >200nm from coast, add a \u201Cthreat to land\u201D indicator, or rename DPS to explicitly convey it\u2019s raw atmospheric potential.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Differentiate IAS surge from DPS surge: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "The cumulative score double-counts surge. Give IAS a unique surge metric (bay funneling, tidal timing) or reduce its surge weight.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Add orographic rainfall factor: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Mountain ranges near coastlines (Blue Mountains, Cordillera Central, Appalachians) dramatically amplify rainfall. A lookup table for known ranges would improve IAS for Caribbean and SE US storms.", font: "Arial", size: 20 })] }),
      
      heading("Low Priority (Edge case / cosmetic)", HeadingLevel.HEADING_2),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Raise ERS size component normalization: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Change sqrt(R34/250) to sqrt(R34/400) to preserve discrimination for Sandy+ sized storms.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Split Greater Antilles shelf region: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Jamaica, Puerto Rico, Cuba, and Hispaniola have meaningfully different shelf geometry. Sub-regions would improve surge accuracy.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Estimate forward speed when missing: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "Compute from consecutive lat/lon positions to avoid zero-rainfall scores for HURDAT2 storms.", font: "Arial", size: 20 })] }),
      new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 80 },
        children: [new TextRun({ text: "Add zone bounding box fallback: ", bold: true, font: "Arial", size: 20 }),
                   new TextRun({ text: "When no ECON_ZONE matches, find nearest zone within 150nm instead of defaulting to Open Ocean.", font: "Arial", size: 20 })] }),
    ]
  }]
});

Packer.toBuffer(doc).then(buffer => {
  fs.writeFileSync("/sessions/wonderful-charming-thompson/mnt/hurricane_app/2025_Formula_Review.docx", buffer);
  console.log("Document created successfully");
});
