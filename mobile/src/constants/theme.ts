/**
 * Design tokens matching the existing dark-themed frontend.
 * Colors extracted from the original CSS custom properties.
 */

export const COLORS = {
  // Backgrounds
  bgPrimary: "#0a0e1a",
  bgSecondary: "#111827",
  bgCard: "#1e293b",
  bgCardHover: "#334155",
  bgOverlay: "rgba(10, 14, 26, 0.95)",

  // Text
  textPrimary: "#f1f5f9",
  textSecondary: "#94a3b8",
  textMuted: "#64748b",

  // Accents
  accentBlue: "#3b82f6",
  accentCyan: "#22d3ee",
  accentGreen: "#22c55e",
  accentYellow: "#eab308",
  accentOrange: "#f97316",
  accentRed: "#ef4444",
  accentPurple: "#a855f7",

  // Borders
  border: "#1e293b",
  borderActive: "#3b82f6",

  // Category-specific (Saffir-Simpson)
  catTD: "#64748b",
  catTS: "#22c55e",
  cat1: "#eab308",
  cat2: "#f97316",
  cat3: "#ef4444",
  cat4: "#dc2626",
  cat5: "#7c2d12",

  // DPI categories
  dpiMinor: "#22c55e",
  dpiModerate: "#eab308",
  dpiSevere: "#f97316",
  dpiExtreme: "#ef4444",
  dpiDevastating: "#dc2626",
  dpiCatastrophic: "#7c2d12",

  // Status
  statusActive: "#22c55e",
  statusInactive: "#64748b",
  statusWarning: "#f97316",
  statusDanger: "#ef4444",
} as const;

export const SPACING = {
  xs: 4,
  sm: 8,
  md: 12,
  lg: 16,
  xl: 24,
  xxl: 32,
} as const;

export const FONT_SIZES = {
  xs: 10,
  sm: 12,
  md: 14,
  lg: 16,
  xl: 20,
  xxl: 24,
  title: 28,
  hero: 36,
} as const;

export const BORDER_RADIUS = {
  sm: 6,
  md: 10,
  lg: 14,
  xl: 20,
  full: 9999,
} as const;

/** Saffir-Simpson category to color mapping */
export function categoryColor(cat: number): string {
  switch (cat) {
    case 0: return COLORS.catTD;
    case 1: return COLORS.catTS;
    case 2: return COLORS.cat1;
    case 3: return COLORS.cat2;
    case 4: return COLORS.cat3;
    case 5: return COLORS.cat4;
    case 6: return COLORS.cat5;
    default: return COLORS.textMuted;
  }
}

/** DPI score to color mapping */
export function dpiColor(score: number): string {
  if (score < 15) return COLORS.dpiMinor;
  if (score < 30) return COLORS.dpiModerate;
  if (score < 50) return COLORS.dpiSevere;
  if (score < 70) return COLORS.dpiExtreme;
  if (score < 85) return COLORS.dpiDevastating;
  return COLORS.dpiCatastrophic;
}
