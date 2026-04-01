/**
 * Storm detail screen — shows full DPI breakdown, IKE, surge, economic scores.
 * Navigated to from the storms list or map marker press.
 */

import { View, Text, ScrollView, StyleSheet, ActivityIndicator } from "react-native";
import { useLocalSearchParams, Stack } from "expo-router";
import { COLORS, SPACING, FONT_SIZES, BORDER_RADIUS, dpiColor, categoryColor } from "@/constants/theme";
import { useStormValuation, useStormHistory } from "@/hooks/useStorms";

function ScoreCard({ label, score, max, color }: { label: string; score: number; max: number; color: string }) {
  const pct = Math.min(100, (score / max) * 100);
  return (
    <View style={styles.scoreCard}>
      <Text style={styles.scoreLabel}>{label}</Text>
      <Text style={[styles.scoreValue, { color }]}>{score.toFixed(1)}</Text>
      <View style={styles.barTrack}>
        <View style={[styles.barFill, { width: `${pct}%`, backgroundColor: color }]} />
      </View>
    </View>
  );
}

export default function StormDetailScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const { data: val, isLoading, error } = useStormValuation(id ?? null);
  const { data: history } = useStormHistory(id ?? null);

  if (isLoading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator size="large" color={COLORS.accentCyan} />
        <Text style={styles.loadingText}>Computing DPI...</Text>
      </View>
    );
  }

  if (error || !val) {
    return (
      <View style={styles.center}>
        <Text style={styles.errorText}>Failed to load storm data</Text>
      </View>
    );
  }

  return (
    <>
      <Stack.Screen options={{ title: val.name }} />
      <ScrollView style={styles.container} contentContainerStyle={styles.content}>
        {/* Destructive Potential Hero */}
        <View style={[styles.heroCard, { borderColor: dpiColor(val.destructive_potential ?? 0) }]}>
          <Text style={styles.heroLabel}>Destructive Potential</Text>
          <Text style={[styles.heroScore, { color: dpiColor(val.destructive_potential ?? 0) }]}>
            {val.destructive_potential?.toFixed(1) ?? "N/A"}
          </Text>
          <Text style={[styles.heroCategory, { color: dpiColor(val.destructive_potential ?? 0) }]}>
            {val.category ?? "Unknown"}
          </Text>
        </View>

        {/* Valuation Summary */}
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Valuation Summary</Text>
          {val.destructive_potential !== null && val.destructive_potential !== undefined ? (
            <ScoreCard label="Destructive Potential" score={val.destructive_potential} max={100} color={COLORS.accentOrange} />
          ) : (
            <View style={styles.row}>
              <Text style={styles.label}>Destructive Potential</Text>
              <Text style={styles.value}>Data unavailable</Text>
            </View>
          )}
          {val.surge_threat !== null && val.surge_threat !== undefined ? (
            <ScoreCard label="Surge Threat" score={val.surge_threat} max={100} color={COLORS.accentBlue} />
          ) : (
            <View style={styles.row}>
              <Text style={styles.label}>Surge Threat</Text>
              <Text style={styles.value}>Data unavailable</Text>
            </View>
          )}
          {val.overall_value !== null && val.overall_value !== undefined && (
            <>
              <View style={styles.divider} />
              <ScoreCard label="Overall Value" score={val.overall_value} max={100} color={COLORS.accentCyan} />
            </>
          )}
        </View>

        {/* IKE Details */}
        {val.ike ? (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Integrated Kinetic Energy</Text>
            <View style={styles.row}>
              <Text style={styles.label}>Total IKE</Text>
              <Text style={styles.value}>{val.ike.ike_total_tj?.toFixed(1) ?? "N/A"} TJ</Text>
            </View>
            <View style={styles.row}>
              <Text style={styles.label}>Hurricane-force</Text>
              <Text style={styles.value}>{val.ike.ike_hurricane_tj?.toFixed(1) ?? "N/A"} TJ</Text>
            </View>
            <View style={styles.row}>
              <Text style={styles.label}>TS-force</Text>
              <Text style={styles.value}>{val.ike.ike_tropical_storm_tj?.toFixed(1) ?? "N/A"} TJ</Text>
            </View>
            {val.ike.ike_pretty && (
              <View style={styles.row}>
                <Text style={styles.label}>Description</Text>
                <Text style={styles.value}>{val.ike.ike_pretty}</Text>
              </View>
            )}
          </View>
        ) : (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Integrated Kinetic Energy</Text>
            <Text style={styles.value}>Data unavailable</Text>
          </View>
        )}

        {/* Wind Field Information */}
        {val.ike && (val.ike.max_wind_ms !== null || val.ike.min_pressure_hpa !== null) ? (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Wind Field</Text>
            {val.ike.max_wind_ms !== null && val.ike.max_wind_ms !== undefined ? (
              <View style={styles.row}>
                <Text style={styles.label}>Max Wind</Text>
                <Text style={styles.value}>{val.ike.max_wind_ms.toFixed(1)} m/s</Text>
              </View>
            ) : (
              <View style={styles.row}>
                <Text style={styles.label}>Max Wind</Text>
                <Text style={styles.value}>N/A</Text>
              </View>
            )}
            {val.ike.min_pressure_hpa !== null && val.ike.min_pressure_hpa !== undefined ? (
              <View style={styles.row}>
                <Text style={styles.label}>Min Pressure</Text>
                <Text style={styles.value}>{val.ike.min_pressure_hpa.toFixed(1)} hPa</Text>
              </View>
            ) : (
              <View style={styles.row}>
                <Text style={styles.label}>Min Pressure</Text>
                <Text style={styles.value}>N/A</Text>
              </View>
            )}
            {val.ike.rmw_nm !== null && val.ike.rmw_nm !== undefined ? (
              <View style={styles.row}>
                <Text style={styles.label}>Radius of Max Winds</Text>
                <Text style={styles.value}>{val.ike.rmw_nm.toFixed(1)} nm</Text>
              </View>
            ) : (
              <View style={styles.row}>
                <Text style={styles.label}>Radius of Max Winds</Text>
                <Text style={styles.value}>N/A</Text>
              </View>
            )}
            {val.ike.r34_nm !== null && val.ike.r34_nm !== undefined ? (
              <View style={styles.row}>
                <Text style={styles.label}>34-kt Radius</Text>
                <Text style={styles.value}>{val.ike.r34_nm.toFixed(1)} nm</Text>
              </View>
            ) : (
              <View style={styles.row}>
                <Text style={styles.label}>34-kt Radius</Text>
                <Text style={styles.value}>N/A</Text>
              </View>
            )}
            {val.ike.wind_field_source && (
              <View style={styles.row}>
                <Text style={styles.label}>Data Source</Text>
                <Text style={styles.value}>{val.ike.wind_field_source}</Text>
              </View>
            )}
          </View>
        ) : (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Wind Field</Text>
            <Text style={styles.value}>Data unavailable</Text>
          </View>
        )}

        {/* Movement Information */}
        {val.ike && (val.ike.forward_speed_knots !== null || val.ike.forward_direction_deg !== null) ? (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Movement</Text>
            {val.ike.forward_speed_knots !== null && val.ike.forward_speed_knots !== undefined ? (
              <View style={styles.row}>
                <Text style={styles.label}>Forward Speed</Text>
                <Text style={styles.value}>{val.ike.forward_speed_knots.toFixed(1)} kt</Text>
              </View>
            ) : (
              <View style={styles.row}>
                <Text style={styles.label}>Forward Speed</Text>
                <Text style={styles.value}>N/A</Text>
              </View>
            )}
            {val.ike.forward_direction_deg !== null && val.ike.forward_direction_deg !== undefined ? (
              <View style={styles.row}>
                <Text style={styles.label}>Direction</Text>
                <Text style={styles.value}>{val.ike.forward_direction_deg.toFixed(0)}°</Text>
              </View>
            ) : (
              <View style={styles.row}>
                <Text style={styles.label}>Direction</Text>
                <Text style={styles.value}>N/A</Text>
              </View>
            )}
          </View>
        ) : (
          val.ike && (
            <View style={styles.section}>
              <Text style={styles.sectionTitle}>Movement</Text>
              <Text style={styles.value}>Data unavailable</Text>
            </View>
          )
        )}

        {/* Track History Count */}
        {history && (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Track History</Text>
            <Text style={styles.label}>
              {history.length} data points from IBTrACS/HURDAT2
            </Text>
          </View>
        )}
      </ScrollView>
    </>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bgPrimary },
  content: { padding: SPACING.lg, paddingBottom: SPACING.xxl * 2 },
  center: { flex: 1, justifyContent: "center", alignItems: "center", backgroundColor: COLORS.bgPrimary },
  loadingText: { color: COLORS.textSecondary, marginTop: SPACING.md, fontSize: FONT_SIZES.md },
  errorText: { color: COLORS.statusDanger, fontSize: FONT_SIZES.md },

  heroCard: {
    backgroundColor: COLORS.bgCard,
    borderRadius: BORDER_RADIUS.xl,
    padding: SPACING.xl,
    alignItems: "center",
    borderWidth: 2,
    marginBottom: SPACING.lg,
  },
  heroLabel: { color: COLORS.textSecondary, fontSize: FONT_SIZES.sm, marginBottom: SPACING.xs },
  heroScore: { fontSize: 56, fontWeight: "800" },
  heroCategory: { fontSize: FONT_SIZES.xl, fontWeight: "700", marginTop: SPACING.xs },

  section: {
    backgroundColor: COLORS.bgCard,
    borderRadius: BORDER_RADIUS.lg,
    padding: SPACING.lg,
    marginBottom: SPACING.md,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  sectionTitle: {
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.lg,
    fontWeight: "700",
    marginBottom: SPACING.md,
  },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingVertical: SPACING.xs,
  },
  label: { color: COLORS.textSecondary, fontSize: FONT_SIZES.md },
  value: { color: COLORS.textPrimary, fontSize: FONT_SIZES.md, fontWeight: "600" },
  divider: { height: 1, backgroundColor: COLORS.border, marginVertical: SPACING.sm },

  scoreCard: { marginBottom: SPACING.md },
  scoreLabel: { color: COLORS.textSecondary, fontSize: FONT_SIZES.sm, marginBottom: 2 },
  scoreValue: { fontSize: FONT_SIZES.xl, fontWeight: "800", marginBottom: SPACING.xs },
  barTrack: {
    height: 6,
    backgroundColor: COLORS.bgPrimary,
    borderRadius: 3,
    overflow: "hidden",
  },
  barFill: { height: 6, borderRadius: 3 },
});
