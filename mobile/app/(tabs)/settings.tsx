/**
 * Settings screen — API configuration, units, and app info.
 */

import { View, Text, StyleSheet, Switch, Pressable, Linking } from "react-native";
import { Ionicons } from "@expo/vector-icons";
import { COLORS, SPACING, FONT_SIZES, BORDER_RADIUS } from "@/constants/theme";
import { useHealthCheck } from "@/hooks/useStorms";
import { useAppStore } from "@/services/store";
import { CONFIG } from "@/constants/config";

export default function SettingsScreen() {
  const { data: apiOk } = useHealthCheck();
  const { useMetric, setUseMetric } = useAppStore();

  return (
    <View style={styles.container}>
      {/* API Status */}
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Connection</Text>
        <View style={styles.row}>
          <Text style={styles.label}>API Server</Text>
          <View style={styles.statusRow}>
            <View style={[styles.dot, { backgroundColor: apiOk ? COLORS.statusActive : COLORS.statusDanger }]} />
            <Text style={[styles.value, { color: apiOk ? COLORS.statusActive : COLORS.statusDanger }]}>
              {apiOk ? "Connected" : "Disconnected"}
            </Text>
          </View>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Endpoint</Text>
          <Text style={styles.valueMono} numberOfLines={1}>{CONFIG.API_BASE_URL}</Text>
        </View>
      </View>

      {/* Preferences */}
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Preferences</Text>
        <View style={styles.row}>
          <Text style={styles.label}>Metric Units (km, hPa, m/s)</Text>
          <Switch
            value={useMetric}
            onValueChange={setUseMetric}
            trackColor={{ false: COLORS.bgCard, true: COLORS.accentBlue }}
            thumbColor={COLORS.textPrimary}
          />
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Storm Alerts</Text>
          <Text style={styles.comingSoonText}>Coming soon</Text>
        </View>
      </View>

      {/* About */}
      <View style={styles.section}>
        <Text style={styles.sectionTitle}>About</Text>
        <View style={styles.row}>
          <Text style={styles.label}>Version</Text>
          <Text style={styles.value}>1.0.0</Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>DPI Formula</Text>
          <Text style={styles.value}>Production (validated 12 storms, 6.4% avg error)</Text>
        </View>
        <View style={styles.row}>
          <Text style={styles.label}>Source Code</Text>
          <Text style={styles.comingSoonText}>github.com/hurricane-dpi</Text>
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bgPrimary, padding: SPACING.lg },
  section: {
    backgroundColor: COLORS.bgCard,
    borderRadius: BORDER_RADIUS.lg,
    padding: SPACING.lg,
    marginBottom: SPACING.lg,
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
    paddingVertical: SPACING.sm,
  },
  label: { color: COLORS.textSecondary, fontSize: FONT_SIZES.md, flex: 1 },
  value: { color: COLORS.textPrimary, fontSize: FONT_SIZES.md },
  valueMono: {
    color: COLORS.textMuted,
    fontSize: FONT_SIZES.xs,
    fontFamily: "monospace",
    maxWidth: "55%",
  },
  comingSoonText: { color: COLORS.textMuted, fontSize: FONT_SIZES.md },
  statusRow: { flexDirection: "row", alignItems: "center" },
  dot: { width: 8, height: 8, borderRadius: 4, marginRight: SPACING.xs },
});
