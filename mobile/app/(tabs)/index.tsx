/**
 * Map screen — primary view showing active storms on an interactive map.
 *
 * Uses react-native-maps on iOS/Android (Apple Maps / Google Maps).
 * Falls back to a styled storm list on web (react-native-maps has no
 * first-party web support).
 */

import { useEffect, useCallback, useMemo, useRef, useState } from "react";
import {
  View,
  Text,
  StyleSheet,
  Platform,
  ActivityIndicator,
  Pressable,
  ScrollView,
} from "react-native";
import { useRouter } from "expo-router";
import { COLORS, SPACING, FONT_SIZES, BORDER_RADIUS, categoryColor, dpiColor } from "@/constants/theme";
import { useActiveStorms, useHealthCheck } from "@/hooks/useStorms";
import { useAppStore } from "@/services/store";
import type { StormSummary } from "@/types/storm";

// react-native-maps is only available on iOS/Android
const isNative = Platform.OS === "ios" || Platform.OS === "android";

let MapView: any = null;
let Marker: any = null;
let Callout: any = null;
let Circle: any = null;

if (isNative) {
  try {
    const Maps = require("react-native-maps");
    MapView = Maps.default;
    Marker = Maps.Marker;
    Callout = Maps.Callout;
    Circle = Maps.Circle;
  } catch {
    // Graceful fallback if maps fails to load
  }
}

/** Approximate Saffir-Simpson category from wind speed in knots */
function safirSimpsonFromKnots(knots: number | null): number {
  if (knots == null) return 0;
  if (knots >= 137) return 5;
  if (knots >= 113) return 4;
  if (knots >= 96) return 3;
  if (knots >= 83) return 2;
  if (knots >= 64) return 1;
  if (knots >= 34) return -1; // TS
  return -2; // TD
}

/** Category label for display */
function categoryLabel(knots: number | null): string {
  const cat = safirSimpsonFromKnots(knots);
  if (cat >= 1) return `Cat ${cat}`;
  if (cat === -1) return "TS";
  return "TD";
}

/** Marker color based on storm intensity */
function stormColor(knots: number | null): string {
  const cat = safirSimpsonFromKnots(knots);
  switch (cat) {
    case 5: return COLORS.cat5;
    case 4: return COLORS.cat4;
    case 3: return COLORS.cat3;
    case 2: return COLORS.cat2;
    case 1: return COLORS.cat1;
    case -1: return COLORS.catTS;
    default: return COLORS.catTD;
  }
}

/** Rough wind radius in meters for the circle overlay (r34 approximation) */
function windRadiusMeters(knots: number | null): number {
  if (knots == null) return 150_000;
  if (knots >= 137) return 350_000;
  if (knots >= 113) return 300_000;
  if (knots >= 96) return 260_000;
  if (knots >= 64) return 220_000;
  if (knots >= 34) return 180_000;
  return 120_000;
}

/** Default region: Atlantic basin centered */
const ATLANTIC_REGION = {
  latitude: 25.0,
  longitude: -70.0,
  latitudeDelta: 40,
  longitudeDelta: 60,
};

// ─────────────────────────────────────────────────────────────────
// Native Map Component
// ─────────────────────────────────────────────────────────────────

function NativeMap({
  storms,
  onStormPress,
}: {
  storms: StormSummary[];
  onStormPress: (id: string) => void;
}) {
  const mapRef = useRef<any>(null);
  const [selectedStorm, setSelectedStorm] = useState<string | null>(null);

  // Fit map to show all storms when data loads
  useEffect(() => {
    if (!mapRef.current || storms.length === 0) return;
    const coords = storms
      .filter((s) => s.lat != null && s.lon != null)
      .map((s) => ({ latitude: s.lat!, longitude: s.lon! }));
    if (coords.length > 0) {
      mapRef.current.fitToCoordinates(coords, {
        edgePadding: { top: 80, right: 40, bottom: 80, left: 40 },
        animated: true,
      });
    }
  }, [storms]);

  if (!MapView) {
    return <WebFallback storms={storms} onStormPress={onStormPress} />;
  }

  return (
    <MapView
      ref={mapRef}
      style={StyleSheet.absoluteFillObject}
      initialRegion={ATLANTIC_REGION}
      mapType="standard"
      showsCompass
      showsScale
      rotateEnabled={false}
      customMapStyle={darkMapStyle}
    >
      {storms
        .filter((s) => s.lat != null && s.lon != null)
        .map((s) => {
          const color = stormColor(s.intensity_knots);
          const isSelected = selectedStorm === s.id;
          return (
            <View key={s.id}>
              {/* Wind field circle */}
              {Circle && (
                <Circle
                  center={{ latitude: s.lat!, longitude: s.lon! }}
                  radius={windRadiusMeters(s.intensity_knots)}
                  strokeWidth={1}
                  strokeColor={color + "66"}
                  fillColor={color + "1A"}
                />
              )}
              {/* Storm marker */}
              <Marker
                coordinate={{ latitude: s.lat!, longitude: s.lon! }}
                pinColor={color}
                onPress={() => setSelectedStorm(s.id)}
              >
                {Callout && (
                  <Callout
                    tooltip
                    onPress={() => onStormPress(s.id)}
                  >
                    <View style={styles.callout}>
                      <Text style={styles.calloutTitle}>{s.name}</Text>
                      <Text style={styles.calloutSubtitle}>
                        {s.classification} \u00B7 {categoryLabel(s.intensity_knots)}
                      </Text>
                      {s.intensity_knots != null && (
                        <Text style={styles.calloutDetail}>
                          {s.intensity_knots} kt
                          {s.pressure_mb ? ` \u00B7 ${s.pressure_mb} mb` : ""}
                        </Text>
                      )}
                      {s.movement && (
                        <Text style={styles.calloutDetail}>
                          Moving {s.movement}
                          {s.movement_speed_knots ? ` at ${s.movement_speed_knots} kt` : ""}
                        </Text>
                      )}
                      <Text style={styles.calloutAction}>Tap for details</Text>
                    </View>
                  </Callout>
                )}
              </Marker>
            </View>
          );
        })}
    </MapView>
  );
}

// ─────────────────────────────────────────────────────────────────
// Web Fallback (styled card list — react-native-maps has no web support)
// ─────────────────────────────────────────────────────────────────

function WebFallback({
  storms,
  onStormPress,
}: {
  storms: StormSummary[];
  onStormPress: (id: string) => void;
}) {
  return (
    <ScrollView
      style={styles.webList}
      contentContainerStyle={styles.webListContent}
    >
      <Text style={styles.webHeader}>Active Storms</Text>
      {storms.length === 0 ? (
        <Text style={styles.webEmpty}>No active storms at this time.</Text>
      ) : (
        storms.map((s) => {
          const color = stormColor(s.intensity_knots);
          return (
            <Pressable
              key={s.id}
              style={({ pressed }) => [
                styles.webCard,
                pressed && styles.webCardPressed,
              ]}
              onPress={() => onStormPress(s.id)}
            >
              <View style={[styles.webCardStripe, { backgroundColor: color }]} />
              <View style={styles.webCardBody}>
                <Text style={styles.webCardName}>{s.name}</Text>
                <Text style={styles.webCardMeta}>
                  {s.classification} \u00B7 {categoryLabel(s.intensity_knots)}
                  {s.intensity_knots != null ? ` \u00B7 ${s.intensity_knots} kt` : ""}
                </Text>
                {s.lat != null && s.lon != null && (
                  <Text style={styles.webCardCoords}>
                    {s.lat.toFixed(1)}\u00B0N, {Math.abs(s.lon!).toFixed(1)}\u00B0{s.lon! < 0 ? "W" : "E"}
                  </Text>
                )}
                {s.movement && (
                  <Text style={styles.webCardMovement}>
                    Moving {s.movement}
                    {s.movement_speed_knots ? ` at ${s.movement_speed_knots} kt` : ""}
                  </Text>
                )}
              </View>
            </Pressable>
          );
        })
      )}
    </ScrollView>
  );
}

// ─────────────────────────────────────────────────────────────────
// Main Screen
// ─────────────────────────────────────────────────────────────────

export default function MapScreen() {
  const router = useRouter();
  const { data: storms, isLoading, error } = useActiveStorms();
  const { data: apiOk } = useHealthCheck();
  const setApiConnected = useAppStore((s) => s.setApiConnected);

  useEffect(() => {
    if (apiOk !== undefined) setApiConnected(apiOk);
  }, [apiOk]);

  const handleStormPress = useCallback(
    (stormId: string) => {
      router.push(`/storm/${stormId}`);
    },
    [router],
  );

  const stormList = useMemo(() => storms ?? [], [storms]);

  return (
    <View style={styles.container}>
      {/* Status bar */}
      <View style={styles.statusBar}>
        <View
          style={[
            styles.dot,
            { backgroundColor: apiOk ? COLORS.statusActive : COLORS.statusDanger },
          ]}
        />
        <Text style={styles.statusText}>
          {apiOk ? "API Connected" : "Connecting\u2026"}
        </Text>
        {storms && (
          <Text style={styles.stormCount}>
            {storms.length} active storm{storms.length !== 1 ? "s" : ""}
          </Text>
        )}
      </View>

      {/* Map area */}
      <View style={styles.mapContainer}>
        {isLoading ? (
          <ActivityIndicator size="large" color={COLORS.accentCyan} />
        ) : error ? (
          <Text style={styles.errorText}>
            Unable to load storm data. Check your connection.
          </Text>
        ) : isNative && MapView ? (
          <NativeMap storms={stormList} onStormPress={handleStormPress} />
        ) : (
          <WebFallback storms={stormList} onStormPress={handleStormPress} />
        )}
      </View>

      {/* Legend overlay (native only, bottom-left) */}
      {isNative && !isLoading && !error && stormList.length > 0 && (
        <View style={styles.legend}>
          <Text style={styles.legendTitle}>Saffir-Simpson</Text>
          {[
            { label: "TD", color: COLORS.catTD },
            { label: "TS", color: COLORS.catTS },
            { label: "Cat 1", color: COLORS.cat1 },
            { label: "Cat 2", color: COLORS.cat2 },
            { label: "Cat 3", color: COLORS.cat3 },
            { label: "Cat 4", color: COLORS.cat4 },
            { label: "Cat 5", color: COLORS.cat5 },
          ].map(({ label, color }) => (
            <View key={label} style={styles.legendRow}>
              <View style={[styles.legendDot, { backgroundColor: color }]} />
              <Text style={styles.legendLabel}>{label}</Text>
            </View>
          ))}
        </View>
      )}
    </View>
  );
}

// ─────────────────────────────────────────────────────────────────
// Dark map style (Google Maps only; Apple Maps uses system dark mode)
// ─────────────────────────────────────────────────────────────────
const darkMapStyle = Platform.OS === "android"
  ? [
      { elementType: "geometry", stylers: [{ color: "#0a0e1a" }] },
      { elementType: "labels.text.fill", stylers: [{ color: "#94a3b8" }] },
      { elementType: "labels.text.stroke", stylers: [{ color: "#0a0e1a" }] },
      {
        featureType: "water",
        elementType: "geometry",
        stylers: [{ color: "#111827" }],
      },
      {
        featureType: "road",
        elementType: "geometry",
        stylers: [{ color: "#1e293b" }],
      },
    ]
  : undefined;

// ─────────────────────────────────────────────────────────────────
// Styles
// ─────────────────────────────────────────────────────────────────
const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.bgPrimary,
  },
  statusBar: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: SPACING.lg,
    paddingVertical: SPACING.sm,
    backgroundColor: COLORS.bgSecondary,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    zIndex: 10,
  },
  dot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    marginRight: SPACING.sm,
  },
  statusText: {
    color: COLORS.textSecondary,
    fontSize: FONT_SIZES.sm,
  },
  stormCount: {
    color: COLORS.accentCyan,
    fontSize: FONT_SIZES.sm,
    marginLeft: "auto",
    fontWeight: "600",
  },
  mapContainer: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  errorText: {
    color: COLORS.statusDanger,
    fontSize: FONT_SIZES.md,
    textAlign: "center",
    padding: SPACING.xl,
  },

  // ── Callout (native markers) ──────────────────────────────────
  callout: {
    backgroundColor: COLORS.bgCard,
    borderRadius: BORDER_RADIUS.md,
    padding: SPACING.md,
    minWidth: 180,
    maxWidth: 260,
  },
  calloutTitle: {
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.lg,
    fontWeight: "700",
    marginBottom: 2,
  },
  calloutSubtitle: {
    color: COLORS.accentCyan,
    fontSize: FONT_SIZES.sm,
    marginBottom: 4,
  },
  calloutDetail: {
    color: COLORS.textSecondary,
    fontSize: FONT_SIZES.sm,
    marginBottom: 2,
  },
  calloutAction: {
    color: COLORS.accentBlue,
    fontSize: FONT_SIZES.xs,
    marginTop: 6,
    textAlign: "right",
  },

  // ── Legend overlay (native) ───────────────────────────────────
  legend: {
    position: "absolute",
    bottom: SPACING.xl,
    left: SPACING.lg,
    backgroundColor: COLORS.bgOverlay,
    borderRadius: BORDER_RADIUS.md,
    padding: SPACING.md,
    zIndex: 20,
  },
  legendTitle: {
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.xs,
    fontWeight: "700",
    marginBottom: SPACING.xs,
  },
  legendRow: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 2,
  },
  legendDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    marginRight: 6,
  },
  legendLabel: {
    color: COLORS.textSecondary,
    fontSize: FONT_SIZES.xs,
  },

  // ── Web fallback ──────────────────────────────────────────────
  webList: {
    flex: 1,
  },
  webListContent: {
    padding: SPACING.lg,
  },
  webHeader: {
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.xxl,
    fontWeight: "800",
    marginBottom: SPACING.lg,
  },
  webEmpty: {
    color: COLORS.textSecondary,
    fontSize: FONT_SIZES.md,
    textAlign: "center",
    marginTop: SPACING.xxl,
  },
  webCard: {
    flexDirection: "row",
    backgroundColor: COLORS.bgCard,
    borderRadius: BORDER_RADIUS.md,
    marginBottom: SPACING.md,
    overflow: "hidden",
  },
  webCardPressed: {
    backgroundColor: COLORS.bgCardHover,
  },
  webCardStripe: {
    width: 4,
  },
  webCardBody: {
    flex: 1,
    padding: SPACING.lg,
  },
  webCardName: {
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.lg,
    fontWeight: "700",
    marginBottom: 2,
  },
  webCardMeta: {
    color: COLORS.accentCyan,
    fontSize: FONT_SIZES.sm,
    marginBottom: 4,
  },
  webCardCoords: {
    color: COLORS.textMuted,
    fontSize: FONT_SIZES.xs,
    marginBottom: 2,
  },
  webCardMovement: {
    color: COLORS.textSecondary,
    fontSize: FONT_SIZES.xs,
  },
});
