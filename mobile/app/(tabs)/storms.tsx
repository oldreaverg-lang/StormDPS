/**
 * Storms list screen — browse active and historical storms.
 */

import { useState, useCallback } from "react";
import {
  View, Text, TextInput, FlatList, Pressable,
  StyleSheet, ActivityIndicator, RefreshControl,
} from "react-native";
import { useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { COLORS, SPACING, FONT_SIZES, BORDER_RADIUS, categoryColor } from "@/constants/theme";
import { useActiveStorms, useStormSearch } from "@/hooks/useStorms";
import type { StormSummary } from "@/types/storm";

export default function StormsScreen() {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [isRefreshing, setIsRefreshing] = useState(false);
  const { data: active, isLoading: loadingActive, error: errorActive, refetch: refetchActive } = useActiveStorms();
  const { data: results, isLoading: loadingSearch, error: errorSearch, refetch: refetchSearch } = useStormSearch(query);

  const storms = query.length >= 2 ? results : active;
  const isLoading = query.length >= 2 ? loadingSearch : loadingActive;
  const error = query.length >= 2 ? errorSearch : errorActive;
  const refetch = query.length >= 2 ? refetchSearch : refetchActive;

  const handleRefresh = useCallback(async () => {
    setIsRefreshing(true);
    try {
      await refetch?.();
    } finally {
      setIsRefreshing(false);
    }
  }, [refetch]);

  const handlePress = useCallback(
    (id: string) => router.push(`/storm/${id}`),
    [router]
  );

  const renderStorm = useCallback(
    ({ item }: { item: StormSummary }) => (
      <Pressable
        style={({ pressed }) => [styles.card, pressed && styles.cardPressed]}
        onPress={() => handlePress(item.id)}
      >
        <View style={styles.cardHeader}>
          <Text style={styles.stormName}>{item.name}</Text>
          <View style={[styles.catBadge, { backgroundColor: categoryColor(item.classification) }]}>
            <Text style={styles.catText}>
              {item.classification}
            </Text>
          </View>
        </View>
        <View style={styles.cardRow}>
          <Text style={styles.detailText}>
            {item.intensity_knots !== null ? `${item.intensity_knots} kt` : "N/A"} {item.pressure_mb !== null ? `\u00B7 ${item.pressure_mb} mb` : ""}
          </Text>
          {item.movement_speed_knots !== null && (
            <Text style={styles.yearText}>{item.movement_speed_knots} kt {item.movement_direction_deg}°</Text>
          )}
        </View>
      </Pressable>
    ),
    [handlePress]
  );

  return (
    <View style={styles.container}>
      {/* Search bar */}
      <View style={styles.searchContainer}>
        <Ionicons name="search" size={18} color={COLORS.textMuted} style={styles.searchIcon} />
        <TextInput
          style={styles.searchInput}
          placeholder="Search storms (e.g. Katrina, 2017, Atlantic)"
          placeholderTextColor={COLORS.textMuted}
          value={query}
          onChangeText={setQuery}
          autoCorrect={false}
          returnKeyType="search"
        />
        {query.length > 0 && (
          <Pressable onPress={() => setQuery("")}>
            <Ionicons name="close-circle" size={18} color={COLORS.textMuted} />
          </Pressable>
        )}
      </View>

      {/* Results */}
      {isLoading ? (
        <ActivityIndicator size="large" color={COLORS.accentCyan} style={styles.loader} />
      ) : error ? (
        <View style={styles.errorContainer}>
          <Text style={styles.errorText}>
            {error instanceof Error ? error.message : "Failed to load storms"}
          </Text>
          <Pressable onPress={handleRefresh} style={styles.retryButton}>
            <Text style={styles.retryButtonText}>Retry</Text>
          </Pressable>
        </View>
      ) : (
        <FlatList
          data={storms}
          keyExtractor={(item) => item.id}
          renderItem={renderStorm}
          contentContainerStyle={styles.list}
          refreshControl={
            <RefreshControl
              refreshing={isRefreshing}
              onRefresh={handleRefresh}
              tintColor={COLORS.accentCyan}
            />
          }
          ListEmptyComponent={
            <Text style={styles.emptyText}>
              {query.length >= 2 ? "No storms found" : "No active storms"}
            </Text>
          }
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bgPrimary },
  searchContainer: {
    flexDirection: "row",
    alignItems: "center",
    backgroundColor: COLORS.bgCard,
    marginHorizontal: SPACING.lg,
    marginTop: SPACING.md,
    marginBottom: SPACING.sm,
    borderRadius: BORDER_RADIUS.md,
    paddingHorizontal: SPACING.md,
    height: 44,
  },
  searchIcon: { marginRight: SPACING.sm },
  searchInput: {
    flex: 1,
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.md,
  },
  list: { padding: SPACING.lg, paddingTop: SPACING.sm },
  card: {
    backgroundColor: COLORS.bgCard,
    borderRadius: BORDER_RADIUS.lg,
    padding: SPACING.lg,
    marginBottom: SPACING.md,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  cardPressed: { backgroundColor: COLORS.bgCardHover },
  cardHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: SPACING.xs,
  },
  stormName: {
    color: COLORS.textPrimary,
    fontSize: FONT_SIZES.lg,
    fontWeight: "700",
  },
  catBadge: {
    paddingHorizontal: SPACING.sm,
    paddingVertical: 2,
    borderRadius: BORDER_RADIUS.sm,
  },
  catText: { color: "#fff", fontSize: FONT_SIZES.xs, fontWeight: "700" },
  cardRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  detailText: { color: COLORS.textSecondary, fontSize: FONT_SIZES.sm },
  yearText: { color: COLORS.textMuted, fontSize: FONT_SIZES.sm },
  activeBadge: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: SPACING.sm,
  },
  activeDot: {
    width: 6,
    height: 6,
    borderRadius: 3,
    backgroundColor: COLORS.statusActive,
    marginRight: SPACING.xs,
  },
  activeText: { color: COLORS.statusActive, fontSize: FONT_SIZES.xs, fontWeight: "600" },
  loader: { marginTop: SPACING.xxl },
  emptyText: {
    color: COLORS.textMuted,
    fontSize: FONT_SIZES.md,
    textAlign: "center",
    marginTop: SPACING.xxl,
  },
  errorContainer: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    marginHorizontal: SPACING.lg,
  },
  errorText: {
    color: COLORS.statusError || "#ef4444",
    fontSize: FONT_SIZES.md,
    textAlign: "center",
    marginBottom: SPACING.lg,
  },
  retryButton: {
    backgroundColor: COLORS.accentBlue,
    paddingHorizontal: SPACING.lg,
    paddingVertical: SPACING.sm,
    borderRadius: BORDER_RADIUS.md,
  },
  retryButtonText: {
    color: "#fff",
    fontSize: FONT_SIZES.md,
    fontWeight: "600",
  },
});
