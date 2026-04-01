/**
 * Root layout — wraps the entire app with providers.
 * Uses Expo Router for file-based routing (works on web/iOS/Android).
 */

import { useEffect } from "react";
import { StatusBar } from "expo-status-bar";
import { Stack } from "expo-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { StyleSheet } from "react-native";
import * as SplashScreen from "expo-splash-screen";
import { COLORS } from "@/constants/theme";
import { useAppStore } from "@/services/store";
import { healthCheck } from "@/services/api";

SplashScreen.preventAutoHideAsync();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      refetchOnWindowFocus: true,
    },
  },
});

export default function RootLayout() {
  const setApiConnected = useAppStore((s) => s.setApiConnected);

  useEffect(() => {
    // Check API connectivity on mount
    healthCheck()
      .then((ok) => {
        setApiConnected(ok);
        SplashScreen.hideAsync();
      })
      .catch(() => {
        setApiConnected(false);
        SplashScreen.hideAsync();
      });
  }, []);

  return (
    <GestureHandlerRootView style={styles.container}>
      <QueryClientProvider client={queryClient}>
        <StatusBar style="light" />
        <Stack
          screenOptions={{
            headerStyle: { backgroundColor: COLORS.bgSecondary },
            headerTintColor: COLORS.textPrimary,
            headerTitleStyle: { fontWeight: "700" },
            contentStyle: { backgroundColor: COLORS.bgPrimary },
          }}
        >
          <Stack.Screen
            name="(tabs)"
            options={{ headerShown: false }}
          />
          <Stack.Screen
            name="storm/[id]"
            options={{ title: "Storm Details", presentation: "card" }}
          />
        </Stack>
      </QueryClientProvider>
    </GestureHandlerRootView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.bgPrimary,
  },
});
