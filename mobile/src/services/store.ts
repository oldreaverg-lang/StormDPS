/**
 * Global state management using Zustand.
 * Lightweight alternative to Redux — works identically on web/iOS/Android.
 */

import { create } from "zustand";

interface AppState {
  /** Connected status */
  isApiConnected: boolean;
  setApiConnected: (connected: boolean) => void;

  /** User preferences */
  useMetric: boolean;
  setUseMetric: (val: boolean) => void;
}

export const useAppStore = create<AppState>((set) => ({
  isApiConnected: false,
  setApiConnected: (connected) => set({ isApiConnected: connected }),

  useMetric: true,
  setUseMetric: (val) => set({ useMetric: val }),
}));
