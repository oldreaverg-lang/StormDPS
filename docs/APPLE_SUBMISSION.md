# Apple App Store submission — paused mid-flow

## Current state

- `app.json` renamed to "StormDPS", bundle ID `com.stormdps.app`, slug `stormdps`
- Stripped unused capabilities (no location, no background modes, no notifications — declared but unused; would fail Apple Guideline 2.5.4)
- Icon and splash set to 1024×1024 brand logo (Apple-spec RGB, no alpha)
- Privacy policy live at https://stormdps.com/privacy
- Mobile `package.json` deps cleaned (expo-location, expo-notifications removed)

## Next user action (BLOCKING)

```
cd mobile
npx eas-cli login        # interactive — user's Expo account
npx eas-cli init         # writes projectId into app.json
```

TTY-interactive, cannot be done from inside an agent session. User must run this.

## Then user runs

```
npx eas-cli build --platform ios --profile preview
```

Builds in EAS Cloud (10–15 min), auto-uploads to App Store Connect.

## Then user adds testers via App Store Connect → TestFlight → Internal Testing

- Self (iPhone 13 — testing only, not screenshot device)
- Friend with iPhone Pro Max (6.9" screenshots — Apple requires this size)
- Dad with iPad (iPad screenshots — required since `supportsTablet: true`)

## Once testers screenshot

Agent writes App Store Connect listing copy:
- Description (max 4000 chars)
- Subtitle (max 30 chars)
- Promotional text (max 170 chars)
- Keywords (max 100 chars comma-separated)
- What's-new, support URL (https://stormdps.com/about), marketing URL (https://stormdps.com)
- Age rating: 4+
- Privacy nutrition labels: Data Not Collected (verified — no analytics SDKs in mobile code)

## Weather-app review risk

Apple scrutinizes weather/safety apps. The web footer has "Experimental & Educational — defer to NHC." The **mobile app needs the same disclaimer** somewhere obvious — probably first-launch screen + Settings tab. Audit before submitting.

## Notifications (deferred to v1.1)

User wants push notifications when DPS score changes. Building requires: APNs auth key from Apple Developer portal, server-side push token storage + monitoring job + sender service, mobile permission UI + subscribe UI. ~1–2 weeks. Ship v1 first.
