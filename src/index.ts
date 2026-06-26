/**
 * @c9up/relay — Realtime SSE SDK for the Ream framework.
 *
 * Adonis-Transmit-style API: `authorize` channels, `broadcast` events,
 * `on` lifecycle hooks. The provider auto-registers SSE routes at
 * `/__relay/events`, `/__relay/subscribe`, `/__relay/unsubscribe`.
 *
 * @implements MISS-20
 */

import type { RelayConfig } from "./Relay.js";

/**
 * Author-time config helper for `config/relay.ts` — AdonisJS `defineConfig`
 * parity (Transmit ships the same). Identity at runtime; the generic preserves
 * literal types for inference.
 */
export function defineConfig<T extends RelayConfig>(config: T): T {
	return config;
}

export { Hub, type HubContext, type HubGuardOptions } from "./Hub.js";
export {
	type ChannelAuthorizer,
	Relay,
	type RelayAuth,
	type RelayConfig,
	type RelayContext,
	type RelayLifecycleEvents,
	type RelayRouteBuilder,
	type RelayRouteCustomizer,
	type RelaySseStream,
	type SubscribeFailure,
	type SubscribeResult,
	type SubscribeSuccess,
} from "./Relay.js";
export { default as RelayProvider } from "./RelayProvider.js";
export {
	type NegotiateResponse,
	SignalRAdapter,
	type SignalRMessageType,
} from "./SignalRAdapter.js";
