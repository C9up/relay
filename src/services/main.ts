/**
 * Default `Relay` singleton — import from anywhere for broadcast +
 * authorization config. Mirrors Adonis's
 * `import transmit from '@adonisjs/transmit/services/main'` ergonomics.
 *
 * The instance is lazily initialised on first access. The provider
 * (`@c9up/relay/provider`) wires the auto-registered SSE routes
 * against the same instance so route handlers and `broadcast` calls
 * always meet on the same map.
 */

import { Relay } from "../Relay.js";

let instance: Relay | undefined;
/**
 * Whether `instance` is the lazy default the Proxy below built, rather than one
 * a provider bound. Only that one's registrations are rescued when the slot
 * changes hands: replacing a real instance with another — a hot reload, a
 * second Ignitor in a test — must hand over a clean one, not drag the previous
 * application's authorizers into it.
 */
let instanceIsLazyDefault = false;

/**
 * @internal Replace the singleton (used by the provider on first boot).
 *
 * Whatever had been registered on the instance being replaced moves onto the
 * new one. The Proxy below builds a default on first access, so an
 * `authorize()` that ran before the provider bound its own instance was
 * recorded on an object nothing ever served from — and the channel answered
 * `E_CHANNEL_NO_AUTHORIZER`, with both calls reading identically at the call
 * site.
 */
export function setRelay(value: Relay): void {
	if (instanceIsLazyDefault && instance !== undefined && instance !== value) {
		value.adoptRegistrationsFrom(instance);
	}
	instance = value;
	instanceIsLazyDefault = false;
}

/** @internal Get the underlying instance (or `undefined` pre-boot). */
export function getRelay(): Relay | undefined {
	return instance;
}

const relay: Relay = new Proxy({} as Relay, {
	get(_target, prop) {
		if (!instance) {
			// Lazy default — apps that never registered the provider get a
			// usable broadcast surface (no-op until a client connects).
			instance = new Relay();
			instanceIsLazyDefault = true;
		}
		const value = Reflect.get(instance, prop, instance);
		return typeof value === "function" ? value.bind(instance) : value;
	},
});

export default relay;
