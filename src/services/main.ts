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

let _instance: Relay | undefined;

/** @internal Replace the singleton (used by the provider on first boot). */
export function _setRelay(instance: Relay): void {
	_instance = instance;
}

/** @internal Get the underlying instance (or `undefined` pre-boot). */
export function _getRelay(): Relay | undefined {
	return _instance;
}

const relay: Relay = new Proxy({} as Relay, {
	get(_target, prop) {
		if (!_instance) {
			// Lazy default — apps that never registered the provider get a
			// usable broadcast surface (no-op until a client connects).
			_instance = new Relay();
		}
		const value = Reflect.get(_instance, prop, _instance);
		return typeof value === "function" ? value.bind(_instance) : value;
	},
});

export default relay;
