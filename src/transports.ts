/**
 * The bus factories a config file names.
 *
 *   import { defineConfig, transports } from '@c9up/relay'
 *
 *   export default defineConfig({
 *     transport: transports.redis({ connection: 'main' }),
 *   })
 *
 * A transport is what carries a broadcast from the instance that made it to
 * the SSE clients attached to every other instance. Leave it out and relay is
 * single-instance: broadcasts reach the clients of the instance they were
 * made on, and no further.
 */

import { quasarConnection } from "./quasar.js";
import {
	RedisRelayTransport,
	type RelayPubSubResolver,
} from "./RedisRelayTransport.js";
import type { RelayTransport } from "./Relay.js";

/** A transport, built when the Relay that uses it is constructed. */
export type RelayTransportFactory = () => RelayTransport;

export const transports = {
	/**
	 * Redis pub/sub. `connection` takes a client, a function answering one, or
	 * the NAME of a `@c9up/quasar` connection — the last of which is resolved at
	 * runtime without relay importing quasar, which stays an optional peer.
	 */
	redis(options: {
		connection: RelayPubSubResolver | string;
	}): RelayTransportFactory {
		// Read into a local before the closure: narrowing a mutable property
		// does not survive into a deferred body, and the only way to keep the
		// property was to assert the type back — a claim about an object the
		// caller still holds and can change.
		const connection = options.connection;
		const client: RelayPubSubResolver =
			typeof connection === "string"
				? () => quasarConnection(connection)
				: connection;
		return () => new RedisRelayTransport(client);
	},
};
