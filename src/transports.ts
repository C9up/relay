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
		/**
		 * Close the connection when relay shuts down. Default `false`.
		 *
		 * A named `@c9up/quasar` connection is NEVER relay's to close, whatever
		 * this says: it belongs to the application, and closing it takes down
		 * the cache, the sessions and the queues that share it. Set this only
		 * for a client opened for relay and used by nothing else.
		 */
		owned?: boolean;
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
		// A name is a lookup into someone else's connection manager, so it is
		// borrowed by construction and the flag cannot override that.
		const owned =
			typeof connection === "string" ? false : options.owned === true;
		return () => new RedisRelayTransport(client, owned);
	},
};
