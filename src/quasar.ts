/**
 * Resolving a Redis connection by name, from `@c9up/quasar`.
 *
 * The loading, the shape check and the messages are the same in every package
 * that offers a Redis-backed option, so they are vendored rather than written
 * again: `src/vendor/quasarConnection.ts`, generated from one source. What is
 * specific to this package — the commands it issues, and what it does with
 * them — stays here, because that is the part a reader needs.
 */

import type { RelayPubSubClient } from "./RedisRelayTransport.js";
import { quasarConnection as loadQuasarConnection } from "./vendor/quasarConnection.js";

// A transport publishes, subscribes and unsubscribes. Nothing else.
const REQUIRED = ["publish", "subscribe", "unsubscribe"] as const;

/** The named connection, once it is known to carry what this package issues. */
export async function quasarConnection(
	name?: string,
): Promise<RelayPubSubClient> {
	return loadQuasarConnection<RelayPubSubClient>({
		pkg: "relay",
		name,
		required: REQUIRED,
		what: "the relay transport",
		raise: (_reason, message, cause) => new Error(message, { cause }),
	});
}
