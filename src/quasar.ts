/**
 * Resolving a Redis connection by name, from `@c9up/quasar`.
 *
 * The loading, the shape check and the messages are the same in every package
 * that offers a Redis-backed option, so they are vendored rather than written
 * again: `src/vendor/quasarConnection.ts`, generated from one source. What is
 * specific to this package — the commands it issues, and what it does with
 * them — stays here, because that is the part a reader needs.
 */

import { E_RELAY_QUASAR_MISSING, E_RELAY_REDIS_INCOMPLETE } from "./errors.js";
import type { RelayPubSubClient } from "./RedisRelayTransport.js";
import {
	hasCommands,
	quasarConnection as loadQuasarConnection,
} from "./vendor/quasarConnection.js";

// A transport publishes, subscribes and unsubscribes. Nothing else.
const REQUIRED = ["publish", "subscribe", "unsubscribe"] as const;

/**
 * The connection is handed over only once it carries every command above.
 */
function isRelayPubSubClient(value: unknown): value is RelayPubSubClient {
	return hasCommands(value, REQUIRED);
}

/** The named connection, once it is known to carry what this package issues. */
export async function quasarConnection(
	name?: string,
): Promise<RelayPubSubClient> {
	return loadQuasarConnection({
		pkg: "relay",
		name,
		required: REQUIRED,
		isConnection: isRelayPubSubClient,
		what: "the relay transport",
		raise: (reason, message, cause) =>
			reason === "quasar-missing"
				? new E_RELAY_QUASAR_MISSING([message], { cause })
				: new E_RELAY_REDIS_INCOMPLETE([message], { cause }),
	});
}
