/**
 * Resolving a Redis connection by name, from `@c9up/quasar`.
 *
 * Relay does not depend on quasar: it is an optional peer, and this module
 * never imports it statically — the specifier is built at runtime so the
 * TypeScript build stays free of it too.
 *
 * The connection is duck-typed before use rather than asserted: a client
 * missing a command would otherwise fail on the first broadcast, far from the
 * cause.
 */

import type { RelayPubSubClient } from "./RedisRelayTransport.js";

/** The slice of quasar's manager this needs: a connection, by name. */
interface ConnectionSource {
	connection(name?: string): unknown;
}

/** The commands the transport issues. A client missing one cannot serve it. */
const REQUIRED = [
	"publish",
	"subscribe",
	"unsubscribe",
] as const satisfies readonly (keyof RelayPubSubClient)[];

function isConnectionSource(value: unknown): value is ConnectionSource {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof Reflect.get(value, "connection") === "function"
	);
}

function missingCommands(value: unknown): string[] {
	if (typeof value !== "object" || value === null) return [...REQUIRED];
	return REQUIRED.filter(
		(command) => typeof Reflect.get(value, command) !== "function",
	);
}

/** A client is one when it answers every command the transport issues. */
function isPubSubClient(value: unknown): value is RelayPubSubClient {
	return missingCommands(value).length === 0;
}

/** Resolve the named quasar connection, or say precisely what is missing. */
export async function quasarConnection(
	name?: string,
): Promise<RelayPubSubClient> {
	// Built at runtime: a static import would put quasar in relay's build graph,
	// and it is optional.
	const specifier = "@c9up/quasar/services/main";
	let module: { default?: unknown };
	try {
		module = (await import(/* @vite-ignore */ specifier)) as {
			default?: unknown;
		};
	} catch (cause) {
		throw new Error(
			"Naming a Redis connection needs @c9up/quasar, which is not installed. " +
				"Install it, or pass a client instead of a connection name.",
			{ cause },
		);
	}

	const manager = module.default;
	if (!isConnectionSource(manager)) {
		throw new Error(
			"@c9up/quasar/services/main does not expose connection(name).",
		);
	}

	const connection = manager.connection(name);
	if (!isPubSubClient(connection)) {
		throw new Error(
			`The quasar connection${name ? ` '${name}'` : ""} is missing ${missingCommands(connection).join(", ")}, which the relay bus issues.`,
		);
	}
	return connection;
}
