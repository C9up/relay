/**
 * Redis-backed cross-instance bus — the missing half of `RelayTransport`.
 *
 * Relay declares the contract and re-delivers whatever arrives on it, but
 * shipped no implementation: an application running more than one instance
 * had to write its own publisher/subscriber, and until it did, a broadcast
 * reached only the SSE clients attached to the instance that made it. Which
 * looks like it works, right up to the second replica.
 *
 * No import of a Redis package. The client is taken structurally — publish,
 * subscribe, unsubscribe — so this works against a `@c9up/quasar` connection
 * without relay depending on quasar, which is an optional peer.
 */

import type { RelayTransport } from "./Relay.js";

/** The pub/sub commands this issues. Any client answering them will do. */
export interface RelayPubSubClient {
	publish(channel: string, message: string): unknown;
	subscribe(
		channel: string,
		handler: (message: string, channel: string) => void,
	): unknown;
	unsubscribe(channel: string): unknown;
	/** Closes the sockets, when the client has a way to. */
	quit?(): unknown;
}

/**
 * How the transport gets its client: the client itself, or something that
 * answers with one.
 *
 * The resolver form is what a config file needs. `config/relay.ts` is read
 * before the application boots, so the connection does not exist yet and
 * cannot be awaited there — a function defers the lookup to the first
 * broadcast.
 */
export type RelayPubSubResolver =
	| RelayPubSubClient
	| (() => RelayPubSubClient | Promise<RelayPubSubClient>);

export class RedisRelayTransport implements RelayTransport {
	readonly #source: RelayPubSubResolver;
	#resolved: Promise<RelayPubSubClient> | undefined;

	constructor(client: RelayPubSubResolver) {
		this.#source = client;
	}

	/** The client, resolved once and kept. */
	#client(): Promise<RelayPubSubClient> {
		if (!this.#resolved) {
			this.#resolved = Promise.resolve(
				typeof this.#source === "function" ? this.#source() : this.#source,
			);
		}
		return this.#resolved;
	}

	async publish(channel: string, message: unknown): Promise<void> {
		const client = await this.#client();
		await client.publish(channel, JSON.stringify(message));
	}

	async subscribe(
		channel: string,
		handler: (message: unknown) => void,
	): Promise<void> {
		const client = await this.#client();
		await client.subscribe(channel, (raw) => {
			// Anything unreadable is dropped rather than thrown: this runs inside
			// the client's own message loop, where a throw takes down the whole
			// subscription and every later broadcast with it. Relay ignores what
			// it does not recognise anyway.
			const parsed = parseMessage(raw);
			if (parsed !== undefined) handler(parsed);
		});
	}

	async unsubscribe(channel: string): Promise<void> {
		const client = await this.#client();
		await client.unsubscribe(channel);
	}

	/**
	 * Close the connection, when the client has a way to — and only when one was
	 * ever opened, so a shutdown does not connect just to disconnect.
	 */
	async disconnect(): Promise<void> {
		if (!this.#resolved) return;
		const client = await this.#resolved;
		await client.quit?.();
	}
}

function parseMessage(raw: string): unknown {
	try {
		return JSON.parse(raw);
	} catch {
		return undefined;
	}
}
