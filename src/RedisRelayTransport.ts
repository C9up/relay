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
		/**
		 * How a failure is reported. Some clients — `@c9up/quasar` among them —
		 * REPORT a failed subscribe through a callback and resolve normally, so
		 * awaiting the call proves nothing: without this the transport believed
		 * it was subscribed and the instance silently missed every message
		 * published elsewhere.
		 */
		options?: { onError?: (error: unknown) => void },
	): unknown;
	/**
	 * Stop listening. The HANDLER is what must be passed: a client shared with
	 * the rest of the application drops every listener on the channel when it
	 * is not told which one to remove, so relay shutting down would silence
	 * whatever else was listening on the same channel.
	 */
	unsubscribe(
		channel: string,
		handler?: (message: string, channel: string) => void,
	): unknown;
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
	/**
	 * The wrapper actually registered with the client, per channel.
	 *
	 * Kept so `unsubscribe` can name it. Without it the only available call was
	 * "drop everything on this channel", which is not relay's to do on a client
	 * it shares.
	 */
	readonly #handlers = new Map<
		string,
		(message: string, channel: string) => void
	>();
	/**
	 * Whether closing the sockets is relay's business.
	 *
	 * `false` for a connection relay merely looked up — a named `@c9up/quasar`
	 * connection belongs to the application, and `quit()` on it closes BOTH its
	 * sockets, taking the cache, the sessions and the queues down with relay.
	 */
	readonly #ownsConnection: boolean;
	#resolved: Promise<RelayPubSubClient> | undefined;

	/**
	 * @param ownsConnection Whether closing the sockets is relay's business.
	 *   Defaults to `false`: a client relay was handed belongs to whoever
	 *   handed it over, and closing someone else's connection is the worse
	 *   mistake of the two.
	 */
	constructor(client: RelayPubSubResolver, ownsConnection = false) {
		this.#source = client;
		this.#ownsConnection = ownsConnection;
	}

	/**
	 * The client, resolved once — but a FAILED resolution is not kept.
	 *
	 * Caching the rejected promise meant one blip at start-up was permanent:
	 * every later publish and subscribe rejected instantly, with the original
	 * error, long after Redis came back. The in-flight promise is still shared,
	 * so concurrent callers do not each open a connection; only the failure is
	 * forgotten, so the next call gets to try again.
	 */
	#client(): Promise<RelayPubSubClient> {
		if (!this.#resolved) {
			const attempt = Promise.resolve(
				typeof this.#source === "function" ? this.#source() : this.#source,
			).catch((err: unknown) => {
				// Forget it only while it is still the current attempt, so a
				// retry already under way is not cleared out from under itself.
				if (this.#resolved === attempt) this.#resolved = undefined;
				throw err;
			});
			this.#resolved = attempt;
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
		const wrapper = (raw: string): void => {
			// Anything unreadable is dropped rather than thrown: this runs inside
			// the client's own message loop, where a throw takes down the whole
			// subscription and every later broadcast with it. Relay ignores what
			// it does not recognise anyway.
			const parsed = parseMessage(raw);
			if (parsed !== undefined) handler(parsed);
		};
		this.#handlers.set(channel, wrapper);
		// Turn a reported failure back into a rejection. A client that resolves
		// after failing leaves the caller with no way to tell the two apart.
		let reported: unknown;
		await client.subscribe(channel, wrapper, {
			onError: (error) => {
				reported = error;
			},
		});
		if (reported !== undefined) {
			this.#handlers.delete(channel);
			throw reported instanceof Error ? reported : new Error(String(reported));
		}
	}

	async unsubscribe(channel: string): Promise<void> {
		// NOTHING to remove means nothing to call. `unsubscribe(channel)` with no
		// handler means "drop every listener on this channel" on a shared client,
		// so calling it when relay never subscribed — a failed `ready()`, or a
		// second shutdown — would cut the cache's and the sessions' listeners
		// instead of relay's. Reaching the client at all would also connect one
		// just to disconnect it.
		const wrapper = this.#handlers.get(channel);
		if (wrapper === undefined) return;
		this.#handlers.delete(channel);
		const client = await this.#client();
		await client.unsubscribe(channel, wrapper);
	}

	/**
	 * Close the connection — only one relay OWNS, only when the client has a
	 * way to, and only when one was ever opened so a shutdown does not connect
	 * just to disconnect.
	 *
	 * A borrowed connection is left alone. `quit()` closes both sockets of a
	 * `@c9up/quasar` connection, so relay stopping used to take down the cache,
	 * the sessions and the queues that shared it.
	 */
	async disconnect(): Promise<void> {
		if (!this.#ownsConnection || !this.#resolved) return;
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
