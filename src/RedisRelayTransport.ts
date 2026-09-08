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
	 * Per channel, the wrapper registered with the client for each of relay's
	 * handlers.
	 *
	 * Kept so `unsubscribe` can name one. Without it the only available call
	 * was "drop everything on this channel", which is not relay's to do on a
	 * client it shares — and a single wrapper per channel was not enough
	 * either: the client STACKS subscriptions (`@c9up/quasar` keeps a `Set`),
	 * so a second subscribe replaced the map entry while both wrappers stayed
	 * registered. The first became untrackable and every message arrived twice.
	 */
	readonly #handlers = new Map<
		string,
		Map<
			(message: unknown) => void,
			Array<(message: string, channel: string) => void>
		>
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
		// Recorded BEFORE the call, because a client that registers the wrapper
		// and then fails still has it — and forgotten again on ANY failure, not
		// just the reported kind. A direct rejection used to leave the wrapper
		// recorded: `unsubscribe` would then ask the client to remove something
		// it may never have registered, and a retry was refused by bookkeeping
		// for a subscription that never happened.
		// APPENDED, not replaced. Subscribing the same callback twice registers
		// two wrappers with the client; keeping one meant the second overwrote
		// the only way to name the first, and an unsubscribe removed one of the
		// two listeners it was asked about.
		const wrappers = this.#handlers.get(channel) ?? new Map();
		wrappers.set(handler, [...(wrappers.get(handler) ?? []), wrapper]);
		this.#handlers.set(channel, wrappers);
		// Turn a reported failure back into a rejection. A client that resolves
		// after failing leaves the caller with no way to tell the two apart.
		let reported: unknown;
		try {
			await client.subscribe(channel, wrapper, {
				onError: (error) => {
					reported = error;
				},
			});
		} catch (error) {
			await this.#rollback(client, channel, handler, wrapper);
			throw error;
		}
		if (reported !== undefined) {
			await this.#rollback(client, channel, handler, wrapper);
			throw reported instanceof Error ? reported : new Error(String(reported));
		}
	}

	/**
	 * Undo a subscribe that failed, because it may have half-succeeded.
	 *
	 * A client is free to register the callback and THEN fail — an ambiguous
	 * network result, or a duck-typed adapter that does its bookkeeping first.
	 * Simply forgetting the wrapper left it live and unnameable: the later
	 * unsubscribe removed nothing, and every broadcast still reached it.
	 *
	 * The record is dropped only once the client has accepted the removal. If
	 * that fails too, keeping it is the only way a shutdown can still name what
	 * may be listening.
	 */
	async #rollback(
		client: RelayPubSubClient,
		channel: string,
		handler: (message: unknown) => void,
		wrapper: (message: string, channel: string) => void,
	): Promise<void> {
		try {
			await client.unsubscribe(channel, wrapper);
			this.#forget(channel, handler, wrapper);
		} catch {
			// Left recorded on purpose — see above.
		}
	}

	/**
	 * Stop listening — one named handler, or every one relay put on `channel`.
	 *
	 * NOTHING to remove means nothing to call. `unsubscribe(channel)` with no
	 * wrapper means "drop every listener on this channel" on a shared client,
	 * so calling it when relay never subscribed — a failed `ready()`, or a
	 * second shutdown — would cut the cache's and the sessions' listeners
	 * instead of relay's. Reaching the client at all would also connect one
	 * just to disconnect it.
	 */
	async unsubscribe(
		channel: string,
		handler?: (message: unknown) => void,
	): Promise<void> {
		const wrappers = this.#handlers.get(channel);
		if (wrappers === undefined) return;

		// Chosen first, DROPPED last. Clearing the bookkeeping before asking the
		// client left a live subscription nothing could name again when the
		// call failed — neither to retry it nor to remove it at shutdown.
		const doomed: Array<
			[(message: unknown) => void, (message: string, channel: string) => void]
		> = [];
		if (handler === undefined) {
			for (const [owner, list] of wrappers) {
				for (const wrapper of list) doomed.push([owner, wrapper]);
			}
		} else {
			for (const wrapper of wrappers.get(handler) ?? []) {
				doomed.push([handler, wrapper]);
			}
		}
		if (doomed.length === 0) return;

		const client = await this.#client();
		for (const [owner, wrapper] of doomed) {
			await client.unsubscribe(channel, wrapper);
			// One at a time, so a failure halfway through leaves the rest
			// nameable rather than losing them all with the one that refused.
			this.#forget(channel, owner, wrapper);
		}
	}

	/** Drop one registration without touching the client. */
	#forget(
		channel: string,
		handler: (message: unknown) => void,
		wrapper?: (message: string, channel: string) => void,
	): void {
		const wrappers = this.#handlers.get(channel);
		if (wrappers === undefined) return;
		if (wrapper === undefined) {
			wrappers.delete(handler);
		} else {
			const rest = (wrappers.get(handler) ?? []).filter((w) => w !== wrapper);
			if (rest.length > 0) wrappers.set(handler, rest);
			else wrappers.delete(handler);
		}
		if (wrappers.size === 0) this.#handlers.delete(channel);
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
		// Every listener THIS transport put on the bus comes off, whoever owns
		// the connection. A borrowed one is not relay's to close — but the
		// listeners are relay's to remove, and leaving them meant a shutdown
		// that released nothing at all on the connection an application shares.
		if (this.#resolved) {
			for (const channel of [...this.#handlers.keys()]) {
				await this.unsubscribe(channel);
			}
		}
		if (!this.#ownsConnection || !this.#resolved) return;
		// FORGOTTEN before it is closed. Left cached, the quit client was handed
		// straight back to the next `subscribe` — so an application that stops
		// and starts again in one process, a hot reload or a test, came up on a
		// socket that was already shut and never reached the bus again.
		const pending = this.#resolved;
		this.#resolved = undefined;
		const client = await pending;
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
