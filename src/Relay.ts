/**
 * Relay — realtime SSE SDK for the Ream framework. Shape mirrors
 * `@adonisjs/transmit`: app code is limited to ONE provider entry + a
 * handful of `authorize` / `broadcast` calls. The provider auto-
 * registers three routes against the host router:
 *
 *     GET  /__relay/events?uid=<client_uid>
 *     POST /__relay/subscribe   { uid, channel }
 *     POST /__relay/unsubscribe { uid, channel }
 *
 * The GET endpoint opens an SSE stream via `ctx.response.sse()` (built
 * on top of the HyperServer NAPI streaming primitives — see
 * `packages/ream/src/http/SseStream.ts`). The two POST endpoints
 * manage channel subscriptions for a previously-connected client.
 *
 * Usage:
 *
 *     // start/realtime.ts
 *     import relay from '@c9up/relay/services/main'
 *
 *     relay.authorize<{ id: string }>('users/:id/notifications', (ctx, { id }) => {
 *       return ctx.auth?.user?.id === id
 *     })
 *
 *     // controllers
 *     import relay from '@c9up/relay/services/main'
 *
 *     relay.broadcast(`users/${userId}/notifications`, { type: 'post.published' })
 */

import { randomUUID } from "node:crypto";

/**
 * Event name the keep-alive goes out under. Prefixed so it cannot collide with
 * an application channel, and named for this package rather than borrowed from
 * Transmit's `$$transmit/ping`.
 */
export const PING_CHANNEL = "$$relay/ping";

/** Slim shape of the per-client SSE transport. Matches `SseStream`. */
export interface RelaySseStream {
	id: string;
	isOpen(): boolean;
	send(event: string, data: unknown, eventId?: string): Promise<boolean>;
	onClose(cb: () => void): void;
	end(): Promise<void>;
}

/**
 * What {@link Relay.hub} accepts. Structural on purpose: the relay records a
 * hub and hands it to the transport, and never calls into it itself.
 */
export type HubLike = object;

/** Likewise for an optional pre-built SignalR adapter. */
export type SignalRAdapterLike = object;

/** Auth state forwarded to authorize callbacks. */
export interface RelayAuth {
	isAuthenticated: boolean;
	user?: { id: string; [key: string]: unknown };
	roles?: string[];
	permissions?: string[];
}

export interface RelayContext {
	auth?: RelayAuth;
}

export type ChannelAuthorizer<TParams = Record<string, string>> = (
	ctx: RelayContext,
	params: TParams,
) => Promise<boolean> | boolean;

/**
 * Duck-typed message bus used to synchronize broadcasts across multiple
 * app instances (horizontal scaling). Mirrors Adonis Transmit's injected
 * `@boringnode/bus` `Transport`, but kept structural so relay never has to
 * import a concrete driver: objects exposing `publish`/`subscribe` work —
 * `@c9up/pulsar`, a Redis/NATS bus, or an in-memory fake in tests.
 *
 * Each broadcast is published on `transportChannel`; every instance
 * (including the originator) is subscribed and re-emits the payload to its
 * OWN local SSE clients. The re-emit is local-only — it never re-publishes,
 * so messages don't loop between instances.
 */
export interface RelayTransport {
	publish(channel: string, message: unknown): void | Promise<void>;
	subscribe(
		channel: string,
		handler: (message: unknown) => void,
	): void | Promise<void>;
	unsubscribe?(channel: string): void | Promise<void>;
	disconnect?(): void | Promise<void>;
}

export interface RelayConfig {
	/**
	 * Allow subscription to channels that have no authorizer registered.
	 * Default `false`.
	 *
	 * NAMED DEVIATION from Adonis Transmit, which allows them: its
	 * `verifyAccess` answers `true` the moment no secured definition matches
	 * the channel, so a channel nobody thought to authorize is world-readable.
	 * Default-deny is the opposite bet — a channel you forgot to cover refuses
	 * subscribers instead of serving them — and the flag is here for anyone who
	 * wants upstream's behavior back.
	 */
	allowUnauthorizedChannels?: boolean;
	/**
	 * Maximum concurrent SSE clients. Default 10 000 — bumps the soft
	 * cap before degraded behavior (new connects 503'd).
	 */
	maxClients?: number;
	/**
	 * Per-client channel subscription cap. Default 100. A bound is
	 * essential because client-supplied channel names land in the
	 * `channelIndex`; an unbounded cap would let one socket pin memory.
	 */
	maxChannelsPerClient?: number;
	/**
	 * Optional message bus for multi-instance broadcast sync. When set,
	 * every `broadcast(...)` is mirrored onto the bus and re-delivered to
	 * the SSE clients of every other instance. Absent → single-instance, no
	 * bus: a broadcast reaches the clients of the instance that made it, and
	 * no further.
	 *
	 * Takes a transport, or a factory answering one — which is what the
	 * `transports.*` helpers return, so a config file can name a bus it has no
	 * way of building yet.
	 */
	transport?: RelayTransport | (() => RelayTransport);
	/**
	 * Bus channel the broadcasts are published on. Default
	 * `relay::broadcast`. Mirrors Transmit's `transport.channel`.
	 */
	transportChannel?: string;
	/**
	 * Milliseconds between keep-alive frames sent to every connected client,
	 * or `false` for none. Default `false` — Transmit's `pingInterval`, same
	 * shape and same default.
	 *
	 * An SSE connection that carries no traffic is indistinguishable from a
	 * hung one to everything between the client and the process: nginx closes
	 * an idle upstream at 60 s by default, and most load balancers and mobile
	 * networks are less patient still. The client reconnects, so the symptom
	 * is not silence but a connection that drops and reopens forever, losing
	 * whatever was published in between. A periodic frame is what stops that.
	 */
	pingInterval?: number | false;
}

/** Cross-instance broadcast envelope carried over `RelayTransport`. */
interface RelayTransportMessage {
	type: "broadcast";
	channel: string;
	payload: unknown;
}

export interface RelayLifecycleEvents {
	connect: { uid: string };
	disconnect: { uid: string };
	subscribe: { uid: string; channel: string };
	unsubscribe: { uid: string; channel: string };
	broadcast: { channel: string; payload: unknown };
}

type LifecycleEventName = keyof RelayLifecycleEvents;
type LifecycleListener<E extends LifecycleEventName> = (
	evt: RelayLifecycleEvents[E],
) => void;

/**
 * Slim, duck-typed contract for the route-builder object the host
 * router hands back from `router.get(path, …)` / `router.post(…)`.
 * Matches the surface of `@c9up/ream`'s `Route` class — `middleware`,
 * `use`, `guard`, `role`, `permission` — without forcing the relay
 * package to import `@c9up/ream`.
 *
 * Apps consume this through `relay.registerRoutes((route) => { … })`
 * to attach auth middleware on the auto-registered SSE endpoints:
 *
 *   relay.registerRoutes((route) => {
 *     route.middleware('auth')
 *   })
 *
 * Mirrors AdonisJS's `transmit.registerRoutes((route) => route.middleware('auth'))`.
 */
export interface RelayRouteBuilder {
	middleware(...names: string[]): RelayRouteBuilder;
	use(...mw: unknown[]): RelayRouteBuilder;
	guard(...guards: string[]): RelayRouteBuilder;
	role(...roles: string[]): RelayRouteBuilder;
	permission(...permissions: string[]): RelayRouteBuilder;
}

export type RelayRouteCustomizer = (route: RelayRouteBuilder) => void;

interface RegisteredClient {
	uid: string;
	sse: RelaySseStream;
	channels: Set<string>;
	auth?: RelayAuth;
}

/**
 * Relay — single shared instance per app (use the
 * `@c9up/relay/services/main` re-export). Holds the active SSE clients,
 * channel subscriptions, and authorizer callbacks.
 */
export class Relay {
	#clients = new Map<string, RegisteredClient>();
	/** channel → set of uids subscribed to it (O(1) broadcast). */
	#channelIndex = new Map<string, Set<string>>();
	#authorizers = new Map<string, ChannelAuthorizer<Record<string, string>>>();
	#listeners: { [E in LifecycleEventName]?: Set<LifecycleListener<E>> } = {};
	#routeCustomizer?: RelayRouteCustomizer;
	/** Hubs mounted with {@link hub}, keyed by path. */
	readonly #hubs = new Map<
		string,
		{ path: string; hub: HubLike; adapter?: SignalRAdapterLike }
	>();
	readonly #config: Required<
		Pick<
			RelayConfig,
			"allowUnauthorizedChannels" | "maxClients" | "maxChannelsPerClient"
		>
	>;
	readonly #transport?: RelayTransport;
	readonly #transportChannel: string;
	readonly #pingTimer?: ReturnType<typeof setInterval>;

	constructor(config?: RelayConfig) {
		this.#config = {
			allowUnauthorizedChannels: config?.allowUnauthorizedChannels ?? false,
			maxClients: config?.maxClients ?? 10_000,
			maxChannelsPerClient: config?.maxChannelsPerClient ?? 100,
		};
		const transport = config?.transport;
		this.#transport = typeof transport === "function" ? transport() : transport;
		const channel = config?.transportChannel ?? "relay::broadcast";
		this.#transportChannel = channel;
		// Multi-instance sync: subscribe to the bus and re-deliver every remote
		// broadcast to THIS instance's local SSE clients. Local-only re-emit —
		// no re-publish — so a message published by instance A reaches B, C … but
		// never bounces back onto the bus. Mirrors Transmit's `#broadcastLocally`
		// off the transport subscription.
		// `subscribe` may answer synchronously or with a promise, and it is
		// called INSIDE the async function rather than before it:
		// `Promise.resolve(x())` runs `x()` first, so a transport that threw
		// synchronously — a bad URL, a client built without a connection —
		// escaped the constructor entirely and never reached the catch below.
		//
		// The channel is read from the local rather than the field, because a
		// deferred body cannot promise the compiler the field is assigned yet.
		void (async () =>
			this.#transport?.subscribe(channel, (message) => {
				if (isRelayTransportMessage(message)) {
					this.#deliver(message.channel, message.payload);
				}
			}))()
			// A subscribe that fails is how an instance stops hearing the others:
			// it keeps serving its own clients and quietly misses every message
			// published elsewhere. Unawaited, the rejection had nowhere to go, so
			// the split-brain was invisible.
			.catch((error: unknown) => {
				process.stderr.write(
					`[relay] transport subscribe failed — this instance will not receive messages published elsewhere: ${
						error instanceof Error ? error.message : String(error)
					}\n`,
				);
			});
		const ping = config?.pingInterval;
		if (typeof ping === "number" && ping > 0) {
			this.#pingTimer = setInterval(() => {
				this.#ping();
			}, ping);
			// Unreffed: a keep-alive is for the connections that exist, not a
			// reason for the process to stay up once nothing else is running.
			this.#pingTimer.unref();
		}
	}

	/**
	 * One frame to every connected client, whatever they subscribed to. The
	 * point is bytes on the wire, so it goes to the client and not through the
	 * channel index — a client that has subscribed to nothing yet is exactly
	 * the one an idle-timeout would cut.
	 */
	#ping(): void {
		for (const [uid, client] of this.#clients) {
			if (!client.sse.isOpen()) {
				this.#dropClient(uid);
				continue;
			}
			void client.sse.send(PING_CHANNEL, {}).catch((error: unknown) => {
				this.#warn(`keep-alive to ${uid} failed`, error);
				this.#dropClient(uid);
			});
		}
	}

	// ─── Channel authorization ────────────────────────────────

	/**
	 * Register an authorization callback. Patterns support a single
	 * `:param` slot or a trailing `:*` wildcard:
	 *
	 *   authorize('users/:id/notifications', (ctx, { id }) => ...)
	 *   authorize('projects/*', (ctx, _) => ...)
	 */
	authorize<TParams extends Record<string, string> = Record<string, string>>(
		pattern: string,
		callback: ChannelAuthorizer<TParams>,
	): void {
		this.#authorizers.set(
			pattern,
			callback as ChannelAuthorizer<Record<string, string>>,
		);
	}

	// ─── Auto-registered route customization ──────────────────

	/**
	 * Hook into the three auto-registered SSE routes
	 * (`GET /__relay/events`, `POST /__relay/subscribe`,
	 * `POST /__relay/unsubscribe`) — typically to attach an auth
	 * middleware so `ctx.auth` is populated by the time
	 * `relay.subscribe(...)` calls back into the authorizer.
	 *
	 *   relay.registerRoutes((route) => {
	 *     route.middleware('auth')
	 *   })
	 *
	 * The callback runs once per route, against the host router's
	 * route-builder object. Repeated calls overwrite the previous
	 * customizer — last call wins.
	 *
	 * Equivalent of AdonisJS Transmit's
	 * `transmit.registerRoutes((route) => route.middleware(['auth']))`.
	 */
	registerRoutes(customizer: RelayRouteCustomizer): void {
		this.#routeCustomizer = customizer;
	}

	/**
	 * Mount a {@link Hub} at `path`, reachable over SignalR's Server-Sent
	 * Events transport.
	 *
	 *   relay.hub("/hubs/chat", new ChatHub())
	 *
	 * Call it from a preload (`start/services.ts`), like
	 * {@link registerRoutes}: the provider registers the routes in `start()`,
	 * after preloads have run, so a hub recorded there is always picked up.
	 *
	 * Mounting the same path twice throws rather than replacing the first hub —
	 * a route silently shadowed is how half an application stops answering with
	 * nothing in the log.
	 */
	hub(path: string, hub: HubLike, adapter?: SignalRAdapterLike): void {
		if (this.#hubs.has(path)) {
			throw new Error(
				`A hub is already mounted at "${path}" — unmount it or pick another path.`,
			);
		}
		this.#hubs.set(path, { path, hub, adapter });
	}

	/** @internal The hubs recorded so far, read by the provider at `start()`. */
	mountedHubs(): Array<{
		path: string;
		hub: HubLike;
		adapter?: SignalRAdapterLike;
	}> {
		return [...this.#hubs.values()];
	}

	/**
	 * @internal Apply the stored customizer to a freshly-built route.
	 * Called by `RelayProvider` for each of the three SSE endpoints.
	 */
	applyRouteCustomization(route: RelayRouteBuilder): void {
		this.#routeCustomizer?.(route);
	}

	// ─── Broadcasting ─────────────────────────────────────────

	/**
	 * Send a payload to every SSE client subscribed to `channel`.
	 * Returns the number of clients reached. Dead writers (client gone)
	 * are reaped during the iteration.
	 *
	 * With a `transport` configured, the payload is also mirrored onto the
	 * bus so the SSE clients of every OTHER instance receive it too.
	 */
	broadcast(channel: string, payload: unknown): number {
		// `undefined` is not a value JSON can carry: it survives in-process,
		// disappears through `JSON.stringify` on the way to the SSE frame, and
		// comes back from the bus as a missing key on the other instances. Which
		// of those a listener sees would depend on where it happened to run.
		// Transmit substitutes null here for the same reason.
		const carried = payload === undefined ? null : payload;
		const reached = this.#deliver(channel, carried);
		this.#publishToBus(channel, carried);
		this.#emit("broadcast", { channel, payload: carried });
		return reached;
	}

	/**
	 * Broadcast to every subscriber of `channel` EXCEPT the given uid(s).
	 * Returns the number of clients reached. Mirrors Adonis Transmit's
	 * `broadcastExcept` — used to skip the socket that originated an action
	 * (e.g. optimistic-UI echo suppression). Local-only, like Transmit: it
	 * does not mirror onto the bus, so the exclusion is honored on the
	 * originating instance where the sender lives.
	 */
	broadcastExcept(
		channel: string,
		payload: unknown,
		senderUid: string | string[],
	): number {
		return this.#deliver(channel, payload, senderUid);
	}

	/**
	 * Fan a payload out to the local SSE clients subscribed to `channel`,
	 * skipping any uid in `exclude`. Reaps dead writers during the walk.
	 * Shared by `broadcast` / `broadcastExcept` and the transport re-emit.
	 */
	#deliver(
		channel: string,
		payload: unknown,
		exclude?: string | string[],
	): number {
		const subscribers = this.#channelIndex.get(channel);
		if (!subscribers || subscribers.size === 0) return 0;
		const excluded =
			exclude === undefined
				? null
				: Array.isArray(exclude)
					? new Set(exclude)
					: exclude;
		let reached = 0;
		const dead: string[] = [];
		for (const uid of subscribers) {
			if (
				excluded !== null &&
				(typeof excluded === "string" ? excluded === uid : excluded.has(uid))
			) {
				continue;
			}
			const client = this.#clients.get(uid);
			if (!client) {
				// Subscribed but no longer a client: the index outlived the
				// client it points at. `#dropClient` cannot clear this one — it
				// reads the channels off the client, and there is none — so the
				// entry is removed here or never.
				this.#indexRemove(channel, uid);
				continue;
			}
			if (!client.sse.isOpen()) {
				dead.push(uid);
				continue;
			}
			// Fire-and-forget — the underlying SSE writer queues onto a
			// bounded mpsc, so a slow client cannot block the broadcast.
			//
			// Not awaited, but not unhandled either: a write that rejects is a
			// client that cannot receive, so it is dropped rather than left in
			// the map to fail on every later broadcast. Unhandled, that
			// rejection took the process down on a default Node — one dead
			// socket ending the broadcast for everybody.
			void client.sse.send(channel, payload).catch((error: unknown) => {
				this.#warn(`delivery to ${uid} on '${channel}' failed`, error);
				this.#dropClient(uid);
			});
			reached++;
		}
		for (const uid of dead) this.#dropClient(uid);
		return reached;
	}

	#publishToBus(channel: string, payload: unknown): void {
		if (!this.#transport) return;
		const message: RelayTransportMessage = {
			type: "broadcast",
			channel,
			payload,
		};
		// A publish that fails means the other instances never hear this
		// broadcast — the same split-brain a failed subscribe causes, and worth
		// the same line on stderr rather than an unhandled rejection.
		const transport = this.#transport;
		void (async () =>
			transport.publish(this.#transportChannel, message))().catch(
			(error: unknown) => {
				this.#warn(
					`publishing '${channel}' to the bus failed — other instances will not see it`,
					error,
				);
			},
		);
	}

	/** One place for the "it kept going, but you should know" line. */
	#warn(what: string, error: unknown): void {
		process.stderr.write(
			`[relay] ${what}: ${error instanceof Error ? error.message : String(error)}\n`,
		);
	}

	// ─── Lifecycle hooks ──────────────────────────────────────

	/**
	 * Register a lifecycle listener. Returns a detacher that removes it —
	 * mirrors Adonis Transmit's `on` (built on Emittery, whose `on` returns
	 * an unsubscribe function).
	 */
	on<E extends LifecycleEventName>(
		event: E,
		listener: LifecycleListener<E>,
	): () => void {
		const set = (this.#listeners[event] ?? new Set()) as Set<
			LifecycleListener<E>
		>;
		set.add(listener);
		// Mapped-type narrowing limitation: TS can't propagate `E` from the
		// index expression to the assignment target. Object spread sidesteps
		// the assignability check by re-typing the whole structure with the
		// added entry (the same variance dance as the upstream read cast).
		this.#listeners = { ...this.#listeners, [event]: set };
		return () => {
			set.delete(listener);
		};
	}

	// ─── Connection / subscription (called by the provider routes) ─

	/**
	 * Register a freshly-opened SSE stream. Called by the auto-registered
	 * `GET /__relay/events` route handler.
	 *
	 * SECURITY: the uid is server-derived, not client-chosen. A client that
	 * could pick its own uid could (a) impersonate another user by claiming
	 * their id, (b) evict their stream via the reconnect short-circuit, or
	 * (c) listen to their channels by re-using the uid. To prevent this:
	 *
	 *   - authenticated request → uid is forced to `ctx.auth.user.id`;
	 *     any client-supplied hint that disagrees yields `'forbidden'`.
	 *   - anonymous request → uid is a fresh server-generated `randomUUID()`;
	 *     the client learns it from the initial `connected` SSE frame and
	 *     must echo it back in `subscribe`/`unsubscribe` bodies. The hint
	 *     parameter is ignored.
	 *
	 * Returns `'ok'` with the issued uid on success, `'capped'` when the
	 * client cap is reached (handler responds 503), `'forbidden'` when the
	 * client tried to claim a uid that doesn't match the authenticated
	 * identity (handler responds 403).
	 */
	connect(
		clientUidHint: string | undefined,
		sse: RelaySseStream,
		ctx: RelayContext,
	):
		| { outcome: "ok"; uid: string }
		| { outcome: "capped" }
		| { outcome: "forbidden"; reason: string } {
		const authUserId = ctx.auth?.isAuthenticated
			? ctx.auth.user?.id
			: undefined;
		let uid: string;
		if (authUserId !== undefined) {
			// Authenticated: identity is fixed. A mismatched hint is a hijack
			// attempt — reject rather than silently rewriting.
			if (clientUidHint !== undefined && clientUidHint !== authUserId) {
				return {
					outcome: "forbidden",
					reason: "uid hint does not match authenticated user",
				};
			}
			uid = authUserId;
		} else {
			// Anonymous: server issues a cryptographically random uid so a
			// guessing attacker can't connect on behalf of a specific session.
			uid = randomUUID();
		}
		// Reconnect short-circuit BEFORE the cap check: the slot for `uid` is
		// already accounted for in `#clients.size`, so a refresh / network
		// reconnect should replace the prior writer (drop + re-add) rather
		// than 503 with `capped`. The cap only matters for *new* uids — those
		// genuinely add a slot to the map.
		const isReconnect = this.#clients.has(uid);
		if (isReconnect) {
			this.#dropClient(uid);
		} else if (this.#clients.size >= this.#config.maxClients) {
			return { outcome: "capped" };
		}
		this.#clients.set(uid, {
			uid,
			sse,
			channels: new Set(),
			auth: ctx.auth,
		});
		// Identity-guarded: a stale stream's onClose must not drop a newer client
		// that reused the same uid after a reconnect.
		sse.onClose(() => {
			if (this.#clients.get(uid)?.sse.id === sse.id) this.#dropClient(uid);
		});
		// Initial frame: confirms the connection to the JS client. Adonis
		// Transmit ships `{ uid }` too. The client uses this uid for the
		// subscribe/unsubscribe round trips.
		// A `connected` frame that never arrives leaves the client without the
		// uid it needs to subscribe or unsubscribe — it is connected to nothing
		// it can use, so it is dropped rather than kept as a silent tenant of
		// the client map. Unhandled, this rejection also ended the process.
		void sse.send("connected", { uid }).catch((error: unknown) => {
			this.#warn(`the connected frame for ${uid} failed`, error);
			this.#dropClient(uid);
		});
		this.#emit("connect", { uid });
		return { outcome: "ok", uid };
	}

	/**
	 * Handle a subscribe request. Returns an outcome the provider
	 * routes translate into HTTP status codes (204 OK / 400 / 403 / 429).
	 */
	async subscribe(
		uid: unknown,
		channel: unknown,
		ctx: RelayContext,
	): Promise<SubscribeResult> {
		// Both arrive straight off a JSON request body, so neither is a string
		// because the signature says so. A channel that was an object reached
		// the pattern matcher and threw `channel.split is not a function` out of
		// the route — a 500, and a stack trace, for a body anyone can post.
		if (!isNonEmptyString(uid) || !isNonEmptyString(channel)) {
			return { ok: false, status: 400, code: "E_RELAY_BAD_REQUEST" };
		}
		if (channel.length > 256) {
			return { ok: false, status: 400, code: "E_CHANNEL_TOO_LONG" };
		}
		const client = this.#clients.get(uid);
		if (!client) {
			return { ok: false, status: 400, code: "E_NOT_CONNECTED" };
		}
		// Ownership: reject subscribe attempts from a request whose auth
		// identity doesn't match the identity recorded at connect-time.
		// Without this check, any party who learns a uid (logs, sibling
		// session, leaked SSE init frame) can subscribe on that uid's
		// behalf. `#assertOwnership` returns null when the request is
		// allowed to act on the client.
		const ownership = this.#assertOwnership(client, ctx);
		if (ownership !== null) return ownership;
		// Idempotent: re-subscribing to a channel the client already holds is
		// a no-op success, not a quota error. SSE / WebSocket clients
		// naturally replay subscribe on reconnect; charging that replay
		// against `maxChannelsPerClient` would 429 the legitimate path.
		if (client.channels.has(channel)) {
			return { ok: true };
		}
		if (client.channels.size >= this.#config.maxChannelsPerClient) {
			return { ok: false, status: 429, code: "E_MAX_CHANNELS" };
		}
		const match = this.#findAuthorizer(channel);
		if (match) {
			let allowed: boolean;
			try {
				allowed = await match.authorize(ctx, match.params);
			} catch {
				return { ok: false, status: 403, code: "E_CHANNEL_FORBIDDEN" };
			}
			if (!allowed) {
				return { ok: false, status: 403, code: "E_CHANNEL_FORBIDDEN" };
			}
		} else if (!this.#config.allowUnauthorizedChannels) {
			return { ok: false, status: 403, code: "E_CHANNEL_NO_AUTHORIZER" };
		}
		// An authorizer may await — a database lookup is the usual one — and the
		// socket can close in that window. Writing the subscription then put the
		// uid back into the channel index for a client that no longer exists,
		// and nothing could take it out again: `#dropClient` walks the client's
		// own channel set, and that client is gone. The entry stayed for the
		// life of the process, counted by `channelSubscribers`, returned by
		// `getSubscribersFor`, and walked on every broadcast. Worse on a
		// reconnect: the uid is stable for an authenticated client, so the
		// ghost attached the old subscription to the NEW connection, which had
		// never asked for that channel and was never checked for it.
		if (this.#clients.get(uid) !== client) {
			return { ok: false, status: 400, code: "E_NOT_CONNECTED" };
		}
		client.channels.add(channel);
		this.#indexAdd(channel, uid);
		this.#emit("subscribe", { uid, channel });
		return { ok: true };
	}

	/**
	 * Drop a single channel from a client's subscription set.
	 *
	 * Returns `'ok'` on success (including no-op when channel wasn't
	 * subscribed — keeps the wire idempotent), `'forbidden'` when the
	 * requester doesn't own the uid.
	 */
	unsubscribe(
		uid: unknown,
		channel: unknown,
		ctx: RelayContext,
	): "ok" | "forbidden" {
		// Same untrusted body as `subscribe`. Nothing here would throw on a
		// non-string, but a `Set.delete` that silently misses is how a client
		// believes it has stopped listening while the frames keep arriving.
		if (!isNonEmptyString(uid) || !isNonEmptyString(channel)) return "ok";
		const client = this.#clients.get(uid);
		if (!client) return "ok";
		// Same ownership check as subscribe — without it, any party who
		// knows a uid can silently terminate that client's channel
		// subscriptions, denying service.
		const ownership = this.#assertOwnership(client, ctx);
		if (ownership !== null) return "forbidden";
		if (!client.channels.delete(channel)) return "ok";
		this.#indexRemove(channel, uid);
		this.#emit("unsubscribe", { uid, channel });
		return "ok";
	}

	// ─── Stats ────────────────────────────────────────────────

	clientCount(): number {
		return this.#clients.size;
	}

	channelSubscribers(channel: string): number {
		return this.#channelIndex.get(channel)?.size ?? 0;
	}

	/**
	 * List the uids currently subscribed to `channel`. Adonis Transmit
	 * parity (`getSubscribersFor`). Returns a fresh array snapshot.
	 */
	getSubscribersFor(channel: string): string[] {
		const set = this.#channelIndex.get(channel);
		return set ? Array.from(set) : [];
	}

	// ─── Shutdown ─────────────────────────────────────────────

	/**
	 * Release external resources: unsubscribe from and disconnect the bus.
	 * Mirrors Adonis Transmit's `shutdown` (clear timers + disconnect the
	 * transport). Call from the provider's graceful-shutdown hook. Idempotent
	 * — both bus methods are optional on the duck-typed transport.
	 */
	async shutdown(): Promise<void> {
		// Cleared first, and unconditionally: Transmit clears its ping interval
		// before touching the bus, and a relay with no transport still has a
		// timer to stop.
		if (this.#pingTimer) clearInterval(this.#pingTimer);
		if (!this.#transport) return;
		await this.#transport.unsubscribe?.(this.#transportChannel);
		await this.#transport.disconnect?.();
	}

	// ─── Internals ────────────────────────────────────────────

	/**
	 * Verify that the current request is allowed to manipulate `client`'s
	 * subscriptions. Returns `null` when allowed, or a `SubscribeResult`
	 * rejection (subscribe uses it directly; unsubscribe maps non-null to
	 * `'forbidden'`). Ownership policy:
	 *
	 *   - If the client was authenticated at connect-time, the current
	 *     request MUST be authenticated as the same user id.
	 *   - If the client was anonymous at connect-time, the uid itself is
	 *     the proof of ownership (server-issued randomUUID; an attacker
	 *     can't guess it). No further check needed.
	 *
	 * This is the symmetric ownership boundary to `connect()`'s identity
	 * binding — without both, the uid path can be hijacked downstream.
	 */
	#assertOwnership(
		client: { auth?: RelayAuth },
		ctx: RelayContext,
	): SubscribeResult | null {
		const connectedUserId = client.auth?.isAuthenticated
			? client.auth.user?.id
			: undefined;
		if (connectedUserId === undefined) return null;
		const requesterUserId = ctx.auth?.isAuthenticated
			? ctx.auth.user?.id
			: undefined;
		if (requesterUserId !== connectedUserId) {
			return { ok: false, status: 403, code: "E_NOT_OWNER" };
		}
		return null;
	}

	#dropClient(uid: string): void {
		const client = this.#clients.get(uid);
		if (!client) return;
		for (const channel of client.channels) {
			this.#indexRemove(channel, uid);
		}
		this.#clients.delete(uid);
		// Close the underlying stream so a reconnect (drop + re-add) doesn't leak
		// the prior writer — once it's out of #clients, broadcast()'s dead-writer
		// sweep can never reach it. Already-closed streams (the onClose path)
		// no-op via isOpen().
		if (client.sse.isOpen()) {
			void client.sse.end().catch(() => {});
		}
		this.#emit("disconnect", { uid });
	}

	#indexAdd(channel: string, uid: string): void {
		let set = this.#channelIndex.get(channel);
		if (!set) {
			set = new Set();
			this.#channelIndex.set(channel, set);
		}
		set.add(uid);
	}

	#indexRemove(channel: string, uid: string): void {
		const set = this.#channelIndex.get(channel);
		if (!set) return;
		set.delete(uid);
		if (set.size === 0) this.#channelIndex.delete(channel);
	}

	/**
	 * The authorizer covering `channel`, together with the params ITS pattern
	 * yields — resolved in one walk, the way Transmit's
	 * `getSecuredChannelDefinition` hands back the matched definition and its
	 * params together.
	 *
	 * Two walks were two answers. Finding the callback preferred an exact
	 * registration over a pattern, while the params were taken from the first
	 * pattern that happened to match: with `users/:id` and `users/me` both
	 * registered, `users/me` ran the exact callback and handed it `{id: 'me'}`
	 * — params from a pattern it was not written for.
	 */
	#findAuthorizer(channel: string):
		| {
				authorize: ChannelAuthorizer<Record<string, string>>;
				params: Record<string, string>;
		  }
		| undefined {
		const exact = this.#authorizers.get(channel);
		if (exact) return { authorize: exact, params: {} };
		for (const [pattern, authorize] of this.#authorizers) {
			const params = extractParams(pattern, channel);
			if (params) return { authorize, params };
		}
		return undefined;
	}

	#emit<E extends LifecycleEventName>(
		event: E,
		evt: RelayLifecycleEvents[E],
	): void {
		const listeners = this.#listeners[event];
		if (!listeners) return;
		for (const cb of listeners) {
			try {
				cb(evt);
			} catch {
				// Listener errors are isolated.
			}
		}
	}
}

export interface SubscribeSuccess {
	ok: true;
}
export interface SubscribeFailure {
	ok: false;
	status: number;
	code: string;
}
export type SubscribeResult = SubscribeSuccess | SubscribeFailure;

/** A uid or a channel, as it has to arrive to be usable at all. */
function isNonEmptyString(value: unknown): value is string {
	return typeof value === "string" && value.length > 0;
}

/**
 * Type guard for a cross-instance broadcast envelope arriving over the
 * bus. The transport carries `unknown`, so validate the shape before
 * re-delivering — a malformed / foreign message is dropped, not trusted.
 */
function isRelayTransportMessage(
	value: unknown,
): value is RelayTransportMessage {
	if (typeof value !== "object" || value === null) return false;
	if (!("type" in value) || !("channel" in value) || !("payload" in value)) {
		return false;
	}
	return value.type === "broadcast" && typeof value.channel === "string";
}

/**
 * Match `pattern` against `channel`. Patterns can contain `:param`
 * placeholders (`users/:id/notifications`) and a trailing `*`
 * wildcard (`projects/*`).
 */
function matchesPattern(pattern: string, channel: string): boolean {
	if (pattern === channel) return true;
	if (pattern.endsWith("/*")) {
		const prefix = pattern.slice(0, -2);
		return channel === prefix || channel.startsWith(`${prefix}/`);
	}
	const patternParts = pattern.split("/");
	const channelParts = channel.split("/");
	if (patternParts.length !== channelParts.length) return false;
	for (let i = 0; i < patternParts.length; i++) {
		const p = patternParts[i];
		const c = channelParts[i];
		if (p.startsWith(":")) continue;
		if (p !== c) return false;
	}
	return true;
}

/**
 * Extract named params from a pattern + channel pair. Returns
 * `undefined` when the pattern doesn't match — `Relay.#findAuthorizer`
 * already filtered the input so this is mostly a re-walk.
 */
function extractParams(
	pattern: string,
	channel: string,
): Record<string, string> | undefined {
	if (!matchesPattern(pattern, channel)) return undefined;
	const patternParts = pattern.split("/");
	const channelParts = channel.split("/");
	const out: Record<string, string> = {};
	for (let i = 0; i < patternParts.length; i++) {
		const p = patternParts[i];
		if (p.startsWith(":")) {
			out[p.slice(1)] = channelParts[i];
		}
	}
	return out;
}
