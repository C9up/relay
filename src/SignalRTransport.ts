/**
 * The HTTP transport that makes a {@link Hub} reachable.
 *
 * `SignalRAdapter` parses and emits frames but owns no socket — it says so
 * itself. Until this file existed, `Hub` and the adapter were exported and
 * could not run on anything: there was no negotiate endpoint, no downstream
 * channel and no way to post a frame up.
 *
 * Two SignalR transports, both first-class, so a stock `@microsoft/signalr`
 * client speaks to a hub unchanged:
 *
 *   POST <path>/negotiate      → connectionId + connectionToken, and the
 *                                transports this server can serve
 *   WS   <path>?id=<token>     → WebSockets: one socket, frames both ways
 *   GET  <path>?id=<token>     → Server-Sent Events, server → client
 *   POST <path>?id=<token>     → one or more framed messages, client → server
 *
 * WebSockets are announced only when the host router has WebSocket routes and
 * the server under it can upgrade (`server.supportsWebSockets()` in ream):
 * announcing them when nothing can upgrade sends every client down a road
 * that dead-ends. The socket and the SSE stream share the hub's path — ream
 * hands a handshake to the WebSocket route and a plain GET to the stream.
 *
 * Over SSE, every server → client message goes out as an event with NO name,
 * so the browser delivers it through `onmessage` — the shape the SignalR
 * client reads. Over a socket, each is one text message.
 */

import { type Hub, type HubAuth, type HubContext, userIdKey } from "./Hub.js";
import { SignalRAdapter } from "./SignalRAdapter.js";

/** SignalR record separator: every JSON message ends with it. */
const RS = "\x1e";

/** What a hub needs of the host's SSE stream. Matches ream's `SseStream`. */
export interface HubSseStream {
	id: string;
	isOpen(): boolean;
	send(event: string, data: unknown, eventId?: string): Promise<boolean>;
	onClose(cb: () => void): void;
	end(): Promise<void>;
}

/** The slice of the host request/response a hub route reads. */
export interface HubHttpContext {
	request: {
		/**
		 * The request URL. Ream leaves the query string out unless asked
		 * (`url(true)`), and the connection token is in the query string.
		 */
		url(includeQueryString?: boolean): string;
		raw(): string;
		/**
		 * The client address, honouring the trusted proxies — what an
		 * anonymous request is counted under for the per-owner caps.
		 */
		ip?(): string;
	};
	response: {
		status(code: number): { json(body: unknown): void };
		json(body: unknown): void;
		sse(): Promise<HubSseStream>;
	};
	auth?: {
		isAuthenticated?: boolean;
		/** The guard that authenticated the request (Warden's Authenticator). */
		authenticatedViaGuard?: string;
		user?: Record<string, unknown>;
		roles?: string[];
		permissions?: string[];
		/**
		 * The auth layer's own answer to a permission question (Warden's
		 * Authenticator asks its rights resolver). A hub's `permissions` guard
		 * asks it rather than reading the payload's list.
		 */
		hasAllPermissions?(permissions: readonly string[]): Promise<boolean>;
	};
}

/** A hub mounted at a path, as recorded by `relay.hub(...)`. */
export interface MountedHub {
	path: string;
	hub: Hub;
	adapter: SignalRAdapter;
}

/** The router methods this transport needs. */
/**
 * The slice of a router the hub routes need.
 *
 * Generic over the context so a host router whose handlers receive a RICHER
 * context still satisfies it. Pinned to `HubHttpContext`, a host's own router
 * type never matched — handler parameters are contravariant — and the caller
 * had to lie with a double cast.
 */
export interface HubRouter<
	Ctx extends HubHttpContext = HubHttpContext,
	Route = unknown,
> {
	get(path: string, handler: (ctx: Ctx) => Promise<void> | void): Route;
	post(path: string, handler: (ctx: Ctx) => Promise<void> | void): Route;
	/**
	 * A WebSocket route, as ream's `router.ws()`: the handler runs on the
	 * handshake, after the route middleware, with the socket on the context.
	 * A router without it serves the hub over Server-Sent Events only.
	 */
	ws?(
		path: string,
		handler: (ctx: Ctx & { socket: HubSocket }) => Promise<void> | void,
	): Route;
}

/** What a hub needs of the host's WebSocket. Matches ream's `ServerWebSocket`. */
export interface HubSocket {
	readonly readyState: "open" | "closing" | "closed";
	/** `false` when the socket is closing, or its backlog is full. */
	send(data: string): boolean;
	close(code?: number, reason?: string): void;
	on(
		event: "message",
		listener: (data: string | Uint8Array, isBinary: boolean) => void,
	): unknown;
	on(event: "close", listener: (code: number, reason: string) => void): unknown;
}

/** How {@link registerHubRoutes} learns what the server can serve. */
export interface HubRouteOptions {
	/**
	 * Whether the server under the router accepts WebSocket connections now —
	 * ream's `server.supportsWebSockets()`. Asked on every negotiate, since the
	 * server starts after the routes are declared. Absent: Server-Sent Events
	 * only.
	 */
	webSockets?: () => boolean;
}

/**
 * Where a connection's server → client frames go: the SSE stream or the
 * socket. One shape for both, so the hub, the caps and the handshake rules
 * are the same whichever transport the client picked.
 */
interface Downstream {
	isOpen(): boolean;
	/** Write one framed message; a closed downstream drops it. */
	send(frame: string): Promise<void>;
	/** End the connection; `goingAway` when the server is shutting down. */
	end(goingAway?: boolean): Promise<void>;
}

function sseDownstream(sse: HubSseStream): Downstream {
	return {
		isOpen: () => sse.isOpen(),
		send: async (frame) => {
			await sse.send("", frame);
		},
		end: () => sse.end(),
	};
}

/** WebSocket close codes this transport sends (RFC 6455 §7.4.1). */
const WS_NORMAL = 1000;
const WS_GOING_AWAY = 1001;
const WS_UNSUPPORTED_DATA = 1003;
const WS_INTERNAL_ERROR = 1011;
const WS_TRY_AGAIN_LATER = 1013;

/** The longest delay a timer takes: 2^31 - 1 ms, a little under 25 days. */
const MAX_TIMER_MS = 2_147_483_647;

function socketDownstream(socket: HubSocket): Downstream {
	return {
		isOpen: () => socket.readyState === "open",
		send: async (frame) => {
			// `false` is a socket already closing, or a client so far behind that
			// the server closes it (1013): either way its close event releases
			// the connection, so there is nothing to retry here.
			socket.send(frame);
		},
		end: async (goingAway) => {
			socket.close(goingAway === true ? WS_GOING_AWAY : WS_NORMAL);
		},
	};
}

/** The `id` query parameter — SignalR's connection token. */
function readConnectionToken(url: string): string | undefined {
	const q = url.indexOf("?");
	if (q === -1) return undefined;
	return new URLSearchParams(url.slice(q + 1)).get("id") ?? undefined;
}

/**
 * The request's auth as a hub reads it.
 *
 * `hasAllPermissions` stays bound to the request's own auth object, so a
 * `permissions` guard asks the auth layer — and asks it again on every
 * invocation — instead of reading a list copied when the request arrived.
 */
function hubAuth(ctx: HubHttpContext): HubAuth {
	const auth = ctx.auth;
	const ask = auth?.hasAllPermissions;
	return {
		isAuthenticated: auth?.isAuthenticated === true,
		// The hub's guard check compares its `guards` with this.
		strategy: auth?.authenticatedViaGuard,
		user: auth?.user,
		roles: auth?.roles,
		permissions: auth?.permissions,
		...(auth !== undefined && typeof ask === "function"
			? {
					hasAllPermissions: (required: readonly string[]) =>
						ask.call(auth, required),
				}
			: {}),
	};
}

/**
 * When the access token a request authenticated with stops being valid, in
 * epoch milliseconds — `user.currentAccessToken.expiresAt`, where a token
 * guard puts it. `undefined` for a request without a token, or with one that
 * does not expire.
 */
function accessTokenExpiry(auth: HubAuth): number | undefined {
	const token: unknown = auth.user?.currentAccessToken;
	if (typeof token !== "object" || token === null) return undefined;
	const expiresAt: unknown = Reflect.get(token, "expiresAt");
	if (!(expiresAt instanceof Date)) return undefined;
	const time = expiresAt.getTime();
	return Number.isNaN(time) ? undefined : time;
}

/**
 * Who an auth slice identifies.
 *
 * `unusable` is an authenticated request whose user carries no id this can
 * compare: it is somebody, so it must not pass as anonymous — that made every
 * logged-in user of an application with numeric keys (the atlas default) match
 * every other one.
 */
type Identity =
	| { kind: "anonymous" }
	| { kind: "user"; id: string }
	| { kind: "unusable" };

/**
 * The identity of an auth slice. A string id is taken as given; a number or a
 * bigint — an auto-increment primary key — as its decimal text, so `1` and the
 * same user on the next request compare equal.
 */
function identityOf(
	auth:
		| { isAuthenticated?: boolean; user?: Record<string, unknown> }
		| undefined,
): Identity {
	if (auth?.isAuthenticated !== true) return { kind: "anonymous" };
	const id = userIdKey(auth.user?.id);
	return id === undefined ? { kind: "unusable" } : { kind: "user", id };
}

/**
 * Whether a request may act on the connection registered under this client id.
 *
 * The connectionToken travels in a query string — SignalR's own design — so it
 * reaches access logs, proxies and `Referer` headers. Holding it must not be
 * enough to speak as the connection. This is the rule `Relay` applies to its
 * own uid, in both directions: a connection opened as a user takes requests
 * from that user only, and one opened anonymously takes anonymous requests
 * only — the replies go down a stream someone else may hold, so a victim's
 * credentials on the POST (a cross-site form) must not pass the guards for it.
 * An identity that cannot be compared matches nothing: fail closed.
 *
 * Before any connection is registered — the token's first use — the request
 * is held to the identity that NEGOTIATED the token instead. Letting anyone
 * through then meant the first to present a token owned it: a token read
 * between the victim's negotiate and its connect opened the connection as
 * the attacker, and the victim's own connect was then the one refused.
 */
function mayActAs(
	registered: HubContext["auth"] | undefined,
	negotiator: string | undefined,
	ctx: HubHttpContext,
): boolean {
	const requester = identityKey(identityOf(ctx.auth));
	if (requester === undefined) return false;
	if (registered === undefined) return requester === negotiator;
	return requester === identityKey(identityOf(registered));
}

/**
 * An identity as one comparable string: `user:<id>` or `anonymous`;
 * `undefined` for one that cannot be compared, which matches nothing.
 */
function identityKey(identity: Identity): string | undefined {
	if (identity.kind === "user") return `user:${identity.id}`;
	if (identity.kind === "anonymous") return "anonymous";
	return undefined;
}

/**
 * What a request counts against for the per-owner caps: the user when it is
 * identified, the client address otherwise, nothing when neither is known.
 */
function ownerOf(ctx: HubHttpContext): string | undefined {
	const identity = identityOf(ctx.auth);
	if (identity.kind === "user") return `user:${identity.id}`;
	const ip = ctx.request.ip?.();
	return typeof ip === "string" && ip !== "" ? `ip:${ip}` : undefined;
}

/**
 * Split a request body into SignalR frames.
 *
 * A single POST may carry several messages, each terminated by the record
 * separator. A trailing empty piece is dropped; a body with no separator is
 * treated as one frame, because a client that omits it is still trying to say
 * something and the adapter reports the protocol error better than a silent
 * drop would.
 */
export function splitFrames(body: string): string[] {
	if (body === "") return [];
	const pieces = body.split(RS).filter((p) => p !== "");
	return pieces.map((p) => p + RS);
}

/**
 * Frames that arrived together — one WebSocket message, or one POST body —
 * waiting for the hub.
 */
interface Batch {
	frames: string[];
	/** The auth each frame is authorized against, read when it runs. */
	auth: () => HubAuth;
	resolve: () => void;
	reject: (error: unknown) => void;
}

/** One open connection: where its frames go, and what it has sent that waits. */
interface Connection {
	readonly downstream: Downstream;
	/** Batches received and not started, oldest first. */
	readonly inbox: Batch[];
	/** Frames received and not started — what `maxQueuedMessages` bounds. */
	queued: number;
	/** Whether a batch is being handed to the hub. */
	running: boolean;
	/** Set once the connection is over: nothing it sent runs after that. */
	ended: boolean;
	/**
	 * The hub context of a connection whose handshake is still pending:
	 * `onConnect` runs once the handshake is answered, never before — a frame
	 * sent ahead of the handshake reply makes the SignalR client refuse the
	 * connection.
	 */
	awaiting: HubContext | undefined;
	handshakeTimer: ReturnType<typeof setTimeout> | undefined;
	expiryTimer: ReturnType<typeof setTimeout> | undefined;
}

/**
 * Register the routes one hub needs: negotiate, the SSE stream and its
 * upstream POST, and — on a router that has them — the WebSocket route.
 *
 * The hub's guards (`useGuards`) are what it takes to connect: negotiate, the
 * stream and the socket's handshake each ask them and answer a refusal with
 * plain HTTP — `401` or `403` — before a token is issued, a stream opened or
 * a client registered. As ASP.NET Core SignalR does for `[Authorize]` on a
 * hub class, and unlike a middleware on the routes, the requirement belongs
 * to the hub: it does not depend on what else is mounted, or in which order.
 *
 * Returns the route objects so a caller can apply middleware to them the way
 * `relay.registerRoutes()` customizes the relay's own.
 */
export function registerHubRoutes<Ctx extends HubHttpContext, Route>(
	router: HubRouter<Ctx, Route>,
	mounted: MountedHub,
	options: HubRouteOptions = {},
): {
	negotiate: Route;
	stream: Route;
	send: Route;
	/** The WebSocket route; absent on a router without `ws()`. */
	socket?: Route;
	/**
	 * End every open connection and refuse the routes with 503 until
	 * {@link reopen} — what a shutdown does, as Transmit's `closeAllStreams`.
	 */
	close(): Promise<void>;
	/** Accept connections again after {@link close}. */
	reopen(): void;
} {
	const { path, hub, adapter } = mounted;
	/** Set by `close()`: the routes refuse until `reopen()`. */
	let closed = false;
	const refuseWhileClosed = (ctx: Ctx): boolean => {
		if (!closed) return false;
		ctx.response.status(503).json({
			error: {
				code: "E_RELAY_SHUT_DOWN",
				message: "The realtime service is shutting down.",
			},
		});
		return true;
	};
	/**
	 * Whether the hub's guards refuse this request, answering the refusal
	 * itself when they do: `401` for a request that is not authenticated (or
	 * not through an accepted guard), `403` for one without the role or the
	 * permissions. Asked before anything else, so a refused request learns
	 * nothing about tokens and takes no slot.
	 */
	const refusedByGuards = async (ctx: Ctx): Promise<boolean> => {
		const denial = await hub.authorize(hubAuth(ctx));
		if (denial === null) return false;
		ctx.response.status(denial.status).json({
			error: { code: denial.code, message: denial.message },
		});
		return true;
	};
	/** Live connections by connectionId, so an upstream frame can answer downstream. */
	const connections = new Map<string, Connection>();
	/** Open connections per owner, and the owner each client id counts against. */
	const perOwner = new Map<string, number>();
	const ownerOfClient = new Map<string, string>();

	/** The transports to announce: WebSockets first, as SignalR tries them in order. */
	const transports = (): string[] =>
		router.ws !== undefined && options.webSockets?.() === true
			? ["WebSockets", "ServerSentEvents"]
			: ["ServerSentEvents"];

	const negotiate = router.post(`${path}/negotiate`, async (ctx) => {
		if (refuseWhileClosed(ctx)) return;
		if (await refusedByGuards(ctx)) return;
		const owner = ownerOf(ctx);
		if (!adapter.admits(owner)) {
			ctx.response.status(503).json({
				error: {
					code: "E_RELAY_MAX_PENDING",
					message: "Too many pending connections — try again later.",
				},
			});
			return;
		}
		// The connectionId is the hub's client id; the token is what the client
		// echoes back on the data routes.
		ctx.response.json(
			adapter.negotiate(
				crypto.randomUUID(),
				transports(),
				owner,
				identityKey(identityOf(ctx.auth)),
			),
		);
	});

	/**
	 * Whether a downstream may open for this request, answering the refusal
	 * itself when not. Both transports answer it BEFORE anything opens: a 400
	 * written after an SSE upgrade reaches the client as a half-open
	 * connection, and a refused WebSocket handshake gets plain HTTP, no socket.
	 *
	 * The guards are asked again here, not only at negotiate: a token is
	 * handed out to whoever negotiated, and what opens the connection is this
	 * request.
	 */
	async function admit(ctx: Ctx): Promise<
		| {
				clientId: string;
				token: string;
				owner: string | undefined;
				previous: Connection | undefined;
		  }
		| undefined
	> {
		if (await refusedByGuards(ctx)) return undefined;
		const token = readConnectionToken(ctx.request.url(true));
		const clientId = token ? adapter.resolveToken(token) : undefined;
		if (token === undefined || clientId === undefined) {
			ctx.response.status(400).json({
				error: {
					code: "E_RELAY_UNKNOWN_CONNECTION",
					message: "Unknown or missing connection id — call /negotiate first.",
				},
			});
			return undefined;
		}
		if (!mayActAs(hub.authFor(clientId), adapter.negotiatorOf(token), ctx)) {
			ctx.response.status(403).json({
				error: {
					code: "E_RELAY_NOT_OWNER",
					message: "This connection belongs to another user.",
				},
			});
			return undefined;
		}
		const previous = connections.get(clientId);
		const owner = ownerOf(ctx);
		const ownerCap = adapter.maxConnectionsPerOwner;
		if (
			previous === undefined &&
			owner !== undefined &&
			ownerCap !== false &&
			(perOwner.get(owner) ?? 0) >= ownerCap
		) {
			ctx.response.status(503).json({
				error: {
					code: "E_RELAY_MAX_CONNECTIONS_PER_OWNER",
					message: "Too many open connections from this client.",
				},
			});
			return undefined;
		}
		if (previous === undefined && connections.size >= adapter.maxConnections) {
			ctx.response.status(503).json({
				error: {
					code: "E_RELAY_MAX_CONNECTIONS",
					message: "Too many open connections — try again later.",
				},
			});
			return undefined;
		}
		return { clientId, token, owner, previous };
	}

	/** Register an admitted connection on the hub. */
	function attach(
		ctx: Ctx,
		admitted: NonNullable<Awaited<ReturnType<typeof admit>>>,
		downstream: Downstream,
	): Connection {
		const { clientId, token, owner, previous } = admitted;
		const connection: Connection = {
			downstream,
			inbox: [],
			queued: 0,
			running: false,
			ended: false,
			awaiting: undefined,
			handshakeTimer: undefined,
			expiryTimer: undefined,
		};
		// The token is in use now, so it stops counting against the unclaimed
		// budget and stops expiring.
		adapter.claimToken(token);
		connections.set(clientId, connection);
		// A reopened connection for the same client id takes over its slot.
		if (previous === undefined && owner !== undefined) {
			perOwner.set(owner, (perOwner.get(owner) ?? 0) + 1);
			ownerOfClient.set(clientId, owner);
		}
		// The same token opened again: the older connection would stay open and
		// unused. It is no longer the one registered, so ending it leaves the
		// new one alone.
		if (previous !== undefined) void end(clientId, previous);
		// A connection that never completes the handshake is closed: its token
		// no longer expires, so nothing else would ever end it.
		connection.handshakeTimer = setTimeout(() => {
			if (
				!adapter.hasCompletedHandshake(clientId) &&
				connections.get(clientId) === connection
			) {
				void end(clientId, connection);
			}
		}, adapter.handshakeTimeoutMs);
		connection.handshakeTimer.unref?.();
		const send = (event: string, data: unknown): void => {
			// A hub send is fire-and-forget by contract — the caller returns
			// nothing — so a rejection here had nobody to reject to and took the
			// process down over one client's socket.
			downstream
				.send(adapter.encodeInvocation(event, [data]))
				.catch((error: unknown) => {
					process.stderr.write(
						`[relay/signalr] send to ${clientId} failed: ${
							error instanceof Error ? error.message : String(error)
						}\n`,
					);
				});
		};
		const auth = hubAuth(ctx);
		connection.awaiting = hub.registerClient({
			id: clientId,
			groups: new Set<string>(),
			auth,
			send,
			close: () => {
				void end(clientId, connection);
			},
		});
		// A connection lives no longer than the access token that opened it:
		// nothing re-authenticates a stream that only listens, so without this
		// an expired token went on receiving for as long as it stayed open.
		const expiresAt = accessTokenExpiry(auth);
		if (expiresAt !== undefined) endAt(expiresAt, clientId, connection);
		return connection;
	}

	/** End `connection` once `time` has come, however far away that is. */
	function endAt(time: number, clientId: string, connection: Connection): void {
		const left = Math.max(time - Date.now(), 0);
		connection.expiryTimer = setTimeout(
			() => {
				if (connection.ended) return;
				// A delay longer than a timer takes is waited in several legs.
				if (Date.now() < time) endAt(time, clientId, connection);
				else void end(clientId, connection);
			},
			Math.min(left, MAX_TIMER_MS),
		);
		connection.expiryTimer.unref?.();
	}

	/**
	 * Take what a connection sent into its queue. Resolves once every frame
	 * has been handled, or dropped because the connection ended; rejects with
	 * what handling one threw. `undefined` when the queue has no room: nothing
	 * was taken, and the caller refuses.
	 *
	 * A connection's frames are handled one after another, whichever requests
	 * or messages carried them: an invocation's answer must not overtake the
	 * handshake reply, and a hub method may rely on the order its client
	 * called in — one invocation at a time per connection, as SignalR's
	 * `MaximumParallelInvocationsPerClient` defaults to. So a client that sends
	 * faster than a hub method answers builds a backlog here, and the server
	 * under the socket cannot push back for us: it stops reading only while a
	 * listener BLOCKS, and a hub method that awaits does not. The backlog is
	 * therefore counted — every frame, however many one message packs — and a
	 * connection past `maxQueuedMessages` is refused, as ream's JSON-RPC socket
	 * does.
	 */
	function accept(
		clientId: string,
		connection: Connection,
		frames: string[],
		auth: () => HubAuth,
	): Promise<void> | undefined {
		if (connection.queued + frames.length > adapter.maxQueuedMessages) {
			return undefined;
		}
		connection.queued += frames.length;
		return new Promise<void>((resolve, reject) => {
			connection.inbox.push({ frames, auth, resolve, reject });
			void run(clientId, connection);
		});
	}

	/** Hand a connection's waiting batches to the hub, oldest first. */
	async function run(clientId: string, connection: Connection): Promise<void> {
		if (connection.running) return;
		connection.running = true;
		for (
			let batch = connection.inbox.shift();
			batch !== undefined;
			batch = connection.inbox.shift()
		) {
			let started = 0;
			try {
				for (const frame of batch.frames) {
					// What has not started when the connection ends never will:
					// nobody is left to read the answer.
					if (connection.ended) break;
					started++;
					connection.queued--;
					await deliverFrame(clientId, connection, frame, batch.auth());
				}
				batch.resolve();
			} catch (error) {
				batch.reject(error);
			} finally {
				connection.queued -= batch.frames.length - started;
			}
		}
		connection.running = false;
	}

	/** Hand one frame to the hub and write what it answers down the connection. */
	async function deliverFrame(
		clientId: string,
		connection: Connection,
		frame: string,
		auth: HubAuth,
	): Promise<void> {
		const { downstream } = connection;
		// A Close from the CLIENT is the client saying it is done. The
		// adapter answers it with nothing — correctly, the protocol asks for
		// no reply — so watching only the outbound frames meant a graceful
		// disconnect left the connection open until the socket happened to
		// drop.
		const clientClosed = SignalRAdapter.containsClose([frame]);
		const outbound = await adapter.handleFrame(clientId, frame, auth);
		for (const out of outbound) {
			if (downstream.isOpen()) await downstream.send(out);
		}
		// The handshake reply is out: the hub may speak now.
		const pending = connection.awaiting;
		if (pending !== undefined && adapter.hasCompletedHandshake(clientId)) {
			connection.awaiting = undefined;
			try {
				await hub.onConnect(pending);
			} catch (error) {
				// A hub that throws from `onConnect` has refused the connection:
				// it comes off the hub here, and whoever carried the frame closes
				// the stream or the socket. It used to stay registered — over
				// Server-Sent Events the POST failed and the stream went on
				// receiving every broadcast.
				release(clientId, connection);
				throw error;
			}
		}
		// A Close in the answer is the adapter ending the connection: a failed
		// handshake, or guards that refused an invocation.
		if (clientClosed || SignalRAdapter.containsClose(outbound)) {
			await end(clientId, connection, { said: true });
		}
	}

	const stream = router.get(path, async (ctx) => {
		if (refuseWhileClosed(ctx)) return;
		const admitted = await admit(ctx);
		if (admitted === undefined) return;
		const sse = await ctx.response.sse();
		const connection = attach(ctx, admitted, sseDownstream(sse));
		sse.onClose(() => {
			release(admitted.clientId, connection);
		});
	});

	const send = router.post(path, async (ctx) => {
		if (refuseWhileClosed(ctx)) return;
		const token = readConnectionToken(ctx.request.url(true));
		const clientId = token ? adapter.resolveToken(token) : undefined;
		if (clientId === undefined) {
			ctx.response.status(400).json({
				error: {
					code: "E_RELAY_UNKNOWN_CONNECTION",
					message: "Unknown or missing connection id — call /negotiate first.",
				},
			});
			return;
		}

		if (
			token === undefined ||
			!mayActAs(hub.authFor(clientId), adapter.negotiatorOf(token), ctx)
		) {
			ctx.response.status(403).json({
				error: {
					code: "E_RELAY_NOT_OWNER",
					message: "This connection belongs to another user.",
				},
			});
			return;
		}

		// Frames are taken for a connection that exists. One posted before its
		// stream opened used to reach the adapter all the same, which recorded
		// a handshake for a connection nothing would ever close.
		const connection = connections.get(clientId);
		if (connection === undefined || connection.ended) {
			ctx.response.status(400).json({
				error: {
					code: "E_RELAY_NOT_CONNECTED",
					message: "This connection has no open stream — open it first.",
				},
			});
			return;
		}

		// Authorized against THIS request, not the one that opened the stream:
		// a role revoked since then is refused now, and ends the connection.
		const auth = hubAuth(ctx);
		const handled = accept(
			clientId,
			connection,
			splitFrames(ctx.request.raw()),
			() => auth,
		);
		if (handled === undefined) {
			ctx.response.status(429).json({
				error: {
					code: "E_RELAY_MAX_QUEUED",
					message: "Too many messages are waiting on this connection.",
				},
			});
			return;
		}
		try {
			await handled;
		} catch (error) {
			// Handling a frame failed: the connection is over, as a socket's is
			// (closed 1011), and the host answers this POST with the error.
			await end(clientId, connection);
			throw error;
		}
		// SignalR expects an empty 200 for an accepted upstream batch.
		ctx.response.status(200).json({});
	});

	/**
	 * The WebSocket transport: the same admission and the same hub as the
	 * stream, with both directions on the one socket.
	 *
	 * A socket has one request, its handshake, so every frame on it is
	 * authorized against the handshake's auth — where an SSE upstream POST is
	 * authorized against its own. That auth is read again for each frame: a
	 * `permissions` guard asks the auth layer every time, so a permission
	 * taken away mid-connection ends the socket at its next invocation. A role
	 * is what the handshake carried, as in ASP.NET Core SignalR, whose
	 * WebSocket transport authenticates once too.
	 */
	const socket = router.ws?.(path, async (ctx) => {
		if (refuseWhileClosed(ctx)) return;
		const admitted = await admit(ctx);
		if (admitted === undefined) return;
		const { clientId } = admitted;
		const ws = ctx.socket;
		const connection = attach(ctx, admitted, socketDownstream(ws));
		const auth = (): HubAuth => hubAuth(ctx);
		ws.on("message", (data, isBinary) => {
			if (connection.ended) return;
			if (isBinary || typeof data !== "string") {
				// The JSON protocol is text; a binary message is a client
				// speaking something else (MessagePack), refused as such.
				ws.close(WS_UNSUPPORTED_DATA, "text frames only");
				return;
			}
			const handled = accept(clientId, connection, splitFrames(data), auth);
			if (handled === undefined) {
				// Off the hub before the close is even acknowledged: a client
				// that floods must not go on receiving meanwhile.
				release(clientId, connection);
				ws.close(WS_TRY_AGAIN_LATER, "Too many hub messages waiting");
				return;
			}
			handled.catch((error: unknown) => {
				process.stderr.write(
					`[relay/signalr] frame from ${clientId} failed: ${
						error instanceof Error ? error.message : String(error)
					}\n`,
				);
				release(clientId, connection);
				ws.close(WS_INTERNAL_ERROR);
			});
		});
		ws.on("close", () => {
			release(clientId, connection);
		});
	});

	/**
	 * Stop running what a connection sent: it is over. The batches still
	 * waiting are settled so a POST holding one can answer, and the timers
	 * that would end the connection later are dropped.
	 */
	function discard(connection: Connection): void {
		connection.ended = true;
		clearTimeout(connection.handshakeTimer);
		clearTimeout(connection.expiryTimer);
		for (const batch of connection.inbox.splice(0)) {
			connection.queued -= batch.frames.length;
			batch.resolve();
		}
	}

	/**
	 * Forget a connection that has closed.
	 *
	 * Identity-guarded: a client id can carry a second connection — a
	 * reconnect that opens before the old one has finished closing — and the
	 * old one's close must not tear down the live connection that replaced it.
	 * Unguarded, the stale close unregistered the new client from the hub and
	 * dropped its tokens, leaving a socket that was open and could no longer
	 * receive anything.
	 */
	function release(clientId: string, connection: Connection): void {
		discard(connection);
		if (connections.get(clientId) !== connection) return;
		connections.delete(clientId);
		const owner = ownerOfClient.get(clientId);
		if (owner !== undefined) {
			ownerOfClient.delete(clientId);
			const left = (perOwner.get(owner) ?? 1) - 1;
			if (left > 0) perOwner.set(owner, left);
			else perOwner.delete(owner);
		}
		// `removeClient` fires `onDisconnect` itself — calling it here too
		// would deliver every disconnect twice.
		hub.removeClient(clientId);
		adapter.forget(clientId);
	}

	/**
	 * End a connection from the server's side: off the hub first — it receives
	 * nothing from here on — then its stream or socket is closed. A socket
	 * reports its close later, and by then there is nothing left to release.
	 *
	 * The client is told with a Close frame, unless one already went by
	 * (`said`) or the handshake never completed: a SignalR client stops on a
	 * Close, where a stream that merely goes quiet leaves it waiting for its
	 * own timeout.
	 */
	async function end(
		clientId: string,
		connection: Connection,
		how: { said?: boolean; goingAway?: boolean } = {},
	): Promise<void> {
		const announce =
			how.said !== true &&
			connections.get(clientId) === connection &&
			adapter.hasCompletedHandshake(clientId);
		release(clientId, connection);
		const { downstream } = connection;
		if (!downstream.isOpen()) return;
		if (announce) await downstream.send(adapter.encodeClose()).catch(() => {});
		await downstream.end(how.goingAway === true).catch(() => {});
	}

	const close = async (): Promise<void> => {
		closed = true;
		await Promise.all(
			[...connections].map(([clientId, connection]) =>
				end(clientId, connection, { goingAway: true }),
			),
		);
	};
	const reopen = (): void => {
		closed = false;
	};

	return {
		negotiate,
		stream,
		send,
		...(socket === undefined ? {} : { socket }),
		close,
		reopen,
	};
}
