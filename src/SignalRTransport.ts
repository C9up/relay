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

import type { Hub, HubContext } from "./Hub.js";
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

/** Normalise the auth slice a Hub context expects. */
function hubAuth(ctx: HubHttpContext): {
	isAuthenticated: boolean;
	strategy?: string;
	user?: Record<string, unknown>;
	roles?: string[];
	permissions?: string[];
} {
	return {
		isAuthenticated: ctx.auth?.isAuthenticated === true,
		// The hub's guard check compares its `guards` with this.
		strategy: ctx.auth?.authenticatedViaGuard,
		user: ctx.auth?.user,
		roles: ctx.auth?.roles,
		permissions: ctx.auth?.permissions,
	};
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
	const id = auth.user?.id;
	if (typeof id === "string" && id !== "") return { kind: "user", id };
	if (typeof id === "number" && Number.isSafeInteger(id)) {
		return { kind: "user", id: String(id) };
	}
	if (typeof id === "bigint") return { kind: "user", id: String(id) };
	return { kind: "unusable" };
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
 * Register the routes one hub needs: negotiate, the SSE stream and its
 * upstream POST, and — on a router that has them — the WebSocket route.
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
	/** Live connections by connectionId, so an upstream frame can answer downstream. */
	const connections = new Map<string, Downstream>();
	/**
	 * Hub contexts of connections whose handshake is still pending: `onConnect`
	 * runs once the handshake is answered, never before — a frame sent ahead of
	 * the handshake reply makes the SignalR client refuse the connection.
	 */
	const awaitingHandshake = new Map<string, HubContext>();
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
	 */
	function admit(ctx: Ctx):
		| {
				clientId: string;
				token: string;
				owner: string | undefined;
				previous: Downstream | undefined;
		  }
		| undefined {
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

	/**
	 * Register an admitted connection on the hub. Returns the function its
	 * close must call.
	 */
	function attach(
		ctx: Ctx,
		admitted: NonNullable<ReturnType<typeof admit>>,
		downstream: Downstream,
	): () => void {
		const { clientId, token, owner, previous } = admitted;
		// The token is in use now, so it stops counting against the unclaimed
		// budget and stops expiring.
		adapter.claimToken(token);
		connections.set(clientId, downstream);
		// A reopened connection for the same client id takes over its slot.
		if (previous === undefined && owner !== undefined) {
			perOwner.set(owner, (perOwner.get(owner) ?? 0) + 1);
			ownerOfClient.set(clientId, owner);
		}
		// The same token opened again: the older connection would stay open and
		// unused. Its close is ignored below, since it is no longer the one
		// registered.
		if (previous?.isOpen()) void previous.end().catch(() => {});
		// A connection that never completes the handshake is closed: its token
		// no longer expires, so nothing else would ever end it.
		const handshakeTimer = setTimeout(() => {
			if (
				!adapter.hasCompletedHandshake(clientId) &&
				connections.get(clientId) === downstream
			) {
				void downstream.end().catch(() => {});
			}
		}, adapter.handshakeTimeoutMs);
		handshakeTimer.unref?.();
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
		const context = hub.registerClient({
			id: clientId,
			groups: new Set<string>(),
			auth: hubAuth(ctx),
			send,
		});
		awaitingHandshake.set(clientId, context);
		return () => {
			clearTimeout(handshakeTimer);
			release(clientId, downstream);
		};
	}

	/**
	 * Hand one request's or one message's frames to the hub, in order, and
	 * write what it answers down the connection's downstream.
	 */
	async function deliver(
		clientId: string,
		body: string,
		auth: HubContext["auth"],
	): Promise<void> {
		const downstream = connections.get(clientId);
		for (const frame of splitFrames(body)) {
			// A Close from the CLIENT is the client saying it is done. The
			// adapter answers it with nothing — correctly, the protocol asks for
			// no reply — so watching only the outbound frames meant a graceful
			// disconnect left the connection open until the socket happened to
			// drop.
			const clientClosed = SignalRAdapter.containsClose([frame]);
			const outbound = await adapter.handleFrame(clientId, frame, auth);
			for (const out of outbound) {
				// No downstream yet means the client posted before opening the
				// GET. The frames are dropped rather than queued: SignalR's own
				// client opens the stream first, and buffering for a connection
				// that may never arrive is how a hub leaks memory.
				if (downstream?.isOpen()) await downstream.send(out);
			}
			// The handshake reply is out: the hub may speak now.
			const pending = awaitingHandshake.get(clientId);
			if (pending !== undefined && adapter.hasCompletedHandshake(clientId)) {
				awaitingHandshake.delete(clientId);
				await hub.onConnect(pending);
			}
			if (clientClosed || SignalRAdapter.containsClose(outbound)) {
				await downstream?.end();
				break;
			}
		}
	}

	const stream = router.get(path, async (ctx) => {
		if (refuseWhileClosed(ctx)) return;
		const admitted = admit(ctx);
		if (admitted === undefined) return;
		const sse = await ctx.response.sse();
		const released = attach(ctx, admitted, sseDownstream(sse));
		sse.onClose(released);
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

		// Authorized against THIS request, not the one that opened the stream:
		// a role revoked since then stops working now.
		await deliver(clientId, ctx.request.raw(), hubAuth(ctx));
		// SignalR expects an empty 200 for an accepted upstream batch.
		ctx.response.status(200).json({});
	});

	/**
	 * The WebSocket transport: the same admission and the same hub as the
	 * stream, with both directions on the one socket.
	 *
	 * A socket has one request, its handshake, so every frame on it is
	 * authorized against the handshake's auth — where an SSE upstream POST is
	 * authorized against its own. A role revoked mid-connection therefore
	 * takes effect on the next connection, as in ASP.NET Core SignalR, whose
	 * WebSocket transport authenticates once too.
	 */
	const socket = router.ws?.(path, async (ctx) => {
		if (refuseWhileClosed(ctx)) return;
		const admitted = admit(ctx);
		if (admitted === undefined) return;
		const { clientId } = admitted;
		const ws = ctx.socket;
		const downstream = socketDownstream(ws);
		const released = attach(ctx, admitted, downstream);
		const auth = hubAuth(ctx);
		// Messages are handled one after another, as SSE posts are: an
		// invocation's answer must not overtake the handshake reply.
		let queue: Promise<void> = Promise.resolve();
		ws.on("message", (data, isBinary) => {
			if (isBinary || typeof data !== "string") {
				// The JSON protocol is text; a binary message is a client
				// speaking something else (MessagePack), refused as such.
				ws.close(WS_UNSUPPORTED_DATA, "text frames only");
				return;
			}
			queue = queue.then(() =>
				deliver(clientId, data, auth).catch((error: unknown) => {
					process.stderr.write(
						`[relay/signalr] frame from ${clientId} failed: ${
							error instanceof Error ? error.message : String(error)
						}\n`,
					);
					ws.close(WS_INTERNAL_ERROR);
				}),
			);
		});
		ws.on("close", released);
	});

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
	function release(clientId: string, downstream: Downstream): void {
		if (connections.get(clientId) !== downstream) return;
		connections.delete(clientId);
		awaitingHandshake.delete(clientId);
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

	const close = async (): Promise<void> => {
		closed = true;
		await Promise.all(
			[...connections].map(async ([clientId, downstream]) => {
				await downstream.end(true).catch(() => {});
				// A connection whose end did not report its close is released
				// here, or its client stays registered on the hub.
				release(clientId, downstream);
			}),
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
