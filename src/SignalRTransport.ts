/**
 * The HTTP transport that makes a {@link Hub} reachable.
 *
 * `SignalRAdapter` parses and emits frames but owns no socket — it says so
 * itself. Until this file existed, `Hub` and the adapter were exported and
 * could not run on anything: there was no negotiate endpoint, no downstream
 * channel and no way to post a frame up.
 *
 * The transport is Server-Sent Events, which is what `@c9up/relay` already
 * serves and what AdonisJS's own real-time package (`transmit`) uses. It is
 * also a first-class SignalR transport, so a stock `@microsoft/signalr` client
 * configured with `HttpTransportType.ServerSentEvents` speaks it unchanged:
 *
 *   POST <path>/negotiate      → connectionId + connectionToken
 *   GET  <path>?id=<token>     → the SSE stream, server → client
 *   POST <path>?id=<token>     → one or more framed messages, client → server
 *
 * Every server → client message goes out as an SSE event with NO name, so the
 * browser delivers it through `onmessage` — the shape the SignalR client reads.
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
		url(): string;
		raw(): string;
	};
	response: {
		status(code: number): { json(body: unknown): void };
		json(body: unknown): void;
		sse(): Promise<HubSseStream>;
	};
	auth?: {
		isAuthenticated?: boolean;
		strategy?: string;
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
		strategy: ctx.auth?.strategy,
		user: ctx.auth?.user,
		roles: ctx.auth?.roles,
		permissions: ctx.auth?.permissions,
	};
}

/** The user id an auth slice identifies, when it identifies one. */
function userIdOf(
	auth:
		| { isAuthenticated?: boolean; user?: Record<string, unknown> }
		| undefined,
): string | undefined {
	if (auth?.isAuthenticated !== true) return undefined;
	const id = auth.user?.id;
	return typeof id === "string" ? id : undefined;
}

/**
 * Whether a request may act on the connection registered under this client id.
 *
 * The connectionToken travels in a query string — SignalR's own design — so it
 * reaches access logs, proxies and `Referer` headers. Holding it must not be
 * enough to speak as the connection: the frames a POST carries are dispatched
 * with the auth recorded when the stream opened, so a leaked token was an
 * invocation as somebody else. This is the same rule `Relay` applies to its own
 * uid — if the connection was authenticated, the request must be the same user;
 * if it was anonymous, the token is all the identity there is.
 */
function mayActAs(
	registered: HubContext["auth"] | undefined,
	ctx: HubHttpContext,
): boolean {
	const owner = userIdOf(registered);
	if (owner === undefined) return true;
	return userIdOf(ctx.auth) === owner;
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
 * Register the three routes one hub needs.
 *
 * Returns the route objects so a caller can apply middleware to them the way
 * `relay.registerRoutes()` customizes the relay's own.
 */
export function registerHubRoutes<Ctx extends HubHttpContext, Route>(
	router: HubRouter<Ctx, Route>,
	mounted: MountedHub,
): { negotiate: Route; stream: Route; send: Route } {
	const { path, hub, adapter } = mounted;
	/** Live streams by connectionId, so an upstream POST can answer downstream. */
	const streams = new Map<string, HubSseStream>();

	const negotiate = router.post(`${path}/negotiate`, async (ctx) => {
		// The connectionId is the hub's client id; the token is what the client
		// echoes back on the two data routes.
		ctx.response.json(adapter.negotiate(crypto.randomUUID()));
	});

	const stream = router.get(path, async (ctx) => {
		const token = readConnectionToken(ctx.request.url());
		const clientId = token ? adapter.resolveToken(token) : undefined;
		if (clientId === undefined) {
			// Answered BEFORE opening a stream: a 400 written after the upgrade
			// would reach the client as a half-open connection instead.
			ctx.response.status(400).json({
				error: {
					code: "E_UNKNOWN_CONNECTION",
					message: "Unknown or missing connection id — call /negotiate first.",
				},
			});
			return;
		}

		if (!mayActAs(hub.authFor(clientId), ctx)) {
			ctx.response.status(403).json({
				error: {
					code: "E_NOT_OWNER",
					message: "This connection belongs to another user.",
				},
			});
			return;
		}

		const sse = await ctx.response.sse();
		// The token is in use now, so it stops counting against the unclaimed
		// budget and stops expiring.
		if (token !== undefined) adapter.claimToken(token);
		streams.set(clientId, sse);
		// An SSE event with NO name arrives as `onmessage`, which is where the
		// SignalR client reads its frames.
		const send = (event: string, data: unknown): void => {
			// A hub send is fire-and-forget by contract — the caller returns
			// nothing — so a rejection here had nobody to reject to and took the
			// process down over one client's socket.
			void sse
				.send("", adapter.encodeInvocation(event, [data]))
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
		sse.onClose(() => {
			// Identity-guarded: a client id can carry a second stream — a
			// reconnect that opens before the old socket has finished closing —
			// and the old one's close must not tear down the live connection that
			// replaced it. Unguarded, the stale close unregistered the new
			// client from the hub and dropped its tokens, leaving a socket that
			// was open and could no longer receive anything.
			if (streams.get(clientId) !== sse) return;
			streams.delete(clientId);
			// `removeClient` fires `onDisconnect` itself — calling it here too
			// would deliver every disconnect twice.
			hub.removeClient(clientId);
			adapter.forget(clientId);
		});
		await hub.onConnect(context);
	});

	const send = router.post(path, async (ctx) => {
		const token = readConnectionToken(ctx.request.url());
		const clientId = token ? adapter.resolveToken(token) : undefined;
		if (clientId === undefined) {
			ctx.response.status(400).json({
				error: {
					code: "E_UNKNOWN_CONNECTION",
					message: "Unknown or missing connection id — call /negotiate first.",
				},
			});
			return;
		}

		if (!mayActAs(hub.authFor(clientId), ctx)) {
			ctx.response.status(403).json({
				error: {
					code: "E_NOT_OWNER",
					message: "This connection belongs to another user.",
				},
			});
			return;
		}

		const sse = streams.get(clientId);
		for (const frame of splitFrames(ctx.request.raw())) {
			// A Close from the CLIENT is the client saying it is done. The
			// adapter answers it with nothing — correctly, the protocol asks for
			// no reply — so watching only the outbound frames meant a graceful
			// disconnect left the stream open until the socket happened to drop.
			const clientClosed = SignalRAdapter.containsClose([frame]);
			const outbound = await adapter.handleFrame(clientId, frame);
			for (const out of outbound) {
				// No stream yet means the client posted before opening the GET.
				// The frames are dropped rather than queued: SignalR's own client
				// opens the stream first, and buffering for a connection that may
				// never arrive is how a hub leaks memory.
				if (sse?.isOpen()) await sse.send("", out);
			}
			if (clientClosed || SignalRAdapter.containsClose(outbound)) {
				await sse?.end();
				break;
			}
		}
		// SignalR expects an empty 200 for an accepted upstream batch.
		ctx.response.status(200).json({});
	});

	return { negotiate, stream, send };
}
