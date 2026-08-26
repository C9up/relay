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

import type { Hub } from "./Hub.js";
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
export interface HubRouter {
	get(path: string, handler: (ctx: HubHttpContext) => Promise<void>): unknown;
	post(path: string, handler: (ctx: HubHttpContext) => Promise<void>): unknown;
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
export function registerHubRoutes(
	router: HubRouter,
	mounted: MountedHub,
): { negotiate: unknown; stream: unknown; send: unknown } {
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

		const sse = await ctx.response.sse();
		streams.set(clientId, sse);
		// An SSE event with NO name arrives as `onmessage`, which is where the
		// SignalR client reads its frames.
		const send = (event: string, data: unknown): void => {
			void sse.send("", adapter.encodeInvocation(event, [data]));
		};
		const context = hub.registerClient({
			id: clientId,
			groups: new Set<string>(),
			auth: hubAuth(ctx),
			send,
		});
		sse.onClose(() => {
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

		const sse = streams.get(clientId);
		for (const frame of splitFrames(ctx.request.raw())) {
			const outbound = await adapter.handleFrame(clientId, frame);
			for (const out of outbound) {
				// No stream yet means the client posted before opening the GET.
				// The frames are dropped rather than queued: SignalR's own client
				// opens the stream first, and buffering for a connection that may
				// never arrive is how a hub leaks memory.
				if (sse?.isOpen()) await sse.send("", out);
			}
			if (SignalRAdapter.containsClose(outbound)) {
				await sse?.end();
				break;
			}
		}
		// SignalR expects an empty 200 for an accepted upstream batch.
		ctx.response.status(200).json({});
	});

	return { negotiate, stream, send };
}
